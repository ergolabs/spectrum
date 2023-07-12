use std::collections::HashSet;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, time::Duration};

use async_std::sync::Mutex;
use futures::channel::mpsc::Sender;
use futures::{
    channel::{mpsc, oneshot},
    SinkExt, StreamExt,
};
use libp2p::swarm::SwarmBuilder;
use libp2p::{
    core::{transport::Transport, upgrade::Version},
    identity, noise,
    swarm::SwarmEvent,
    tcp, yamux, Multiaddr, PeerId,
};
use log::LevelFilter;
use log4rs_test_utils::test_logging::init_logging_once_for;

use spectrum_crypto::digest::{blake2b256_hash, Blake2bDigest256};
use spectrum_crypto::pubkey::PublicKey;
use spectrum_network::protocol::{OneShotProtocolConfig, OneShotProtocolSpec, ProtocolConfig};
use spectrum_network::protocol_api::ProtocolEvent;
use spectrum_network::protocol_handler::aggregation::AggregationAction;
use spectrum_network::protocol_handler::handel::{
    partitioning::{MakePeerPartitions, PeerIx, PeerPartitions},
    Threshold, Weighted,
};
use spectrum_network::protocol_handler::multicasting::overlay::{
    MakeDagOverlay, RedundancyDagOverlayBuilder,
};
use spectrum_network::protocol_handler::sigma_aggregation::types::Contributions;
use spectrum_network::types::{ProtocolTag, RawMessage};
use spectrum_network::{
    network_controller::{NetworkController, NetworkControllerIn, NetworkControllerOut, NetworkMailbox},
    peer_conn_handler::{ConnHandlerError, PeerConnHandlerConf},
    peer_manager::{
        data::{ConnectionLossReason, PeerDestination, ReputationChange},
        peers_state::PeerRepo,
        NetworkingConfig, PeerManager, PeerManagerConfig, PeersMailbox,
    },
    protocol::{StatefulProtocolConfig, StatefulProtocolSpec, DISCOVERY_PROTOCOL_ID},
    protocol_api::ProtocolMailbox,
    protocol_handler::{
        discovery::{
            message::{DiscoveryMessage, DiscoveryMessageV1, DiscoverySpec},
            DiscoveryBehaviour, NodeStatus,
        },
        ProtocolBehaviour, ProtocolHandler,
    },
    types::{ProtocolId, ProtocolVer, Reputation},
};
use tracing::{info, trace};

use crate::integration_tests::fake_sync_behaviour::{FakeSyncBehaviour, FakeSyncMessage, FakeSyncMessageV1};
use crate::integration_tests::multicasting::{SetTask, Statements};

mod aggregation;
mod fake_sync_behaviour;
mod multicasting;

/// Identifies particular peers
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    /// The tag for `peer_0`
    First,
    /// The tag for `peer_1`
    Second,
    /// The tag for `peer_2`
    Third,
}

/// Unifies [`NetworkController`] and protocol messages.
pub enum Msg<M> {
    /// Messages from `NetworkController`.
    NetworkController(NetworkControllerOut),
    /// Protocol message.
    Protocol(M),
}

/// Integration test which covers:
///  - outbound one-shot message delivery attempt
///  - inbound one-shot message handling
#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[async_std::test]
async fn one_shot_messaging() {
    //  --------             --------
    // | peer_0 | <~~~~~~~~ | peer_1 |
    //  --------             --------
    //
    // In this scenario `peer_1` sends one-shot message to `peer_0`.
    let local_key_0 = identity::Keypair::generate_ed25519();
    let local_peer_id_0 = PeerId::from(local_key_0.public());
    let local_key_1 = identity::Keypair::generate_ed25519();

    let addr_0: Multiaddr = "/ip4/127.0.0.1/tcp/1234".parse().unwrap();
    let addr_1: Multiaddr = "/ip4/127.0.0.1/tcp/1235".parse().unwrap();
    let peers_1 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_0, addr_0.clone())];

    let (protocol_snd_0, mut protocol_recv_0) = mpsc::channel::<ProtocolEvent>(100);
    let prot_mailbox_0 = ProtocolMailbox::new(protocol_snd_0);
    let (protocol_snd_1, _protocol_recv_1) = mpsc::channel::<ProtocolEvent>(100);
    let prot_mailbox_1 = ProtocolMailbox::new(protocol_snd_1);

    let pid = ProtocolId::from_u8(1u8);
    let ver = ProtocolVer::from(1u8);
    let one_shot_proto_conf = OneShotProtocolConfig {
        version: ver,
        spec: OneShotProtocolSpec {
            max_message_size: 100,
        },
    };
    let protocols_0 = HashMap::from([(
        pid,
        (
            ProtocolConfig::OneShot(one_shot_proto_conf.clone()),
            prot_mailbox_0,
        ),
    )]);
    let protocols_1 = HashMap::from([(
        pid,
        (ProtocolConfig::OneShot(one_shot_proto_conf), prot_mailbox_1),
    )]);
    let (nc_0, _nc_mailbox_0) = make_nc_without_protocol_handler(vec![], protocols_0);
    let (nc_1, nc_mailbox_1) = make_nc_without_protocol_handler(peers_1, protocols_1);

    let protocol = ProtocolTag::new(pid, ver);
    let message = RawMessage::from(vec![0, 0, 0]);

    let (abortable_peer_0, handle_0) =
        futures::future::abortable(aggregation::create_swarm(local_key_0, nc_0, addr_0, 1));
    let (abortable_peer_1, handle_1) =
        futures::future::abortable(aggregation::create_swarm(local_key_1, nc_1, addr_1, 2));
    let (cancel_tx_0, cancel_rx_0) = oneshot::channel::<()>();
    let (cancel_tx_1, cancel_rx_1) = oneshot::channel::<()>();

    // Spawn tasks for peer_0
    async_std::task::spawn(async move {
        let _ = cancel_rx_0.await;
        handle_0.abort();
    });

    // Spawn tasks for peer_1
    async_std::task::spawn(async move {
        let _ = cancel_rx_1.await;
        handle_1.abort();
    });

    async_std::task::spawn(abortable_peer_0);
    async_std::task::spawn(abortable_peer_1);
    wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();

    let _ = futures::executor::block_on(
        nc_mailbox_1
            .clone()
            .send(NetworkControllerIn::SendOneShotMessage {
                peer: local_peer_id_0,
                addr_hint: None,
                protocol,
                message: message.clone(),
            }),
    );

    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();
        cancel_tx_0.send(()).unwrap();
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();
        cancel_tx_1.send(()).unwrap();
    });

    // Collect messages from the peers. Note that the while loop below will end since all tasks that
    // use clones of `msg_tx` are guaranteed to drop, leading to the senders dropping too.
    let mut protocol_mailbox = vec![];

    while let Some(event) = protocol_recv_0.next().await {
        protocol_mailbox.push(event);
    }

    dbg!(&protocol_mailbox);

    let maybe_message = if let ProtocolEvent::Message { content, .. } = &protocol_mailbox[0] {
        Some(content.clone())
    } else {
        None
    };
    assert_eq!(maybe_message, Some(message));
}

/// Integration test which covers:
///  - peer connection
///  - peer disconnection by sudden shutdown (`ResetByPeer`)
///  - peer punishment due to no-response
#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[async_std::test]
async fn integration_test_0() {
    //               --------             --------
    // ?? <~~~~~~~~ | peer_0 | <~~~~~~~~ | peer_1 |
    //               --------             --------
    //
    // In this scenario `peer_0` has a non-existent peer in the bootstrap-peer set and `peer_1` has
    // only `peer_0` as a bootstrap peer.
    //   - `peer_1` will successfully establish a connection with `peer_0`
    //   - `peer_0`s attempted connection will trigger peer-punishment
    //   - Afterwards we shutdown `peer_1` and check for peer disconnection event in `peer_0`.
    let local_key_0 = identity::Keypair::generate_ed25519();
    let local_peer_id_0 = PeerId::from(local_key_0.public());
    let local_key_1 = identity::Keypair::generate_ed25519();
    let local_peer_id_1 = PeerId::from(local_key_1.public());

    // Non-existent peer
    let fake_peer_id = PeerId::random();
    let fake_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1236".parse().unwrap();

    let addr_0: Multiaddr = "/ip4/127.0.0.1/tcp/1234".parse().unwrap();
    let addr_1: Multiaddr = "/ip4/127.0.0.1/tcp/1235".parse().unwrap();
    let peers_0 = vec![PeerDestination::PeerIdWithAddr(fake_peer_id, fake_addr)];
    let peers_1 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_0, addr_0.clone())];

    let local_status_0 = NodeStatus {
        supported_protocols: Vec::from([DISCOVERY_PROTOCOL_ID]),
        height: 0,
    };
    let local_status_1 = local_status_0.clone();
    let sync_behaviour_0 = |p| DiscoveryBehaviour::new(p, local_status_0);
    let sync_behaviour_1 = |p| DiscoveryBehaviour::new(p, local_status_1);

    // Though we spawn multiple tasks we use this single channel for messaging.
    let (msg_tx, mut msg_rx) = mpsc::channel::<(Peer, Msg<DiscoveryMessage>)>(10);

    let (mut sync_handler_0, nc_0) = make_swarm_components(peers_0, sync_behaviour_0, 10);
    let (mut sync_handler_1, nc_1) = make_swarm_components(peers_1, sync_behaviour_1, 10);

    let mut msg_tx_sync_handler_0 = msg_tx.clone();
    let sync_handler_0_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_0.select_next_some().await;
            msg_tx_sync_handler_0
                .try_send((Peer::First, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let mut msg_tx_sync_handler_1 = msg_tx.clone();
    let sync_handler_1_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_1.select_next_some().await;
            msg_tx_sync_handler_1
                .try_send((Peer::Second, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let (abortable_peer_0, handle_0) =
        futures::future::abortable(create_swarm::<DiscoveryBehaviour<PeersMailbox>>(
            local_key_0,
            nc_0,
            addr_0,
            Peer::First,
            msg_tx.clone(),
        ));
    let (abortable_peer_1, handle_1) =
        futures::future::abortable(create_swarm::<DiscoveryBehaviour<PeersMailbox>>(
            local_key_1,
            nc_1,
            addr_1,
            Peer::Second,
            msg_tx,
        ));
    let (cancel_tx_0, cancel_rx_0) = oneshot::channel::<()>();
    let (cancel_tx_1, cancel_rx_1) = oneshot::channel::<()>();

    // Spawn tasks for peer_0
    async_std::task::spawn(async move {
        let _ = cancel_rx_0.await;
        handle_0.abort();
        sync_handler_0_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();
        cancel_tx_0.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_0);

    // Spawn tasks for peer_1
    async_std::task::spawn(async move {
        let _ = cancel_rx_1.await;
        handle_1.abort();
        sync_handler_1_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(4)).await.unwrap();
        cancel_tx_1.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_1);

    // Collect messages from the peers. Note that the while loop below will end since all tasks that
    // use clones of `msg_tx` are guaranteed to drop, leading to the senders dropping too.
    let mut nc_peer_0 = vec![];
    let mut nc_peer_1 = vec![];
    let mut prot_peer_0 = vec![];
    let mut prot_peer_1 = vec![];
    while let Some((peer, msg)) = msg_rx.next().await {
        match msg {
            Msg::NetworkController(nc_msg) => match peer {
                Peer::First => nc_peer_0.push(nc_msg),
                Peer::Second => nc_peer_1.push(nc_msg),
                Peer::Third => (),
            },
            Msg::Protocol(p_msg) => match peer {
                Peer::First => prot_peer_0.push(p_msg),
                Peer::Second => prot_peer_1.push(p_msg),
                Peer::Third => (),
            },
        }
    }

    dbg!(&nc_peer_0);
    dbg!(&nc_peer_1);
    dbg!(&prot_peer_0);
    dbg!(&prot_peer_1);

    let protocol_id = ProtocolId::from(0);
    let protocol_ver = ProtocolVer::from(1);
    let expected_nc_peer_0 = vec![
        NetworkControllerOut::PeerPunished {
            peer_id: fake_peer_id,
            reason: ReputationChange::NoResponse,
        },
        NetworkControllerOut::ConnectedWithInboundPeer(local_peer_id_1),
        NetworkControllerOut::ProtocolPendingApprove {
            peer_id: local_peer_id_1,
            protocol_id,
        },
        NetworkControllerOut::ProtocolEnabled {
            peer_id: local_peer_id_1,
            protocol_id,
            protocol_ver,
        },
        NetworkControllerOut::Disconnected {
            peer_id: local_peer_id_1,
            reason: ConnectionLossReason::ResetByPeer,
        },
    ];
    assert_eq!(nc_peer_0, expected_nc_peer_0);

    let expected_nc_peer_1 = vec![
        NetworkControllerOut::ConnectedWithOutboundPeer(local_peer_id_0),
        NetworkControllerOut::ProtocolPendingEnable {
            peer_id: local_peer_id_0,
            protocol_id,
        },
        NetworkControllerOut::ProtocolEnabled {
            peer_id: local_peer_id_0,
            protocol_id,
            protocol_ver,
        },
    ];
    assert_eq!(nc_peer_1, expected_nc_peer_1);

    let expected_prot_peer_0 = vec![
        DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::GetPeers),
        DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(vec![])),
    ];

    let expected_prot_peer_1 = expected_prot_peer_0.clone();
    assert_eq!(prot_peer_0, expected_prot_peer_0);
    assert_eq!(prot_peer_1, expected_prot_peer_1);
}

/// Integration test which covers:
///  - peer connection
///  - peer punishment due to malformed message
///  - peer disconnection from reputation being too low
#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[async_std::test]
async fn integration_test_1() {
    //   --------             --------
    //  | peer_0 | <~~~~~~~~ | peer_1 |
    //   --------             --------
    //
    // In this scenario `peer_0` has no bootstrap peers and `peer_1` has only `peer_0` as a
    // bootstrap peer. `peer_0` is running the Sync protocol and `peer_1` a fake-Sync protocol.
    // After `peer_1` establishes a connection to `peer_0`, `peer_1` will send a message which is
    // regarded as malformed by `peer_0`. `peer_0` then punishes `peer_1` and a disconnection is
    // triggered due to reputation being too low.
    let local_key_0 = identity::Keypair::generate_ed25519();
    let local_peer_id_0 = PeerId::from(local_key_0.public());
    let local_key_1 = identity::Keypair::generate_ed25519();
    let local_peer_id_1 = PeerId::from(local_key_1.public());

    let addr_0: Multiaddr = "/ip4/127.0.0.1/tcp/1237".parse().unwrap();
    let addr_1: Multiaddr = "/ip4/127.0.0.1/tcp/1238".parse().unwrap();
    let peers_0 = vec![];
    let peers_1 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_0, addr_0.clone())];

    let local_status_0 = NodeStatus {
        supported_protocols: Vec::from([DISCOVERY_PROTOCOL_ID]),
        height: 0,
    };
    let local_status_1 = local_status_0.clone();
    let sync_behaviour_0 = |p| DiscoveryBehaviour::new(p, local_status_0);
    let fake_sync_behaviour = |p| FakeSyncBehaviour::new(p, local_status_1);

    // Note that we use 2 channels here since `peer_0` sends `DiscoveryMessage`s while `peer_1` sends `FakeSyncMessage`s.
    let (msg_tx, msg_rx) = mpsc::channel::<(Peer, Msg<DiscoveryMessage>)>(10);
    let (fake_msg_tx, fake_msg_rx) = mpsc::channel::<(Peer, Msg<FakeSyncMessage>)>(10);

    let (mut sync_handler_0, nc_0) = make_swarm_components(peers_0, sync_behaviour_0, 10);
    let (mut sync_handler_1, nc_1) = make_swarm_components(peers_1, fake_sync_behaviour, 10);

    let mut msg_tx_sync_handler_0 = msg_tx.clone();
    let sync_handler_0_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_0.select_next_some().await;
            msg_tx_sync_handler_0
                .try_send((Peer::First, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let mut fake_msg_tx_sync_handler_1 = fake_msg_tx.clone();
    let sync_handler_1_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_1.select_next_some().await;
            fake_msg_tx_sync_handler_1
                .try_send((Peer::Second, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let (abortable_peer_0, handle_0) =
        futures::future::abortable(create_swarm::<DiscoveryBehaviour<PeersMailbox>>(
            local_key_0,
            nc_0,
            addr_0,
            Peer::First,
            msg_tx,
        ));
    let (abortable_peer_1, handle_1) =
        futures::future::abortable(create_swarm::<FakeSyncBehaviour<PeersMailbox>>(
            local_key_1,
            nc_1,
            addr_1,
            Peer::Second,
            fake_msg_tx,
        ));

    let (cancel_tx_0, cancel_rx_0) = oneshot::channel::<()>();
    let (cancel_tx_1, cancel_rx_1) = oneshot::channel::<()>();

    // Spawn tasks for peer_0
    async_std::task::spawn(async move {
        let _ = cancel_rx_0.await;
        handle_0.abort();
        sync_handler_0_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(6)).await.unwrap();
        cancel_tx_0.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_0);

    // Spawn tasks for peer_1
    async_std::task::spawn(async move {
        let _ = cancel_rx_1.await;
        handle_1.abort();
        sync_handler_1_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();
        cancel_tx_1.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_1);

    // We use this enum to combine `msg_rx` and `fake_msg_rx` streams
    enum C {
        SyncMsg((Peer, Msg<DiscoveryMessage>)),
        FakeMsg((Peer, Msg<FakeSyncMessage>)),
    }

    type CombinedStream = std::pin::Pin<Box<dyn futures::stream::Stream<Item = C> + Send>>;

    let streams: Vec<CombinedStream> = vec![
        msg_rx.map(C::SyncMsg).boxed(),
        fake_msg_rx.map(C::FakeMsg).boxed(),
    ];
    let mut combined_stream = futures::stream::select_all(streams);

    // Collect messages from the peers. Note that the while loop below will end since all tasks that
    // use clones of `msg_tx` and `fake_msg_tx` are guaranteed to drop, leading to the senders
    // dropping too.
    let mut nc_peer_0 = vec![];
    let mut nc_peer_1 = vec![];
    let mut prot_peer_0: Vec<DiscoveryMessage> = vec![];
    let mut prot_peer_1: Vec<FakeSyncMessage> = vec![];
    while let Some(c) = combined_stream.next().await {
        match c {
            C::SyncMsg((peer, msg)) => match msg {
                Msg::NetworkController(nc_msg) => match peer {
                    Peer::First => nc_peer_0.push(nc_msg),
                    Peer::Second => nc_peer_1.push(nc_msg),
                    Peer::Third => (),
                },
                Msg::Protocol(p_msg) => prot_peer_0.push(p_msg),
            },
            C::FakeMsg((peer, msg)) => match msg {
                Msg::NetworkController(nc_msg) => match peer {
                    Peer::First => nc_peer_0.push(nc_msg),
                    Peer::Second => nc_peer_1.push(nc_msg),
                    Peer::Third => (),
                },
                Msg::Protocol(p_msg) => prot_peer_1.push(p_msg),
            },
        }
    }

    dbg!(&nc_peer_0);
    dbg!(&nc_peer_1);
    dbg!(&prot_peer_0);
    dbg!(&prot_peer_1);

    let protocol_id = ProtocolId::from(0);
    let protocol_ver = ProtocolVer::from(1);
    let expected_nc_peer_0 = vec![
        NetworkControllerOut::ConnectedWithInboundPeer(local_peer_id_1),
        NetworkControllerOut::ProtocolPendingApprove {
            peer_id: local_peer_id_1,
            protocol_id,
        },
        NetworkControllerOut::ProtocolEnabled {
            peer_id: local_peer_id_1,
            protocol_id,
            protocol_ver,
        },
        NetworkControllerOut::Disconnected {
            peer_id: local_peer_id_1,
            reason: ConnectionLossReason::ResetByPeer,
        },
    ];

    assert_eq!(expected_nc_peer_0, nc_peer_0);

    let expected_nc_peer_1 = vec![
        NetworkControllerOut::ConnectedWithOutboundPeer(local_peer_id_0),
        NetworkControllerOut::ProtocolPendingEnable {
            peer_id: local_peer_id_0,
            protocol_id,
        },
        NetworkControllerOut::ProtocolEnabled {
            peer_id: local_peer_id_0,
            protocol_id,
            protocol_ver,
        },
    ];
    assert_eq!(expected_nc_peer_1, nc_peer_1);

    assert_eq!(
        prot_peer_0,
        vec![DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::GetPeers),]
    );
    assert_eq!(
        prot_peer_1,
        vec![FakeSyncMessage::SyncMessageV1(FakeSyncMessageV1::FakeMsg),]
    );
}

#[async_std::test]
#[cfg_attr(not(feature = "test_peer_punish_too_slow"), ignore)]
async fn integration_test_peer_punish_too_slow() {
    //   --------             --------
    //  | peer_0 | <~~~~~~~~ | peer_1 |
    //   --------             --------
    //
    // In this scenario `peer_0` has no bootstrap peers and `peer_1` has only `peer_0` as a
    // bootstrap peer.  After `peer_1` establishes a connection to `peer_0`, each peer will send
    // multiple `GetPeers` messages in order to saturate the message buffers of each peer, resulting
    // in peer disconnection.
    let local_key_0 = identity::Keypair::generate_ed25519();
    let local_peer_id_0 = PeerId::from(local_key_0.public());
    let local_key_1 = identity::Keypair::generate_ed25519();
    let local_peer_id_1 = PeerId::from(local_key_1.public());

    let addr_0: Multiaddr = "/ip4/127.0.0.1/tcp/1237".parse().unwrap();
    let addr_1: Multiaddr = "/ip4/127.0.0.1/tcp/1238".parse().unwrap();
    let peers_0 = vec![];
    let peers_1 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_0, addr_0.clone())];

    let local_status_0 = NodeStatus {
        supported_protocols: Vec::from([DISCOVERY_PROTOCOL_ID]),
        height: 0,
    };
    let local_status_1 = local_status_0.clone();
    let sync_behaviour_0 = |p| FakeSyncBehaviour::new(p, local_status_0);
    let sync_behaviour_1 = |p| FakeSyncBehaviour::new(p, local_status_1);

    let (msg_tx, mut msg_rx) = mpsc::channel::<(Peer, Msg<FakeSyncMessage>)>(10);

    // It's crucial to have a buffer of size 1 for this test
    let msg_buffer_size = 1;
    let (mut sync_handler_0, nc_0) = make_swarm_components(peers_0, sync_behaviour_0, msg_buffer_size);
    let (mut sync_handler_1, nc_1) = make_swarm_components(peers_1, sync_behaviour_1, msg_buffer_size);

    let mut msg_tx_sync_handler_0 = msg_tx.clone();
    let sync_handler_0_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_0.select_next_some().await;
            msg_tx_sync_handler_0
                .try_send((Peer::First, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let mut msg_tx_sync_handler_1 = msg_tx.clone();
    let sync_handler_1_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_1.select_next_some().await;
            msg_tx_sync_handler_1
                .try_send((Peer::Second, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let (abortable_peer_0, handle_0) =
        futures::future::abortable(create_swarm::<FakeSyncBehaviour<PeersMailbox>>(
            local_key_0,
            nc_0,
            addr_0,
            Peer::First,
            msg_tx.clone(),
        ));
    let (abortable_peer_1, handle_1) =
        futures::future::abortable(create_swarm::<FakeSyncBehaviour<PeersMailbox>>(
            local_key_1,
            nc_1,
            addr_1,
            Peer::Second,
            msg_tx,
        ));
    let (cancel_tx_0, cancel_rx_0) = oneshot::channel::<()>();
    let (cancel_tx_1, cancel_rx_1) = oneshot::channel::<()>();

    // Spawn tasks for peer_0
    async_std::task::spawn(async move {
        let _ = cancel_rx_0.await;
        handle_0.abort();
        sync_handler_0_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(6)).await.unwrap();
        cancel_tx_0.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_0);

    // Spawn tasks for peer_1
    async_std::task::spawn(async move {
        let _ = cancel_rx_1.await;
        handle_1.abort();
        sync_handler_1_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();
        cancel_tx_1.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_1);

    // Collect messages from the peers. Note that the while loop below will end since all tasks that
    // use clones of `msg_tx` are guaranteed to drop, leading to the senders dropping too.
    let mut nc_peer_0 = vec![];
    let mut nc_peer_1 = vec![];
    let mut prot_peer_0 = vec![];
    let mut prot_peer_1 = vec![];
    while let Some((peer, msg)) = msg_rx.next().await {
        match msg {
            Msg::NetworkController(nc_msg) => match peer {
                Peer::First => nc_peer_0.push(nc_msg),
                Peer::Second => nc_peer_1.push(nc_msg),
                Peer::Third => (),
            },
            Msg::Protocol(p_msg) => match peer {
                Peer::First => prot_peer_0.push(p_msg),
                Peer::Second => prot_peer_1.push(p_msg),
                Peer::Third => (),
            },
        }
    }

    dbg!(&nc_peer_0);
    dbg!(&nc_peer_1);
    dbg!(&prot_peer_0);
    dbg!(&prot_peer_1);

    let protocol_id = ProtocolId::from(0);
    let protocol_ver = ProtocolVer::from(1);
    let expected_nc_peer_0 = vec![
        NetworkControllerOut::ConnectedWithInboundPeer(local_peer_id_1),
        NetworkControllerOut::ProtocolPendingApprove {
            peer_id: local_peer_id_1,
            protocol_id,
        },
        NetworkControllerOut::ProtocolEnabled {
            peer_id: local_peer_id_1,
            protocol_id,
            protocol_ver,
        },
        NetworkControllerOut::Disconnected {
            peer_id: local_peer_id_1,
            reason: ConnectionLossReason::Reset(ConnHandlerError::SyncChannelExhausted),
        },
        NetworkControllerOut::PeerPunished {
            peer_id: local_peer_id_1,
            reason: ReputationChange::TooSlow,
        },
    ];
    assert_eq!(expected_nc_peer_0, nc_peer_0);

    let expected_nc_peer_1 = vec![
        NetworkControllerOut::ConnectedWithOutboundPeer(local_peer_id_0),
        NetworkControllerOut::ProtocolPendingEnable {
            peer_id: local_peer_id_0,
            protocol_id,
        },
        NetworkControllerOut::ProtocolEnabled {
            peer_id: local_peer_id_0,
            protocol_id,
            protocol_ver,
        },
        NetworkControllerOut::Disconnected {
            peer_id: local_peer_id_0,
            reason: ConnectionLossReason::Reset(ConnHandlerError::SyncChannelExhausted),
        },
        NetworkControllerOut::PeerPunished {
            peer_id: local_peer_id_0,
            reason: ReputationChange::TooSlow,
        },
    ];
    assert_eq!(expected_nc_peer_1, nc_peer_1);
}

#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[async_std::test]
async fn integration_test_2() {
    //   --------              --------             --------
    //  | peer_0 |  ~~~~~~~~> | peer_1 | ~~~~~~~~> | peer_2 |
    //   --------              --------             --------
    //     ^                                            |
    //     |                                            |
    //     | ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|
    //
    // In this scenario `peer_0`, `peer_1` and `peer_2` has `peer_1`, `peer_2` and `peer_0` as a
    // bootstrap-peer, respectively (indicated by the arrows)
    let local_key_0 = identity::Keypair::generate_ed25519();
    let local_peer_id_0 = PeerId::from(local_key_0.public());
    let local_key_1 = identity::Keypair::generate_ed25519();
    let local_peer_id_1 = PeerId::from(local_key_1.public());
    let local_key_2 = identity::Keypair::generate_ed25519();
    let local_peer_id_2 = PeerId::from(local_key_2.public());

    let addr_0: Multiaddr = "/ip4/127.0.0.1/tcp/1240".parse().unwrap();
    let addr_1: Multiaddr = "/ip4/127.0.0.1/tcp/1241".parse().unwrap();
    let addr_2: Multiaddr = "/ip4/127.0.0.1/tcp/1242".parse().unwrap();
    let peers_0 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_1, addr_1.clone())];
    let peers_1 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_2, addr_2.clone())];
    let peers_2 = vec![PeerDestination::PeerIdWithAddr(local_peer_id_0, addr_0.clone())];

    let local_status_0 = NodeStatus {
        supported_protocols: Vec::from([DISCOVERY_PROTOCOL_ID]),
        height: 0,
    };
    let local_status_1 = local_status_0.clone();
    let local_status_2 = local_status_0.clone();
    let sync_behaviour_0 = |p| DiscoveryBehaviour::new(p, local_status_0);
    let sync_behaviour_1 = |p| DiscoveryBehaviour::new(p, local_status_1);
    let sync_behaviour_2 = |p| DiscoveryBehaviour::new(p, local_status_2);

    // Though we spawn multiple tasks we use this single channel for messaging.
    let (msg_tx, mut msg_rx) = mpsc::channel::<(Peer, Msg<DiscoveryMessage>)>(10);

    let (mut sync_handler_0, nc_0) = make_swarm_components(peers_0, sync_behaviour_0, 10);
    let (mut sync_handler_1, nc_1) = make_swarm_components(peers_1, sync_behaviour_1, 10);
    let (mut sync_handler_2, nc_2) = make_swarm_components(peers_2, sync_behaviour_2, 10);

    let mut msg_tx_sync_handler_0 = msg_tx.clone();
    let sync_handler_0_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_0.select_next_some().await;
            msg_tx_sync_handler_0
                .try_send((Peer::First, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let mut msg_tx_sync_handler_1 = msg_tx.clone();
    let sync_handler_1_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_1.select_next_some().await;
            msg_tx_sync_handler_1
                .try_send((Peer::Second, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let mut msg_tx_sync_handler_2 = msg_tx.clone();
    let sync_handler_2_handle = async_std::task::spawn(async move {
        loop {
            let msg = sync_handler_2.select_next_some().await;
            msg_tx_sync_handler_2
                .try_send((Peer::Third, Msg::Protocol(msg)))
                .unwrap();
        }
    });

    let (abortable_peer_0, handle_0) =
        futures::future::abortable(create_swarm::<DiscoveryBehaviour<PeersMailbox>>(
            local_key_0,
            nc_0,
            addr_0.clone(),
            Peer::First,
            msg_tx.clone(),
        ));
    let (abortable_peer_1, handle_1) =
        futures::future::abortable(create_swarm::<DiscoveryBehaviour<PeersMailbox>>(
            local_key_1,
            nc_1,
            addr_1.clone(),
            Peer::Second,
            msg_tx.clone(),
        ));
    let (abortable_peer_2, handle_2) =
        futures::future::abortable(create_swarm::<DiscoveryBehaviour<PeersMailbox>>(
            local_key_2,
            nc_2,
            addr_2.clone(),
            Peer::Third,
            msg_tx,
        ));
    let (cancel_tx_0, cancel_rx_0) = oneshot::channel::<()>();
    let (cancel_tx_1, cancel_rx_1) = oneshot::channel::<()>();
    let (cancel_tx_2, cancel_rx_2) = oneshot::channel::<()>();

    let secs = 5;

    // Spawn tasks for peer_0
    async_std::task::spawn(async move {
        let _ = cancel_rx_0.await;
        handle_0.abort();
        sync_handler_0_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(secs)).await.unwrap();
        cancel_tx_0.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_0);

    // Spawn tasks for peer_1
    async_std::task::spawn(async move {
        let _ = cancel_rx_1.await;
        handle_1.abort();
        sync_handler_1_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(secs)).await.unwrap();
        cancel_tx_1.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_1);

    // Spawn tasks for peer_2
    async_std::task::spawn(async move {
        let _ = cancel_rx_2.await;
        handle_2.abort();
        sync_handler_2_handle.cancel().await;
    });
    async_std::task::spawn(async move {
        wasm_timer::Delay::new(Duration::from_secs(secs)).await.unwrap();
        cancel_tx_2.send(()).unwrap();
    });
    async_std::task::spawn(abortable_peer_2);

    // Collect messages from the peers. Note that the while loop below will end since all tasks that
    // use clones of `msg_tx` are guaranteed to drop, leading to the senders dropping too.
    let mut nc_peer_0 = vec![];
    let mut nc_peer_1 = vec![];
    let mut nc_peer_2 = vec![];
    let mut prot_peer_0 = vec![];
    let mut prot_peer_1 = vec![];
    let mut prot_peer_2 = vec![];
    while let Some((peer, msg)) = msg_rx.next().await {
        match msg {
            Msg::NetworkController(nc_msg) => match peer {
                Peer::First => nc_peer_0.push(nc_msg),
                Peer::Second => nc_peer_1.push(nc_msg),
                Peer::Third => nc_peer_2.push(nc_msg),
            },
            Msg::Protocol(p_msg) => match peer {
                Peer::First => prot_peer_0.push(p_msg),
                Peer::Second => prot_peer_1.push(p_msg),
                Peer::Third => prot_peer_2.push(p_msg),
            },
        }
    }

    dbg!(&nc_peer_0);
    dbg!(&nc_peer_1);
    dbg!(&nc_peer_2);
    dbg!(&prot_peer_0);
    dbg!(&prot_peer_1);
    dbg!(&prot_peer_2);

    // Check that `peer_0` is sending out the necessary `Peers` messages.
    assert!(
        prot_peer_0.contains(&DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(
            vec![PeerDestination::PeerIdWithAddr(local_peer_id_1, addr_1)]
        )))
    );
    assert!(
        prot_peer_0.contains(&DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(
            vec![PeerDestination::PeerId(local_peer_id_2)]
        )))
    );

    // Check that `peer_1` is sending out the necessary `Peers` messages.
    assert!(
        prot_peer_1.contains(&DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(
            vec![PeerDestination::PeerIdWithAddr(local_peer_id_2, addr_2)]
        )))
    );
    assert!(
        prot_peer_1.contains(&DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(
            vec![PeerDestination::PeerId(local_peer_id_0)]
        )))
    );

    // Check that `peer_2` is sending out the necessary `Peers` messages.
    assert!(
        prot_peer_2.contains(&DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(
            vec![PeerDestination::PeerIdWithAddr(local_peer_id_0, addr_0)]
        )))
    );
    assert!(
        prot_peer_2.contains(&DiscoveryMessage::DiscoveryMessageV1(DiscoveryMessageV1::Peers(
            vec![PeerDestination::PeerId(local_peer_id_1)]
        )))
    );
}

async fn create_swarm<P>(
    local_key: identity::Keypair,
    nc: NetworkController<PeersMailbox, PeerManager<PeerRepo>, ProtocolMailbox>,
    addr: Multiaddr,
    peer: Peer,
    mut tx: mpsc::Sender<(
        Peer,
        Msg<<<P as ProtocolBehaviour>::TProto as spectrum_network::protocol_handler::ProtocolSpec>::TMessage>,
    )>,
) where
    P: ProtocolBehaviour + Unpin + Send + 'static,
{
    let transport = tcp::async_io::Transport::default()
        .upgrade(Version::V1Lazy)
        .authenticate(noise::Config::new(&local_key).unwrap()) // todo: avoid auth
        .multiplex(yamux::Config::default())
        .boxed();
    let local_peer_id = PeerId::from(local_key.public());
    let mut swarm = SwarmBuilder::with_async_std_executor(transport, nc, local_peer_id).build();

    swarm.listen_on(addr).unwrap();

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => trace!("Listening on {:?}", address),
            SwarmEvent::Behaviour(event) => tx.try_send((peer, Msg::NetworkController(event))).unwrap(),
            ce => {
                //trace!("{:?} :: Recv event :: {:?}", peer, ce);
            }
        }
    }
}

#[derive(Debug)]
struct Stats {
    num_succ: usize,
    num_fail: usize,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Debug)]
struct U64(u64);

impl Weighted for U64 {
    fn weight(&self) -> usize {
        1
    }
}

#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[tokio::test]
async fn multicasting_normal() {
    let mut peers = multicasting::setup_nodes::<Statements<U64>>(17);
    let statement = Statements(vec![U64(0)]);
    let committee: Vec<(PeerId, Option<Multiaddr>)> = peers
        .iter()
        .map(
            |multicasting::Peer {
                 peer_addr, peer_id, ..
             }| (*peer_id, Some(peer_addr.clone())),
        )
        .collect();
    let overlay_builder = RedundancyDagOverlayBuilder {
        redundancy_factor: 3,
        seed: 64,
    };

    wasm_timer::Delay::new(Duration::from_secs(1)).await.unwrap();
    trace!("Starting testing ..");

    let root_pid = peers[0].peer_id;
    let mut result_futures = Vec::new();

    for peer in peers.iter_mut() {
        let overlay = overlay_builder.make(Some(root_pid), peer.peer_id, committee.clone());
        let (snd, recv) = oneshot::channel();
        result_futures.push(recv);
        let initial_statement = if peer.peer_id == root_pid {
            Some(statement.clone())
        } else {
            None
        };
        peer.aggr_handler_mailbox
            .clone()
            .send(SetTask {
                initial_statement,
                on_response: snd,
                overlay,
            })
            .await
            .unwrap();
        trace!("Assigned task to peer {:?}", peer.peer_addr);
    }

    let started_at = Instant::now();

    let stats = Arc::new(Mutex::new(Stats {
        num_fail: 0,
        num_succ: 0,
    }));

    for (ix, fut) in result_futures.into_iter().enumerate() {
        async_std::task::spawn({
            let stats = Arc::clone(&stats);
            async move {
                let res = fut.await;
                let finished_at = Instant::now();
                let elapsed = finished_at.sub(started_at);
                match res {
                    Ok(_) => {
                        trace!(
                            "[Peer-{}] :: Finished mcast in {} millis",
                            ix,
                            elapsed.as_millis()
                        );
                        stats.lock().await.num_succ += 1;
                    }
                    Err(_) => {
                        trace!("[Peer-{}] :: Failed mcast in {} millis", ix, elapsed.as_millis());
                        stats.lock().await.num_fail += 1;
                    }
                }
            }
        });
    }

    wasm_timer::Delay::new(Duration::from_secs(5)).await.unwrap();

    trace!("Timeout");

    for peer in &peers {
        peer.peer_handle.abort();
    }

    trace!("Finished. {:?}", stats.lock().await);
}

#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[tokio::test]
async fn sigma_aggregation_normal() {
    run_sigma_aggregation_test(16, vec![], Threshold { num: 16, denom: 16 }).await;
}

#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
#[tokio::test]
async fn sigma_aggregation_byzantine() {
    //init_logging_once_for(vec![], LevelFilter::Trace, None);
    //console_subscriber::init();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let total_runs = 5;
    let mut num_fails = 0;
    let mut total_passes = 0;
    for _ in 0..total_runs {
        let byzantine_nodes = vec![PeerIx::from(0), PeerIx::from(1), PeerIx::from(2), PeerIx::from(3)];
        let num_passes =
            run_sigma_aggregation_test(16, byzantine_nodes, Threshold { num: 2, denom: 3 }).await;
        if num_passes == 0 {
            num_fails += 1;
        } else {
            total_passes += num_passes;
        }
    }

    info!(
        "number of fails: {}, failure rate: {}%",
        num_fails,
        (num_fails as f64) / (total_runs as f64) * 100.0
    );
    info!(
        "average number of successful nodes per run: {}",
        (total_passes as f64) / (total_runs as f64)
    );
}

#[cfg_attr(feature = "test_peer_punish_too_slow", ignore)]
async fn run_sigma_aggregation_test(
    num_nodes: usize,
    byzantine_nodes: Vec<PeerIx>,
    threshold: Threshold,
) -> usize {
    let (peers, partitioner) = aggregation::setup_nodes(num_nodes, threshold);
    let md = blake2b256_hash(b"foo");
    let committee: HashMap<PublicKey, Option<Multiaddr>> = peers
        .iter()
        .map(
            |aggregation::Peer {
                 peer_addr, peer_pk, ..
             }| ((*peer_pk).into(), Some(peer_addr.clone())),
        )
        .collect();
    let peers_and_addr: Vec<_> = committee
        .iter()
        .map(|(pk, addr)| (PeerId::from(pk.clone()), addr.clone()))
        .collect();

    let mut aggr_handler_mailboxes = vec![];
    let mut abort_handles = vec![];
    for (node_ix, mut peer) in peers.into_iter().enumerate() {
        let peer_key = identity::Keypair::from(identity::secp256k1::Keypair::from(
            aggregation::k256_to_libsecp256k1(peer.peer_sk.clone()),
        ));
        let (abortable_peer, handle) = futures::future::abortable(aggregation::create_swarm(
            peer_key.clone(),
            peer.nc,
            peer.peer_addr.clone(),
            node_ix,
        ));

        let pk: PublicKey = peer.peer_pk.into();
        let host_pid = PeerId::from(pk);
        let partitions = partitioner.make(host_pid, peers_and_addr.clone());
        let host_ix = partitions.try_index_peer(host_pid).unwrap();

        if byzantine_nodes.contains(&host_ix) {
            trace!(
                "PEER:{} IS BYZANTINE ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",
                node_ix
            );
        } else {
            abort_handles.push(handle);
            aggr_handler_mailboxes.push(peer.aggr_handler_mailbox);
            tokio::task::spawn(async move {
                trace!("PEER:{} :: spawning protocol handler..", node_ix);
                loop {
                    peer.aggr_handler.select_next_some().await;
                }
            });
            tokio::task::spawn(async move {
                trace!("PEER:{} :: spawning peer..", node_ix);
                abortable_peer.await
            });
        }
    }

    wasm_timer::Delay::new(Duration::from_millis(100)).await.unwrap();
    let mut result_futures = Vec::new();
    for aggr_handler_mailbox in aggr_handler_mailboxes {
        let (snd, recv) = oneshot::channel();
        result_futures.push(recv);
        async_std::task::block_on(aggr_handler_mailbox.clone().send(AggregationAction::Reset {
            new_committee: committee.clone(),
            new_message: md,
            channel: snd,
        }))
        .unwrap();
    }

    let started_at = Instant::now();

    let num_finished = std::sync::Arc::new(futures::lock::Mutex::new(0));
    for (ix, fut) in result_futures.into_iter().enumerate() {
        let num_finished = num_finished.clone();
        async_std::task::spawn(async move {
            let res = fut.await;
            let finished_at = Instant::now();
            let elapsed = finished_at.sub(started_at);
            match res {
                Ok(_) => {
                    *num_finished.lock().await += 1;
                    trace!("PEER:{} :: Finished aggr in {} millis", ix, elapsed.as_millis())
                }
                Err(_) => trace!("PEER:{} :: Failed aggr in {} millis", ix, elapsed.as_millis()),
            }
        });
    }

    wasm_timer::Delay::new(Duration::from_millis(1200)).await.unwrap();

    for abort_handle in abort_handles {
        abort_handle.abort();
    }

    let num_finished = *num_finished.lock().await;
    trace!(
        "num_finished aggr: {}, {}%",
        num_finished,
        (num_finished as f64) / (num_nodes as f64) * 100.0
    );
    num_finished
}

fn make_swarm_components<P, F>(
    peers: Vec<PeerDestination>,
    gen_protocol_behaviour: F,
    msg_buffer_size: usize,
) -> (
    ProtocolHandler<P, NetworkMailbox>,
    NetworkController<PeersMailbox, PeerManager<PeerRepo>, ProtocolMailbox>,
)
where
    P: ProtocolBehaviour + Unpin + Send + 'static,
    F: FnOnce(PeersMailbox) -> P,
{
    let peer_conn_handler_conf = PeerConnHandlerConf {
        async_msg_buffer_size: msg_buffer_size,
        sync_msg_buffer_size: msg_buffer_size,
        open_timeout: Duration::from_secs(60),
        initial_keep_alive: Duration::from_secs(60),
    };
    let netw_config = NetworkingConfig {
        min_known_peers: 1,
        min_outbound: 1,
        max_inbound: 10,
        max_outbound: 20,
    };
    let peer_manager_conf = PeerManagerConfig {
        min_acceptable_reputation: Reputation::from(0),
        min_reputation: Reputation::from(0),
        conn_reset_outbound_backoff: Duration::from_secs(120),
        conn_alloc_interval: Duration::from_secs(30),
        prot_alloc_interval: Duration::from_secs(30),
        protocols_allocation: Vec::new(),
        peer_manager_msg_buffer_size: 10,
    };
    let peer_state = PeerRepo::new(netw_config, peers);
    let (peer_manager, peers) = PeerManager::new(peer_state, peer_manager_conf);
    let sync_conf = StatefulProtocolConfig {
        supported_versions: vec![(
            DiscoverySpec::v1(),
            StatefulProtocolSpec {
                max_message_size: 100,
                approve_required: true,
            },
        )],
    };

    let (requests_snd, requests_recv) = mpsc::channel::<NetworkControllerIn>(10);
    let network_api = NetworkMailbox {
        mailbox_snd: requests_snd,
    };
    let (sync_handler, sync_mailbox) = ProtocolHandler::new(
        gen_protocol_behaviour(peers.clone()),
        network_api,
        DISCOVERY_PROTOCOL_ID,
        10,
    );
    let nc = NetworkController::new(
        peer_conn_handler_conf,
        HashMap::from([(
            DISCOVERY_PROTOCOL_ID,
            (ProtocolConfig::Stateful(sync_conf), sync_mailbox),
        )]),
        peers,
        peer_manager,
        requests_recv,
    );

    (sync_handler, nc)
}

pub fn make_nc_without_protocol_handler(
    peers: Vec<PeerDestination>,
    protocols: HashMap<ProtocolId, (ProtocolConfig, ProtocolMailbox)>,
) -> (
    NetworkController<PeersMailbox, PeerManager<PeerRepo>, ProtocolMailbox>,
    Sender<NetworkControllerIn>,
) {
    let peer_conn_handler_conf = PeerConnHandlerConf {
        async_msg_buffer_size: 100,
        sync_msg_buffer_size: 100,
        open_timeout: Duration::from_secs(60),
        initial_keep_alive: Duration::from_secs(120),
    };
    let netw_config = NetworkingConfig {
        min_known_peers: 1,
        min_outbound: 1,
        max_inbound: 10,
        max_outbound: 20,
    };
    let peer_manager_conf = PeerManagerConfig {
        min_acceptable_reputation: Reputation::from(-50),
        min_reputation: Reputation::from(-20),
        conn_reset_outbound_backoff: Duration::from_secs(120),
        conn_alloc_interval: Duration::from_secs(30),
        prot_alloc_interval: Duration::from_secs(30),
        protocols_allocation: Vec::new(),
        peer_manager_msg_buffer_size: 1000,
    };
    let peer_state = PeerRepo::new(netw_config, peers);
    let (peer_manager, peers) = PeerManager::new(peer_state, peer_manager_conf);
    let (requests_snd, requests_recv) = mpsc::channel::<NetworkControllerIn>(100);
    let nc = NetworkController::new(
        peer_conn_handler_conf,
        protocols,
        peers,
        peer_manager,
        requests_recv,
    );
    (nc, requests_snd)
}
