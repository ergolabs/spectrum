use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use either::{Either, Left, Right};
use futures::channel::mpsc::{Receiver, Sender};
use futures::{SinkExt, Stream};
use libp2p::allow_block_list::{Behaviour, BlockedPeers};
use libp2p::core::Endpoint;
use libp2p::swarm::behaviour::ConnectionEstablished;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{
    CloseConnection, ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm,
    NetworkBehaviour, NotifyHandler, PollParameters, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{info, trace, warn};

use crate::one_shot_upgrade::OneShotMessage;
use crate::peer_conn_handler::message_sink::MessageSink;
use crate::peer_conn_handler::{
    ConnHandlerError, ConnHandlerIn, ConnHandlerOut, OneShotProtocol, OneShotRequest, OneShotRequestId,
    PeerConnHandler, PeerConnHandlerConf, ProtocolState, StatefulProtocol, ThrottleStage,
};
use crate::peer_manager::data::{ConnectionLossReason, ReputationChange};
use crate::peer_manager::{PeerEvents, PeerManagerOut, Peers};
use crate::protocol::{OneShotProtocolConfig, OneShotProtocolSpec, ProtocolConfig, StatefulProtocolConfig};
use crate::protocol_api::ProtocolEvents;
use crate::protocol_upgrade::handshake::PolyVerHandshakeSpec;
use crate::types::{ProtocolId, ProtocolTag, ProtocolVer, RawMessage};

/// States of an enabled protocol.
#[derive(Debug)]
pub enum EnabledProtocol {
    /// Bi-directional communication on this protocol is enabled.
    Enabled { ver: ProtocolVer, sink: MessageSink },
    /// Substreams for this protocol are requested by peer.
    PendingApprove,
    /// Substreams for this protocol are requested.
    PendingEnable,
    /// Waiting for the substreams to be closed.
    PendingDisable,
}

/// States of a connected peer.
/// `PendingConnect` -> `Connected`
/// `PendingApprove` -> `Connected`
/// `Connected`      -> `PendingDisconnect`
pub enum ConnectedPeer<THandler> {
    /// We are connected to this peer.
    Connected {
        /// Note that we can have multiple connections with each peer.
        conn_ids: Vec<ConnectionId>,
        enabled_protocols: HashMap<ProtocolId, (EnabledProtocol, THandler)>,
    },
    /// The peer is connected but not approved by PM yet.
    PendingApprove(ConnectionId),
    /// PM or Protocol requested that we should connect to this peer.
    PendingConnect {
        /// One-shot messages that the handler should try to deliver once connected.
        tasks: Vec<OneShotMessage>,
        /// Should the handler terminate as soon as possible when no work left.
        terminate_asap: bool,
    },
    /// PM or Protocol requested that we should disconnect this peer.
    PendingDisconnect(ConnectionId),
}

/// Outbound network events.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum NetworkControllerOut {
    /// Connected with peer, initiated by external peer (inbound connection).
    ConnectedWithInboundPeer(PeerId),
    /// Connected with peer, initiated by us (outbound connection).
    ConnectedWithOutboundPeer(PeerId),
    Disconnected {
        peer_id: PeerId,
        reason: ConnectionLossReason,
    },
    ProtocolPendingApprove {
        peer_id: PeerId,
        protocol_id: ProtocolId,
    },
    ProtocolPendingEnable {
        peer_id: PeerId,
        protocol_id: ProtocolId,
    },
    ProtocolEnabled {
        peer_id: PeerId,
        protocol_id: ProtocolId,
        protocol_ver: ProtocolVer,
    },
    ProtocolDisabled {
        peer_id: PeerId,
        protocol_id: ProtocolId,
    },
    PeerPunished {
        peer_id: PeerId,
        reason: ReputationChange,
    },
}

pub enum NetworkControllerIn {
    /// A directive to enable the specified protocol with the specified peer.
    EnableProtocol {
        /// The desired protocol.
        protocol: ProtocolId,
        /// A specific peer we should start the protocol with.
        peer: PeerId,
        /// A handshake to send to the peer upon negotiation of protocol substream.
        handshake: PolyVerHandshakeSpec,
    },
    /// A directive to update the set of protocols supported by the specified peer.
    UpdatePeerProtocols {
        peer: PeerId,
        protocols: Vec<ProtocolId>,
    },
    /// Send the given message to the specified peer without
    /// establishing a persistent two-way communication channel.
    SendOneShotMessage {
        peer: PeerId,
        addr_hint: Option<Multiaddr>,
        protocol: ProtocolTag,
        message: RawMessage,
    },
    /// Ban peer permanently.
    BanPeer(PeerId),
}

/// External API to network controller.
pub trait NetworkAPI {
    /// Enables the specified protocol with the specified peer.
    fn enable_protocol(&self, protocol: ProtocolId, peer: PeerId, handshake: PolyVerHandshakeSpec);

    /// Updates the set of protocols supported by the specified peer.
    fn update_peer_protocols(&self, peer: PeerId, protocols: Vec<ProtocolId>);
    /// Send the given message to the specified peer without
    /// establishing a persistent two-way communication channel.
    fn send_one_shot_message(
        &self,
        peer: PeerId,
        addr_hint: Option<Multiaddr>,
        protocol: ProtocolTag,
        message: RawMessage,
    );
    /// Ban peer permanently.
    fn ban_peer(&self, peer: PeerId);
}

#[derive(Clone)]
pub struct NetworkMailbox {
    pub mailbox_snd: Sender<NetworkControllerIn>,
}

impl NetworkAPI for NetworkMailbox {
    fn enable_protocol(&self, protocol: ProtocolId, peer: PeerId, handshake: PolyVerHandshakeSpec) {
        let _ = futures::executor::block_on(self.mailbox_snd.clone().send(
            NetworkControllerIn::EnableProtocol {
                protocol,
                peer,
                handshake,
            },
        ));
    }
    fn update_peer_protocols(&self, peer: PeerId, protocols: Vec<ProtocolId>) {
        let _ = futures::executor::block_on(
            self.mailbox_snd
                .clone()
                .send(NetworkControllerIn::UpdatePeerProtocols { peer, protocols }),
        );
    }
    fn send_one_shot_message(
        &self,
        peer: PeerId,
        addr_hint: Option<Multiaddr>,
        protocol: ProtocolTag,
        message: RawMessage,
    ) {
        let _ = futures::executor::block_on({
            self.mailbox_snd
                .clone()
                .send(NetworkControllerIn::SendOneShotMessage {
                    peer,
                    addr_hint,
                    protocol,
                    message,
                })
        });
    }
    fn ban_peer(&self, peer: PeerId) {
        let _ =
            futures::executor::block_on(self.mailbox_snd.clone().send(NetworkControllerIn::BanPeer(peer)));
    }
}

/// API to events emitted by the network (swarm in our case).
pub trait NetworkEvents {
    /// Connected with peer, initiated by external peer (inbound connection).
    fn inbound_peer_connected(&mut self, peer_id: PeerId);
    /// Connected with peer, initiated by us (outbound connection).
    fn outbound_peer_connected(&mut self, peer_id: PeerId);
    fn peer_disconnected(&mut self, peer_id: PeerId, reason: ConnectionLossReason);
    fn peer_punished(&mut self, peer_id: PeerId, reason: ReputationChange);
    fn protocol_pending_approve(&mut self, peer_id: PeerId, protocol_id: ProtocolId);
    fn protocol_pending_enable(&mut self, peer_id: PeerId, protocol_id: ProtocolId);
    fn protocol_enabled(&mut self, peer_id: PeerId, protocol_id: ProtocolId, protocol_ver: ProtocolVer);
    fn protocol_disabled(&mut self, peer_id: PeerId, protocol_id: ProtocolId);
}

impl<TPeers, TPeerManager, THandler> NetworkEvents for NetworkController<TPeers, TPeerManager, THandler> {
    fn inbound_peer_connected(&mut self, peer_id: PeerId) {
        self.pending_actions.push_back(ToSwarm::GenerateEvent(
            NetworkControllerOut::ConnectedWithInboundPeer(peer_id),
        ));
    }

    fn outbound_peer_connected(&mut self, peer_id: PeerId) {
        self.pending_actions.push_back(ToSwarm::GenerateEvent(
            NetworkControllerOut::ConnectedWithOutboundPeer(peer_id),
        ));
    }

    fn peer_disconnected(&mut self, peer_id: PeerId, reason: ConnectionLossReason) {
        self.pending_actions
            .push_back(ToSwarm::GenerateEvent(NetworkControllerOut::Disconnected {
                peer_id,
                reason,
            }));
    }

    fn peer_punished(&mut self, peer_id: PeerId, reason: ReputationChange) {
        self.pending_actions
            .push_back(ToSwarm::GenerateEvent(NetworkControllerOut::PeerPunished {
                peer_id,
                reason,
            }));
    }

    fn protocol_enabled(&mut self, peer_id: PeerId, protocol_id: ProtocolId, protocol_ver: ProtocolVer) {
        self.pending_actions
            .push_back(ToSwarm::GenerateEvent(NetworkControllerOut::ProtocolEnabled {
                peer_id,
                protocol_id,
                protocol_ver,
            }));
    }

    fn protocol_pending_approve(&mut self, peer_id: PeerId, protocol_id: ProtocolId) {
        self.pending_actions.push_back(ToSwarm::GenerateEvent(
            NetworkControllerOut::ProtocolPendingApprove { peer_id, protocol_id },
        ));
    }

    fn protocol_pending_enable(&mut self, peer_id: PeerId, protocol_id: ProtocolId) {
        self.pending_actions.push_back(ToSwarm::GenerateEvent(
            NetworkControllerOut::ProtocolPendingEnable { peer_id, protocol_id },
        ));
    }

    fn protocol_disabled(&mut self, peer_id: PeerId, protocol_id: ProtocolId) {
        self.pending_actions
            .push_back(ToSwarm::GenerateEvent(NetworkControllerOut::ProtocolDisabled {
                peer_id,
                protocol_id,
            }));
    }
}

pub struct NetworkController<TPeers, TPeerManager, THandler> {
    conn_handler_conf: PeerConnHandlerConf,
    /// All supported protocols and their handlers
    supported_protocols: HashMap<ProtocolId, (ProtocolConfig, THandler)>,
    /// PeerManager API
    peers: TPeers,
    /// PeerManager stream itself
    peer_manager: TPeerManager,
    enabled_peers: HashMap<PeerId, ConnectedPeer<THandler>>,
    /// Pending one-shot messages awaiting a dialing before being sent
    pending_one_shot_requests: HashMap<PeerId, OneShotMessage>,
    requests_recv: Receiver<NetworkControllerIn>,
    pending_actions: VecDeque<ToSwarm<NetworkControllerOut, ConnHandlerIn>>,
    blocked_list: Behaviour<BlockedPeers>,
}

impl<TPeers, TPeerManager, THandler> NetworkController<TPeers, TPeerManager, THandler>
where
    THandler: Clone,
{
    pub fn new(
        conn_handler_conf: PeerConnHandlerConf,
        supported_protocols: HashMap<ProtocolId, (ProtocolConfig, THandler)>,
        peers: TPeers,
        peer_manager: TPeerManager,
        requests_recv: Receiver<NetworkControllerIn>,
    ) -> Self {
        Self {
            conn_handler_conf,
            supported_protocols,
            peers,
            peer_manager,
            enabled_peers: HashMap::new(),
            pending_one_shot_requests: HashMap::new(),
            requests_recv,
            pending_actions: VecDeque::new(),
            blocked_list: Behaviour::default(),
        }
    }

    fn init_conn_handler(
        &self,
        peer_id: PeerId,
        one_shot_requests: Vec<OneShotMessage>,
        terminate_asap: bool,
    ) -> PeerConnHandler {
        let mut stateful_protocols = HashMap::new();
        let mut one_shot_protocols = HashMap::new();
        for (protocol_id, (p, _)) in self.supported_protocols.iter() {
            match p {
                ProtocolConfig::Stateful(stateful) => {
                    for (ver, spec) in stateful.supported_versions.iter() {
                        stateful_protocols.insert(
                            *protocol_id,
                            StatefulProtocol {
                                ver: *ver,
                                spec: *spec,
                                state: Some(ProtocolState::Closed),
                                all_versions_specs: stateful.supported_versions.clone(),
                            },
                        );
                    }
                }
                ProtocolConfig::OneShot(one_shot) => {
                    one_shot_protocols.insert(
                        *protocol_id,
                        OneShotProtocol {
                            ver: one_shot.version,
                            spec: one_shot.spec.clone(),
                        },
                    );
                }
            }
        }
        #[cfg(not(feature = "test_peer_punish_too_slow"))]
        let throttle_recv = ThrottleStage::Disable;
        #[cfg(feature = "test_peer_punish_too_slow")]
        let throttle_recv = ThrottleStage::Start;

        PeerConnHandler {
            conf: self.conn_handler_conf.clone(),
            stateful_protocols,
            one_shot_protocols,
            created_at: Instant::now(),
            peer_id,
            pending_events: VecDeque::new(),
            fault: None,
            delay: wasm_timer::Delay::new(Duration::from_millis(300)),
            throttle_stage: throttle_recv,
            pending_one_shots: one_shot_requests
                .into_iter()
                .map(|msg| (OneShotRequestId::random(), OneShotRequest::Pending(msg)))
                .collect(),
            terminate_asap,
        }
    }
}

impl<TPeers, TPeerManager, THandler> NetworkBehaviour for NetworkController<TPeers, TPeerManager, THandler>
where
    TPeers: PeerEvents + Peers + 'static,
    TPeerManager: Stream<Item = PeerManagerOut> + Unpin + 'static,
    THandler: ProtocolEvents + Clone + 'static,
{
    type ConnectionHandler = PeerConnHandler;
    type ToSwarm = NetworkControllerOut;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        Ok(self.init_conn_handler(peer, vec![], false))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _addr: &Multiaddr,
        _role_override: Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        match self.enabled_peers.get(&peer) {
            Some(ConnectedPeer::PendingConnect {
                tasks,
                terminate_asap,
                ..
            }) => Ok(self.init_conn_handler(peer, tasks.clone(), *terminate_asap)),
            _ => Ok(self.init_conn_handler(peer, vec![], false)),
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            }) => {
                match self.enabled_peers.entry(peer_id) {
                    Entry::Occupied(mut peer_entry) => match peer_entry.get_mut() {
                        ConnectedPeer::PendingConnect { tasks, .. } => {
                            for os_msg in tasks {
                                self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                    peer_id,
                                    handler: NotifyHandler::One(connection_id),
                                    event: ConnHandlerIn::TryDeliverOnce(OneShotMessage {
                                        protocol: os_msg.protocol,
                                        content: os_msg.content.clone(),
                                    }),
                                });
                            }
                            self.peers.connection_established(peer_id, connection_id); // confirm connection
                            peer_entry.insert(ConnectedPeer::Connected {
                                conn_ids: vec![connection_id],
                                enabled_protocols: HashMap::new(),
                            });
                            // notify all handlers about new connection.
                            for (_, ph) in self.supported_protocols.values() {
                                ph.connected(peer_id);
                            }
                            self.outbound_peer_connected(peer_id);
                        }
                        ConnectedPeer::Connected { conn_ids, .. } => {
                            assert!(!conn_ids.contains(&connection_id));
                            conn_ids.push(connection_id);
                            self.pending_actions.push_back(ToSwarm::CloseConnection {
                                peer_id,
                                connection: CloseConnection::One(connection_id),
                            })
                        }
                        ConnectedPeer::PendingDisconnect(..) => {
                            self.pending_actions.push_back(ToSwarm::CloseConnection {
                                peer_id,
                                connection: CloseConnection::One(connection_id),
                            })
                        }
                        ConnectedPeer::PendingApprove(..) => {
                            self.pending_actions.push_back(ToSwarm::CloseConnection {
                                peer_id,
                                connection: CloseConnection::One(connection_id),
                            })
                        }
                    },
                    Entry::Vacant(entry) => {
                        trace!("[NC] Observing new inbound connection {}", peer_id);
                        self.peers.incoming_connection(peer_id, connection_id);
                        entry.insert(ConnectedPeer::PendingApprove(connection_id));
                    }
                }
            }

            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                handler,
                ..
            }) => {
                let disconnect_reason = match self.enabled_peers.entry(peer_id) {
                    Entry::Occupied(mut peer_entry) => match peer_entry.get_mut() {
                        ConnectedPeer::Connected { conn_ids, .. } => {
                            let ix = conn_ids.iter().position(|c_id| *c_id == connection_id).unwrap();
                            conn_ids.remove(ix);
                            if conn_ids.is_empty() {
                                peer_entry.remove();
                            }
                            if let Some(err) = handler.get_fault() {
                                let reason = ConnectionLossReason::Reset(err);
                                self.peers.connection_lost(peer_id, reason);
                                Some(reason)
                            } else {
                                let reason = ConnectionLossReason::ResetByPeer;
                                self.peers.connection_lost(peer_id, reason);
                                Some(reason)
                            }
                        }

                        ConnectedPeer::PendingDisconnect(..) => {
                            peer_entry.remove();
                            if let Some(err) = handler.get_fault() {
                                let reason = ConnectionLossReason::Reset(err);
                                self.peers.connection_lost(peer_id, reason);
                                Some(reason)
                            } else {
                                let reason = ConnectionLossReason::ResetByPeer;
                                self.peers.connection_lost(peer_id, reason);
                                Some(reason)
                            }
                        }
                        // todo: is it possible in case of simultaneous connection?
                        ConnectedPeer::PendingConnect { .. } | ConnectedPeer::PendingApprove(..) => None,
                    },
                    Entry::Vacant(_) => None,
                };
                if let Some(reason) = disconnect_reason {
                    info!("Disconnecting from {:?}, reason: {:?}", peer_id, reason);
                    self.peer_disconnected(peer_id, reason);
                }
            }

            FromSwarm::DialFailure(DialFailure { peer_id, error, .. }) => {
                info!("[NC] DIAL FAILURE to {:?}, error: {:?}", peer_id, error);
                if let Some(peer_id) = peer_id {
                    self.peers.dial_failure(peer_id);
                }
            }

            FromSwarm::AddressChange(_) => {}
            FromSwarm::ListenFailure(e) => {
                info!("[NC] ListenFailure({:?}", e.error);
            }
            FromSwarm::NewListener(_) => {
                info!("[NC] NewListener");
            }
            FromSwarm::NewListenAddr(_) => {
                info!("[NC] NewListenAddr");
            }
            FromSwarm::ExpiredListenAddr(_) => {
                info!("[NC] ExpiredListenAddr");
            }
            FromSwarm::ListenerError(e) => {
                info!("[NC] ListenerFailure({}", e.err);
            }
            FromSwarm::ListenerClosed(e) => {
                info!("[NC] ListerClosed({:?}", e.reason);
            }
            FromSwarm::NewExternalAddrCandidate(e) => {
                info!("[NC] NewExternalAddrCandidate({:?}", e.addr);
            }
            FromSwarm::ExternalAddrConfirmed(e) => {
                info!("[NC] ExternalAddrConfirmed({:?}", e.addr);
            }
            FromSwarm::ExternalAddrExpired(e) => {
                info!("[NC] ExternalAddrExpired({:?}", e.addr);
            }
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: ConnHandlerOut,
    ) {
        match event {
            ConnHandlerOut::Opened {
                protocol_tag,
                out_channel,
                handshake,
            } => {
                trace!("Protocol {} opened with peer {}", protocol_tag, peer_id);
                if let Some(ConnectedPeer::Connected {
                    enabled_protocols, ..
                }) = self.enabled_peers.get_mut(&peer_id)
                {
                    let protocol_id = protocol_tag.protocol_id();
                    let protocol_ver = protocol_tag.protocol_ver();
                    match enabled_protocols.entry(protocol_id) {
                        Entry::Occupied(mut entry) => {
                            trace!(
                                "Current state of protocol {:?} is {:?}",
                                protocol_id,
                                entry.get().0
                            );
                            if let (EnabledProtocol::PendingEnable, handler) = entry.get() {
                                handler.protocol_enabled(
                                    peer_id,
                                    protocol_ver,
                                    out_channel.clone(),
                                    handshake,
                                );
                                let enabled_protocol = EnabledProtocol::Enabled {
                                    ver: protocol_ver,
                                    sink: out_channel,
                                };
                                entry.insert((enabled_protocol, handler.clone()));
                                self.protocol_enabled(peer_id, protocol_id, protocol_ver);
                            }
                        }
                        Entry::Vacant(entry) => {
                            warn!("Unknown protocol was opened {:?}", entry.key())
                        }
                    }
                }
            }
            ConnHandlerOut::OpenedByPeer {
                protocol_tag,
                handshake,
            } => {
                if let Some(peer) = self.enabled_peers.get_mut(&peer_id) {
                    match peer {
                        ConnectedPeer::Connected {
                            enabled_protocols, ..
                        } => {
                            trace!("Connection opened by {:?} in Connected state", peer_id);
                            let protocol_id = protocol_tag.protocol_id();
                            let (_, prot_handler) = self.supported_protocols.get(&protocol_id).unwrap();
                            match enabled_protocols.entry(protocol_id) {
                                Entry::Vacant(entry) => {
                                    entry.insert((EnabledProtocol::PendingApprove, prot_handler.clone()));
                                    prot_handler.protocol_requested(
                                        peer_id,
                                        protocol_tag.protocol_ver(),
                                        handshake,
                                    );
                                    self.protocol_pending_approve(peer_id, protocol_id);
                                }
                                Entry::Occupied(_) => {
                                    warn!(
                                        "Peer {:?} opened already enabled protocol {:?}",
                                        peer_id, protocol_id
                                    );
                                    self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                        peer_id,
                                        handler: NotifyHandler::One(connection),
                                        event: ConnHandlerIn::Close(protocol_id),
                                    })
                                }
                            }
                        }
                        ConnectedPeer::PendingApprove(_) => {
                            trace!("Connection opened by {:?} in PendingApprove state", peer_id);
                        }
                        ConnectedPeer::PendingConnect { .. } => {
                            trace!("Connection opened by {:?} in PendingConnect state", peer_id);
                        }
                        ConnectedPeer::PendingDisconnect(_) => {
                            trace!("Connection opened by {:?} in PendingDisconnect state", peer_id);
                        }
                    }
                } else {
                    trace!("Connection opened by {:?}, not in enabled peers", peer_id);
                }
            }
            ConnHandlerOut::ClosedByPeer(protocol_id)
            | ConnHandlerOut::RefusedToOpen(protocol_id)
            | ConnHandlerOut::Closed(protocol_id) => {
                if let Some(ConnectedPeer::Connected {
                    enabled_protocols, ..
                }) = self.enabled_peers.get_mut(&peer_id)
                {
                    match enabled_protocols.entry(protocol_id) {
                        Entry::Occupied(entry) => {
                            trace!(
                                "Peer {:?} closed the substream for protocol {:?}",
                                peer_id,
                                protocol_id
                            );
                            entry.remove();
                        }
                        Entry::Vacant(_) => {}
                    }
                }
            }
            ConnHandlerOut::ClosedAllProtocols => {
                assert!(self.enabled_peers.remove(&peer_id).is_some());
            }
            ConnHandlerOut::OneShotMessage {
                protocol_tag,
                content,
            } => {
                if let Some((_, han)) = self.supported_protocols.get(&protocol_tag.protocol_id()) {
                    han.incoming_msg(peer_id, protocol_tag.protocol_ver(), content);
                }
                // todo: punish peer for spam otherwise?
            }
            ConnHandlerOut::Message {
                protocol_tag,
                content,
            } => {
                if let Some(ConnectedPeer::Connected {
                    enabled_protocols, ..
                }) = self.enabled_peers.get_mut(&peer_id)
                {
                    let protocol_id = protocol_tag.protocol_id();
                    match enabled_protocols.get(&protocol_id) {
                        Some((_, prot_handler)) => {
                            prot_handler.incoming_msg(peer_id, protocol_tag.protocol_ver(), content);
                        }
                        None => {} // todo: probably possible?
                    };
                }
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<NetworkControllerOut, ConnHandlerIn>> {
        loop {
            // 1. Try to return a pending action.
            if let Some(action) = self.pending_actions.pop_front() {
                return Poll::Ready(action);
            };
            // 2. Poll for instructions from PM.
            match Stream::poll_next(Pin::new(&mut self.peer_manager), cx) {
                Poll::Ready(Some(PeerManagerOut::Connect(pid))) => {
                    match self.enabled_peers.entry(pid.peer_id()) {
                        Entry::Occupied(_) => {}
                        Entry::Vacant(peer_entry) => {
                            peer_entry.insert(ConnectedPeer::PendingConnect {
                                tasks: Vec::new(),
                                terminate_asap: false,
                            });
                            self.pending_actions.push_back(ToSwarm::Dial { opts: pid.into() })
                        }
                    }
                    continue;
                }
                Poll::Ready(Some(PeerManagerOut::Drop(peer_id))) => {
                    if let Some(ConnectedPeer::Connected { conn_ids, .. }) =
                        self.enabled_peers.get_mut(&peer_id)
                    {
                        self.pending_actions.push_back(ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::One(*conn_ids.first().unwrap()),
                            event: ConnHandlerIn::CloseAllProtocols,
                        });
                        self.peer_disconnected(
                            peer_id,
                            ConnectionLossReason::Reset(ConnHandlerError::UnacceptablePeer),
                        );
                    }
                    continue;
                }
                Poll::Ready(Some(PeerManagerOut::AcceptIncomingConnection(pid, cid))) => {
                    match self.enabled_peers.entry(pid) {
                        Entry::Occupied(mut peer) => {
                            if let ConnectedPeer::PendingApprove(_) = peer.get() {
                                trace!("Inbound connection from peer {} accepted", pid);
                                peer.insert(ConnectedPeer::Connected {
                                    conn_ids: vec![cid],
                                    enabled_protocols: HashMap::new(),
                                });
                                self.inbound_peer_connected(pid);
                            }
                        }
                        Entry::Vacant(_) => {}
                    }
                }
                Poll::Ready(Some(PeerManagerOut::Reject(pid, cid))) => {
                    match self.enabled_peers.entry(pid) {
                        Entry::Occupied(peer) => {
                            if let ConnectedPeer::PendingApprove(_) = peer.get() {
                                trace!("Inbound connection from peer {} rejected", pid);
                                peer.remove();
                                self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                    peer_id: pid,
                                    handler: NotifyHandler::One(cid),
                                    event: ConnHandlerIn::CloseAllProtocols,
                                })
                            }
                        }
                        Entry::Vacant(_) => {}
                    }
                    continue;
                }
                Poll::Ready(Some(PeerManagerOut::StartProtocol(protocol, pid))) => {
                    match self.enabled_peers.entry(pid) {
                        Entry::Occupied(mut peer) => {
                            let peer = peer.get_mut();
                            match peer {
                                ConnectedPeer::Connected {
                                    enabled_protocols, ..
                                } => {
                                    let (_, prot_handler) = self.supported_protocols.get(&protocol).unwrap();
                                    match enabled_protocols.entry(protocol) {
                                        Entry::Occupied(_) => warn!(
                                            "PM requested already enabled protocol {:?} with peer {:?}",
                                            protocol, pid
                                        ),
                                        Entry::Vacant(protocol_entry) => {
                                            protocol_entry.insert((
                                                EnabledProtocol::PendingEnable,
                                                prot_handler.clone(),
                                            ));
                                            prot_handler.protocol_requested_local(pid);
                                            self.protocol_pending_enable(pid, protocol);
                                        }
                                    };
                                }
                                ConnectedPeer::PendingConnect { .. }
                                | ConnectedPeer::PendingApprove(_)
                                | ConnectedPeer::PendingDisconnect(_) => {}
                            }
                        }
                        Entry::Vacant(_) => {}
                    }
                    continue;
                }
                Poll::Ready(Some(PeerManagerOut::NotifyPeerPunished { peer_id, reason })) => {
                    self.peer_punished(peer_id, reason);
                    continue;
                }
                Poll::Pending => {}
                Poll::Ready(None) => unreachable!("PeerManager should never terminate"),
            }

            // 3. Poll commands from protocol handlers.
            if let Poll::Ready(Some(input)) = Stream::poll_next(Pin::new(&mut self.requests_recv), cx) {
                match input {
                    NetworkControllerIn::SendOneShotMessage {
                        peer,
                        addr_hint,
                        protocol,
                        message,
                    } => match self.enabled_peers.entry(peer) {
                        Entry::Occupied(mut enabled_peer) => match enabled_peer.get_mut() {
                            ConnectedPeer::Connected { conn_ids, .. } => {
                                // if the peer is enabled already we choose existing connection
                                self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                    peer_id: peer,
                                    handler: NotifyHandler::One(*conn_ids.first().unwrap()),
                                    event: ConnHandlerIn::TryDeliverOnce(OneShotMessage {
                                        protocol,
                                        content: message,
                                    }),
                                })
                            }
                            ConnectedPeer::PendingApprove(conn_id) => {
                                // if the peer is enabled already we reuse existing connection
                                self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                    peer_id: peer,
                                    handler: NotifyHandler::One(*conn_id),
                                    event: ConnHandlerIn::TryDeliverOnce(OneShotMessage {
                                        protocol,
                                        content: message,
                                    }),
                                })
                            }
                            ConnectedPeer::PendingConnect {
                                tasks: adjacent_tasks,
                                ..
                            } => {
                                // if we are going to connect it anyway then we add an adjacent task
                                adjacent_tasks.push(OneShotMessage {
                                    protocol,
                                    content: message,
                                });
                                info!(
                                    "[NC] adding to adjacent task {:?}, # adjacent_tasks: {}",
                                    peer,
                                    adjacent_tasks.len()
                                );
                            }
                            ConnectedPeer::PendingDisconnect(_) => {
                                info!("[NC] FAILED OS to pending-disconnected-peer {:?}", peer);
                            } // todo: wait for disconnect; reconnect?
                        },
                        Entry::Vacant(not_enabled_peer) => {
                            self.pending_actions.push_back(ToSwarm::Dial {
                                opts: DialOpts::peer_id(peer)
                                    .addresses(addr_hint.map_or(Vec::new(), |a| vec![a]))
                                    .build(),
                            });
                            not_enabled_peer.insert(ConnectedPeer::PendingConnect {
                                tasks: vec![OneShotMessage {
                                    protocol,
                                    content: message,
                                }],
                                terminate_asap: true,
                            });
                        }
                    },
                    NetworkControllerIn::UpdatePeerProtocols { peer, protocols } => {
                        self.peers.set_peer_protocols(peer, protocols);
                    }
                    NetworkControllerIn::EnableProtocol {
                        peer: peer_id,
                        protocol: protocol_id,
                        handshake,
                    } => {
                        if let Some(ConnectedPeer::Connected {
                            conn_ids,
                            enabled_protocols,
                        }) = self.enabled_peers.get_mut(&peer_id)
                        {
                            let (_, prot_handler) = self.supported_protocols.get(&protocol_id).unwrap();
                            match enabled_protocols.entry(protocol_id) {
                                Entry::Occupied(protocol_entry) => match protocol_entry.remove_entry().1 {
                                    // Protocol handler approves either outbound or inbound protocol request.
                                    (
                                        EnabledProtocol::PendingEnable | EnabledProtocol::PendingApprove,
                                        handler,
                                    ) => {
                                        enabled_protocols
                                            .insert(protocol_id, (EnabledProtocol::PendingEnable, handler));
                                        self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                            peer_id,
                                            handler: NotifyHandler::One(*conn_ids.first().unwrap()),
                                            event: ConnHandlerIn::Open {
                                                protocol_id,
                                                handshake,
                                            },
                                        });
                                    }
                                    (
                                        st @ (EnabledProtocol::Enabled { .. }
                                        | EnabledProtocol::PendingDisable),
                                        handler,
                                    ) => {
                                        warn!("Handler requested to open already enabled protocol {:?} with peer {:?}", protocol_id, peer_id);
                                        enabled_protocols.insert(protocol_id, (st, handler));
                                    }
                                },
                                // Also, Protocol Handler can request a substream on its own.
                                Entry::Vacant(protocol_entry) => {
                                    trace!(
                                        "Handler requested to open protocol {:?} with peer {:?}",
                                        protocol_id,
                                        peer_id
                                    );
                                    protocol_entry
                                        .insert((EnabledProtocol::PendingEnable, prot_handler.clone()));
                                    self.peers.force_enabled(peer_id, protocol_id); // notify PM
                                    self.pending_actions.push_back(ToSwarm::NotifyHandler {
                                        peer_id,
                                        handler: NotifyHandler::One(*conn_ids.first().unwrap()),
                                        event: ConnHandlerIn::Open {
                                            protocol_id,
                                            handshake,
                                        },
                                    });
                                    self.protocol_pending_enable(peer_id, protocol_id);
                                }
                            }
                        }
                    }
                    NetworkControllerIn::BanPeer(pid) => {
                        self.blocked_list.block_peer(pid);
                    }
                }
                continue;
            }

            return Poll::Pending;
        }
    }
}
