use crate::peer_conn_handler::message_sink::MessageSink;
use crate::peer_conn_handler::{
    ConnHandlerError, ConnHandlerIn, ConnHandlerOut, PartialPeerConnHandler, PeerConnHandlerConf,
};
use crate::peer_manager::{PeerEvents, PeerManagerOut, Peers};
use crate::protocol::ProtocolConfig;
use crate::protocol_api::ProtocolEvents;
use crate::protocol_upgrade::supported_protocol_vers::SupportedProtocolIdMap;
use crate::types::{ProtocolId, ProtocolVer};

use libp2p::core::connection::ConnectionId;
use libp2p::core::ConnectedPoint;
use libp2p::swarm::{
    CloseConnection, DialError, IntoConnectionHandler, NetworkBehaviour, NetworkBehaviourAction,
    NotifyHandler, PollParameters,
};
use libp2p::{Multiaddr, PeerId};

use log::{trace, warn};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::peer_manager::data::{ConnectionLossReason, ReputationChange};
use crate::protocol_upgrade::handshake::PolyVerHandshakeSpec;
use futures::channel::mpsc::{Receiver, Sender};
use futures::{SinkExt, Stream};

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
pub enum ConnectedPeer<THandler> {
    /// We are connected to this peer.
    Connected {
        conn_id: ConnectionId,
        enabled_protocols: HashMap<ProtocolId, (EnabledProtocol, THandler)>,
    },
    /// The peer is connected but not approved by PM yet.
    PendingApprove(ConnectionId),
    /// PM requested that we should connect to this peer.
    PendingConnect,
    /// PM requested that we should disconnect this peer.
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
}

/// External API to network controller.
pub trait NetworkAPI {
    /// Enables the specified protocol with the specified peer.
    fn enable_protocol(&self, protocol: ProtocolId, peer: PeerId, handshake: PolyVerHandshakeSpec);

    /// Updates the set of protocols supported by the specified peer.
    fn update_peer_protocols(&self, peer: PeerId, protocols: Vec<ProtocolId>);
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
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::ConnectedWithInboundPeer(peer_id),
            ));
    }

    fn outbound_peer_connected(&mut self, peer_id: PeerId) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::ConnectedWithOutboundPeer(peer_id),
            ));
    }

    fn peer_disconnected(&mut self, peer_id: PeerId, reason: ConnectionLossReason) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::Disconnected { peer_id, reason },
            ));
    }

    fn peer_punished(&mut self, peer_id: PeerId, reason: ReputationChange) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::PeerPunished { peer_id, reason },
            ));
    }

    fn protocol_enabled(&mut self, peer_id: PeerId, protocol_id: ProtocolId, protocol_ver: ProtocolVer) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::ProtocolEnabled {
                    peer_id,
                    protocol_id,
                    protocol_ver,
                },
            ));
    }

    fn protocol_pending_approve(&mut self, peer_id: PeerId, protocol_id: ProtocolId) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::ProtocolPendingApprove { peer_id, protocol_id },
            ));
    }

    fn protocol_pending_enable(&mut self, peer_id: PeerId, protocol_id: ProtocolId) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::ProtocolPendingEnable { peer_id, protocol_id },
            ));
    }

    fn protocol_disabled(&mut self, peer_id: PeerId, protocol_id: ProtocolId) {
        self.pending_actions
            .push_back(NetworkBehaviourAction::GenerateEvent(
                NetworkControllerOut::ProtocolDisabled { peer_id, protocol_id },
            ));
    }
}

pub struct NetworkController<TPeers, TPeerManager, THandler> {
    conn_handler_conf: PeerConnHandlerConf,
    /// All supported protocols and their handlers
    supported_protocols: SupportedProtocolIdMap<(ProtocolConfig, THandler)>,
    /// PeerManager API
    peers: TPeers,
    /// PeerManager stream itself
    peer_manager: TPeerManager,
    enabled_peers: HashMap<PeerId, ConnectedPeer<THandler>>,
    requests_recv: Receiver<NetworkControllerIn>,
    pending_actions: VecDeque<NetworkBehaviourAction<NetworkControllerOut, PartialPeerConnHandler>>,
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
            supported_protocols: supported_protocols.into(),
            peers,
            peer_manager,
            enabled_peers: HashMap::new(),
            requests_recv,
            pending_actions: VecDeque::new(),
        }
    }

    fn init_handler(&self) -> PartialPeerConnHandler {
        PartialPeerConnHandler::new(
            self.conn_handler_conf.clone(),
            self.supported_protocols
                .iter()
                .map(|(prot_id, (conf, _))| (prot_id, conf.clone()))
                .collect::<Vec<_>>(),
        )
    }
}

impl<TPeers, TPeerManager, THandler> NetworkBehaviour for NetworkController<TPeers, TPeerManager, THandler>
where
    TPeers: PeerEvents + Peers + 'static,
    TPeerManager: Stream<Item = PeerManagerOut> + Unpin + 'static,
    THandler: ProtocolEvents + Clone + 'static,
{
    type ConnectionHandler = PartialPeerConnHandler;
    type OutEvent = Result<NetworkControllerOut, NetworkControllerError>;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        trace!("New handler is created");
        self.init_handler()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        conn_id: &ConnectionId,
        _endpoint: &ConnectedPoint,
        _failed_addresses: Option<&Vec<Multiaddr>>,
        _other_established: usize,
    ) {
        match self.enabled_peers.entry(*peer_id) {
            Entry::Occupied(mut peer_entry) => match peer_entry.get() {
                ConnectedPeer::PendingConnect => {
                    self.peers.connection_established(*peer_id, *conn_id); // confirm connection
                    peer_entry.insert(ConnectedPeer::Connected {
                        conn_id: *conn_id,
                        enabled_protocols: HashMap::new(),
                    });
                    // notify all handlers about new connection.
                    for (_, ph) in self.supported_protocols.values() {
                        ph.connected(*peer_id);
                    }
                    self.outbound_peer_connected(*peer_id);
                }
                ConnectedPeer::Connected { .. }
                | ConnectedPeer::PendingDisconnect(..)
                | ConnectedPeer::PendingApprove(..) => {
                    self.pending_actions
                        .push_back(NetworkBehaviourAction::CloseConnection {
                            peer_id: *peer_id,
                            connection: CloseConnection::One(*conn_id),
                        })
                }
            },
            Entry::Vacant(entry) => {
                trace!("Observing new inbound connection {}", peer_id);
                self.peers.incoming_connection(*peer_id, *conn_id);
                entry.insert(ConnectedPeer::PendingApprove(*conn_id));
            }
        }
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _conn_id: &ConnectionId,
        _endpoint: &ConnectedPoint,
        handler: <Self::ConnectionHandler as IntoConnectionHandler>::Handler,
        _remaining_established: usize,
    ) {
        let disconnect_reason = match self.enabled_peers.entry(*peer_id) {
            Entry::Occupied(peer_entry) => match peer_entry.get() {
                ConnectedPeer::Connected { .. } | ConnectedPeer::PendingDisconnect(..) => {
                    peer_entry.remove();
                    if let Some(err) = handler.get_fault() {
                        let reason = ConnectionLossReason::Reset(err);
                        self.peers.connection_lost(*peer_id, reason);
                        Some(reason)
                    } else {
                        let reason = ConnectionLossReason::ResetByPeer;
                        self.peers.connection_lost(*peer_id, reason);
                        Some(reason)
                    }
                }
                // todo: is it possible in case of simultaneous connection?
                ConnectedPeer::PendingConnect | ConnectedPeer::PendingApprove(..) => None,
            },
            Entry::Vacant(_) => None,
        };
        if let Some(reason) = disconnect_reason {
            self.peer_disconnected(*peer_id, reason);
        }
    }

    fn inject_event(&mut self, peer_id: PeerId, connection: ConnectionId, event: ConnHandlerOut) {
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
                    match enabled_protocols.entry(protocol_id.get_inner()) {
                        Entry::Occupied(mut entry) => {
                            trace!(
                                "Current state of protocol {:?} is {:?}",
                                protocol_id,
                                entry.get().0
                            );
                            if let (EnabledProtocol::PendingEnable, handler) = entry.get() {
                                handler.protocol_enabled(
                                    peer_id,
                                    protocol_ver.get_inner(),
                                    out_channel.clone(),
                                    handshake,
                                );
                                let enabled_protocol = EnabledProtocol::Enabled {
                                    ver: protocol_ver.get_inner(),
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
                if let Some(ConnectedPeer::Connected {
                    enabled_protocols, ..
                }) = self.enabled_peers.get_mut(&peer_id)
                {
                    let protocol_id = protocol_tag.protocol_id();
                    let (_, prot_handler) = self.supported_protocols.get_supported(protocol_id);
                    match enabled_protocols.entry(protocol_id.get_inner()) {
                        Entry::Vacant(entry) => {
                            entry.insert((EnabledProtocol::PendingApprove, prot_handler.clone()));
                            prot_handler.protocol_requested(
                                peer_id,
                                protocol_tag.protocol_ver().get_inner(),
                                handshake,
                            );
                        }
                        Entry::Occupied(_) => {
                            warn!(
                                "Peer {:?} opened already enabled protocol {:?}",
                                peer_id, protocol_id
                            );
                            self.pending_actions
                                .push_back(NetworkBehaviourAction::NotifyHandler {
                                    peer_id,
                                    handler: NotifyHandler::One(connection),
                                    event: ConnHandlerIn::Close(protocol_id),
                                })
                        }
                    }
                }
            }
            ConnHandlerOut::ClosedByPeer(protocol_id) | ConnHandlerOut::RefusedToOpen(protocol_id) => {
                if let Some(ConnectedPeer::Connected {
                    enabled_protocols, ..
                }) = self.enabled_peers.get_mut(&peer_id)
                {
                    match enabled_protocols.entry(protocol_id.get_inner()) {
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

            ConnHandlerOut::Closed(protocol_id) => {
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

    fn inject_dial_failure(&mut self, peer_id: Option<PeerId>, _: Self::ConnectionHandler, _: &DialError) {
        if let Some(peer_id) = peer_id {
            self.peers.dial_failure(peer_id);
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        loop {
            // 1. Try to return a pending action.
            if let Some(action) = self.pending_actions.pop_front() {
                return Poll::Ready(action);
            };
            // 2. Poll for instructions from PM.
            match Stream::poll_next(Pin::new(&mut self.peer_manager), cx) {
                Poll::Ready(Some(PeerManagerOut::Connect(pid))) => {
                    let handler = self.init_handler();
                    match self.enabled_peers.entry(pid.peer_id()) {
                        Entry::Occupied(_) => {}
                        Entry::Vacant(peer_entry) => {
                            peer_entry.insert(ConnectedPeer::PendingConnect);
                            self.pending_actions.push_back(NetworkBehaviourAction::Dial {
                                opts: pid.into(),
                                handler,
                            })
                        }
                    }
                    continue;
                }
                Poll::Ready(Some(PeerManagerOut::Drop(peer_id))) => {
                    if let Some(ConnectedPeer::Connected { conn_id, .. }) =
                        self.enabled_peers.get_mut(&peer_id)
                    {
                        self.pending_actions
                            .push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id,
                                handler: NotifyHandler::One(*conn_id),
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
                                    conn_id: cid,
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
                                self.pending_actions
                                    .push_back(NetworkBehaviourAction::NotifyHandler {
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
                                    if let Some((_, prot_handler)) = self.supported_protocols.get(protocol) {
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
                                            }
                                        }
                                    } else {
                                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(Err(
                                            NetworkControllerError::UnsupportedProtocol(protocol),
                                        )));
                                    }
                                }
                                ConnectedPeer::PendingConnect
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
                    NetworkControllerIn::UpdatePeerProtocols { peer, protocols } => {
                        self.peers.set_peer_protocols(peer, protocols);
                    }
                    NetworkControllerIn::EnableProtocol {
                        peer: peer_id,
                        protocol: protocol_id,
                        handshake,
                    } => {
                        if let Some(ConnectedPeer::Connected {
                            conn_id,
                            enabled_protocols,
                        }) = self.enabled_peers.get_mut(&peer_id)
                        {
                            let (_, prot_handler) = self.supported_protocols.get(protocol_id).unwrap();
                            match enabled_protocols.entry(protocol_id) {
                                Entry::Occupied(protocol_entry) => match protocol_entry.remove_entry().1 {
                                    // Protocol handler approves either outbound or inbound protocol request.
                                    (
                                        EnabledProtocol::PendingEnable | EnabledProtocol::PendingApprove,
                                        handler,
                                    ) => {
                                        enabled_protocols
                                            .insert(protocol_id, (EnabledProtocol::PendingEnable, handler));
                                        self.pending_actions.push_back(
                                            NetworkBehaviourAction::NotifyHandler {
                                                peer_id,
                                                handler: NotifyHandler::One(*conn_id),
                                                event: ConnHandlerIn::Open {
                                                    protocol_id,
                                                    handshake,
                                                },
                                            },
                                        );
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
                                    self.pending_actions
                                        .push_back(NetworkBehaviourAction::NotifyHandler {
                                            peer_id,
                                            handler: NotifyHandler::One(*conn_id),
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
                }
                continue;
            }

            return Poll::Pending;
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkControllerError {
    #[error("Unsupported protocol: {0:?}")]
    UnsupportedProtocol(ProtocolId),
}
