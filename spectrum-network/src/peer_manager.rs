pub mod data;
pub mod peer_index;
pub mod peers_state;

use crate::peer_manager::data::{
    ConnectionLossReason, ConnectionState, PeerInfo, ProtocolAllocationPolicy, ReputationChange,
};
use crate::peer_manager::peers_state::{PeerInState, PeerStateFilter, PeersState};
use crate::types::{ProtocolId, Reputation};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::channel::oneshot::Receiver;
use futures::Stream;
use libp2p::core::connection::ConnectionId;
use libp2p::PeerId;
use log::trace;
use std::collections::{HashSet, VecDeque};
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Peer Manager output commands.
#[derive(Debug, PartialEq)]
pub enum PeerManagerOut {
    /// Request to open a connection to the given peer.
    Connect(PeerId),
    /// Drop the connection to the given peer, or cancel the connection attempt after a `Connect`.
    Drop(PeerId),
    /// Approves an incoming connection.
    Accept(PeerId, ConnectionId),
    /// Rejects an incoming connection.
    Reject(PeerId, ConnectionId),
    /// An instruction to start the specified protocol with the specified peer.
    StartProtocol(ProtocolId, PeerId),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProtocolSupportIsConfirmed(bool);

impl Into<bool> for ProtocolSupportIsConfirmed {
    fn into(self) -> bool {
        self.0
    }
}

/// Peer Manager inputs.
#[derive(Debug)]
pub enum PeerManagerRequestIn {
    AddPeer(PeerId),
    AddReservedPeer(PeerId),
    SetReservedPeers(HashSet<PeerId>),
    ReportPeer(PeerId, ReputationChange),
    GetPeerReputation(PeerId, oneshot::Sender<Reputation>),
    /// Update set of protocols that the given peer supports.
    SetProtocols(PeerId, Vec<ProtocolId>),
}

/// Events Peer Manager reacts to.
#[derive(Debug)]
pub enum PeerManagerNotificationIn {
    IncomingConnection(PeerId, ConnectionId),
    ConnectionLost(PeerId, ConnectionLossReason),
}

pub enum PeerManagerIn {
    Notification(PeerManagerNotificationIn),
    Request(PeerManagerRequestIn),
}

/// Async API to PeerManager.
pub trait Peers {
    fn add_peer(&mut self, peer_id: PeerId);
    fn add_reserved_peer(&mut self, peer_id: PeerId);
    fn set_reserved_peers(&mut self, peers: HashSet<PeerId>);
    fn report_peer(&mut self, peer_id: PeerId, change: ReputationChange);
    fn get_peer_reputation(&mut self, peer_id: PeerId) -> Receiver<Reputation>;
    fn set_peer_protocols(&mut self, peer_id: PeerId, protocols: Vec<ProtocolId>);
}

/// Async API to PeerManager notifications.
pub trait PeerManagerNotifications {
    fn incoming_connection(&mut self, peer_id: PeerId, conn_id: ConnectionId);
    fn connection_lost(&mut self, peer_id: PeerId, reason: ConnectionLossReason);
}

pub trait PeerManagerRequestsBehavior {
    fn on_add_peer(&mut self, peer_id: PeerId);
    fn on_add_reserved_peer(&mut self, peer_id: PeerId);
    fn on_set_reserved_peers(&mut self, peers: HashSet<PeerId>);
    fn on_report_peer(&mut self, peer_id: PeerId, change: ReputationChange);
    fn on_get_peer_reputation(&mut self, peer_id: PeerId, response: oneshot::Sender<Reputation>);
    fn on_set_peer_protocols(&mut self, peer_id: PeerId, protocols: Vec<ProtocolId>);
    fn on_request_protocol(
        &mut self,
        protocol_id: ProtocolId,
        support_confirmed: ProtocolSupportIsConfirmed,
    );
}

pub trait PeerManagerNotificationsBehavior {
    fn on_incoming_connection(&mut self, peer_id: PeerId, conn_id: ConnectionId);
    fn on_connection_lost(&mut self, peer_id: PeerId, reason: ConnectionLossReason);
}

pub struct PeersLive {
    requests_snd: UnboundedSender<PeerManagerRequestIn>,
}

impl Peers for PeersLive {
    fn add_peer(&mut self, peer_id: PeerId) {
        let _ = self
            .requests_snd
            .unbounded_send(PeerManagerRequestIn::AddPeer(peer_id));
    }

    fn add_reserved_peer(&mut self, peer_id: PeerId) {
        let _ = self
            .requests_snd
            .unbounded_send(PeerManagerRequestIn::AddReservedPeer(peer_id));
    }

    fn set_reserved_peers(&mut self, peers: HashSet<PeerId>) {
        let _ = self
            .requests_snd
            .unbounded_send(PeerManagerRequestIn::SetReservedPeers(peers));
    }

    fn report_peer(&mut self, peer_id: PeerId, change: ReputationChange) {
        let _ = self
            .requests_snd
            .unbounded_send(PeerManagerRequestIn::ReportPeer(peer_id, change));
    }

    fn get_peer_reputation(&mut self, peer_id: PeerId) -> Receiver<Reputation> {
        let (sender, receiver) = oneshot::channel::<Reputation>();
        let _ = self
            .requests_snd
            .unbounded_send(PeerManagerRequestIn::GetPeerReputation(peer_id, sender));
        receiver
    }

    fn set_peer_protocols(&mut self, peer_id: PeerId, protocols: Vec<ProtocolId>) {
        let _ = self
            .requests_snd
            .unbounded_send(PeerManagerRequestIn::SetProtocols(peer_id, protocols));
    }
}

pub struct PeerManagerNotificationsLive {
    notifications_snd: UnboundedSender<PeerManagerNotificationIn>,
}

impl PeerManagerNotifications for PeerManagerNotificationsLive {
    fn incoming_connection(&mut self, peer_id: PeerId, conn_id: ConnectionId) {
        let _ =
            self.notifications_snd
                .unbounded_send(PeerManagerNotificationIn::IncomingConnection(
                    peer_id, conn_id,
                ));
    }

    fn connection_lost(&mut self, peer_id: PeerId, reason: ConnectionLossReason) {
        let _ = self
            .notifications_snd
            .unbounded_send(PeerManagerNotificationIn::ConnectionLost(peer_id, reason));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerManagerConfig {
    min_reputation: Reputation,
    conn_reset_outbound_backoff: Duration,
    periodic_conn_interval: Duration,
    protocols_allocation: Vec<(ProtocolId, ProtocolAllocationPolicy)>,
}

pub struct PeerManager<PState> {
    state: PState,
    conf: PeerManagerConfig,
    notifications_recv: UnboundedReceiver<PeerManagerNotificationIn>,
    requests_recv: UnboundedReceiver<PeerManagerRequestIn>,
    out_queue: VecDeque<PeerManagerOut>,
    next_conn_alloc_at: Instant,
}

impl<S: PeersState> PeerManager<S> {
    /// Connect to reserved peers we are not connected yet.
    pub fn connect_reserved(&mut self) {
        let peers = self
            .state
            .get_reserved_peers(Some(PeerStateFilter::NotConnected));
        for pid in peers {
            self.connect(pid)
        }
    }

    /// Connect to the best peer we are not connected yet.
    pub fn connect_best(&mut self) {
        if let Some(pid) = self.state.peek_best(Some(|pi: &PeerInfo| {
            matches!(pi.state, ConnectionState::NotConnected)
        })) {
            self.connect(pid)
        }
    }

    fn connect(&mut self, peer_id: PeerId) {
        if let Some(PeerInState::NotConnected(ncp)) = self.state.peer(&peer_id) {
            let should_connect = if let Some(backoff_until) = ncp.backoff_until() {
                backoff_until <= Instant::now()
            } else {
                true
            };
            if should_connect && ncp.try_connect().is_ok() {
                self.out_queue.push_back(PeerManagerOut::Connect(peer_id))
            }
        }
    }
}

impl<S: PeersState> PeerManagerRequestsBehavior for PeerManager<S> {
    fn on_add_peer(&mut self, peer_id: PeerId) {
        self.state.try_add_peer(peer_id, false);
    }

    fn on_add_reserved_peer(&mut self, peer_id: PeerId) {
        self.state.try_add_peer(peer_id, true);
    }

    fn on_set_reserved_peers(&mut self, peers: HashSet<PeerId>) {
        let unkown_peers = self.state.set_reserved_peers(peers);
        for pid in unkown_peers {
            self.state.try_add_peer(pid, true);
        }
    }

    fn on_report_peer(&mut self, peer_id: PeerId, adjustment: ReputationChange) {
        match self.state.peer(&peer_id) {
            Some(peer) => {
                peer.adjust_reputation(adjustment);
            }
            None => {} // warn
        }
    }

    fn on_get_peer_reputation(&mut self, peer_id: PeerId, response: oneshot::Sender<Reputation>) {
        match self.state.peer(&peer_id) {
            Some(peer) => {
                let reputation = peer.get_reputation();
                let _ = response.send(reputation);
            }
            None => {} // warn
        }
    }

    fn on_set_peer_protocols(&mut self, peer_id: PeerId, protocols: Vec<ProtocolId>) {
        match self.state.peer(&peer_id) {
            Some(mut peer) => {
                peer.set_protocols(protocols);
            }
            None => {} // warn
        }
    }

    fn on_request_protocol(
        &mut self,
        protocol_id: ProtocolId,
        support_confirmed: ProtocolSupportIsConfirmed,
    ) {
        let peer = self.state.peek_best(if support_confirmed.0 {
            Some(|pi: &PeerInfo| pi.supports(&protocol_id).unwrap_or(false))
        } else {
            None
        });
        if let Some(pid) = peer {
            let _ = self
                .out_queue
                .push_back(PeerManagerOut::StartProtocol(protocol_id, pid));
        }
    }
}

impl<S: PeersState> PeerManagerNotificationsBehavior for PeerManager<S> {
    fn on_incoming_connection(&mut self, peer_id: PeerId, conn_id: ConnectionId) {
        match self.state.peer(&peer_id) {
            Some(PeerInState::NotConnected(ncp)) => {
                if ncp.get_reputation() >= self.conf.min_reputation
                    && ncp.try_accept_connection().is_ok()
                {
                    self.out_queue
                        .push_back(PeerManagerOut::Accept(peer_id, conn_id));
                } else {
                    self.out_queue
                        .push_back(PeerManagerOut::Reject(peer_id, conn_id));
                }
            }
            Some(PeerInState::Connected(_)) => {
                self.out_queue
                    .push_back(PeerManagerOut::Reject(peer_id, conn_id));
            }
            None => {
                if let Some(ncp) = self.state.try_add_peer(peer_id, false) {
                    if ncp.try_accept_connection().is_ok() {
                        self.out_queue
                            .push_back(PeerManagerOut::Accept(peer_id, conn_id));
                    } else {
                        self.out_queue
                            .push_back(PeerManagerOut::Reject(peer_id, conn_id));
                    }
                } else {
                    self.out_queue
                        .push_back(PeerManagerOut::Reject(peer_id, conn_id));
                }
            }
        }
    }

    fn on_connection_lost(&mut self, peer_id: PeerId, reason: ConnectionLossReason) {
        match self.state.peer(&peer_id) {
            Some(PeerInState::Connected(cp)) => {
                let mut ncp = cp.disconnect();
                match reason {
                    ConnectionLossReason::ResetByPeer => {
                        if !ncp.is_reserved() {
                            let backoff_until =
                                Instant::now().add(self.conf.conn_reset_outbound_backoff);
                            ncp.set_backoff_until(backoff_until);
                        }
                    }
                    ConnectionLossReason::Unknown => {}
                }
            }
            Some(PeerInState::NotConnected(_)) => {} // warn
            None => {}                               // warn
        }
    }
}

impl<S: Unpin + PeersState> Stream for PeerManager<S> {
    type Item = PeerManagerOut;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(out) = self.out_queue.pop_front() {
                return Poll::Ready(Some(out));
            }

            let now = Instant::now();
            if self.next_conn_alloc_at >= now {
                self.connect_reserved();
                self.connect_best();
                self.next_conn_alloc_at = now.add(self.conf.periodic_conn_interval);
            }

            if let Poll::Ready(Some(notif)) =
                Stream::poll_next(Pin::new(&mut self.notifications_recv), cx)
            {
                match notif {
                    PeerManagerNotificationIn::IncomingConnection(pid, conn_id) => {
                        self.on_incoming_connection(pid, conn_id)
                    }
                    PeerManagerNotificationIn::ConnectionLost(pid, reason) => {
                        self.on_connection_lost(pid, reason)
                    }
                }
            }

            if let Poll::Ready(Some(req)) = Stream::poll_next(Pin::new(&mut self.requests_recv), cx)
            {
                match req {
                    PeerManagerRequestIn::AddPeer(pid) => self.on_add_peer(pid),
                    PeerManagerRequestIn::ReportPeer(pid, adjustment) => {
                        self.on_report_peer(pid, adjustment)
                    }
                    PeerManagerRequestIn::AddReservedPeer(pid) => self.on_add_reserved_peer(pid),
                    PeerManagerRequestIn::GetPeerReputation(pid, resp) => {
                        self.on_get_peer_reputation(pid, resp)
                    }
                    PeerManagerRequestIn::SetReservedPeers(peers) => {
                        self.on_set_reserved_peers(peers)
                    }
                    PeerManagerRequestIn::SetProtocols(pid, protocols) => {
                        self.on_set_peer_protocols(pid, protocols)
                    }
                }
            }

            // Allocate protocol substreams according to defined policies.
            for (prot, policy) in self.conf.protocols_allocation.iter() {
                if let Some(enabled_peers) = self.state.get_enabled_peers(prot) {
                    let cond = match policy {
                        ProtocolAllocationPolicy::Bounded(max_conn_percent) => {
                            enabled_peers.len() / self.state.num_connected_peers()
                                < *max_conn_percent / 100
                        }
                        ProtocolAllocationPolicy::Max => {
                            enabled_peers.len() < self.state.num_connected_peers()
                        }
                        ProtocolAllocationPolicy::Zero => false,
                    };
                    if cond {
                        if let Some(candidate) =
                            self.state.peek_best(Some(|pid: &PeerId, pi: &PeerInfo| {
                                !enabled_peers.contains(pid) && pi.supports(&prot).unwrap_or(false)
                            }))
                        {
                            if let Some(PeerInState::Connected(mut cp)) =
                                self.state.peer(&candidate)
                            {
                                cp.enable_protocol(*prot);
                                self.out_queue
                                    .push_back(PeerManagerOut::StartProtocol(*prot, candidate));
                            }
                        }
                    }
                }
            }
        }
    }
}
