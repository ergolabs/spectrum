use libp2p::PeerId;
use spectrum_network::peer::peer_store::PeerSetsConfig;
use spectrum_network::peer::peers_state::{DefaultPeersState, PeersState};

#[test]
fn should_add_peer() {
    let mut peer_state = mk_peers_state(4, 2, 10);
    let peer_id = PeerId::random();

    assert_eq!(peer_state.try_add_peer(peer_id, false).is_some(), true);
    assert_eq!(peer_state.peer(&peer_id).is_some(), true);
}

#[test]
fn should_forget_peer() {
    let mut peer_state = mk_peers_state(4, 2, 10);
    let peer_id = PeerId::random();

    let peer = peer_state.try_add_peer(peer_id, false);

    assert_eq!(peer.is_some(), true);
    peer.unwrap().forget_peer();
    assert_eq!(peer_state.peer(&peer_id).is_none(), true);
}

#[test]
fn should_connect_to_peer_when_vacant_connections_available() {
    let mut peer_state = mk_peers_state(4, 2, 10);
    let peer_id = PeerId::random();

    let peer = peer_state.try_add_peer(peer_id, false);
    let connected_peer = peer.unwrap().try_connect();
    assert_eq!(connected_peer.is_ok(), true);
}

#[test]
fn err_connect_to_peer_when_vacant_connections_not_available() {
    let mut peer_state = mk_peers_state(0, 0, 10);
    let peer_id = PeerId::random();

    let peer = peer_state.try_add_peer(peer_id, false).unwrap();
    assert!(peer.try_connect().is_err());
}

fn mk_peers_state(max_incoming: usize, max_outgoing: usize, capacity: usize) -> impl PeersState {
    let pset_config = PeerSetsConfig {
        max_incoming,
        max_outgoing,
    };
    DefaultPeersState::new(pset_config)
}
