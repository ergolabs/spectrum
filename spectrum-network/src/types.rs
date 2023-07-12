use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};

use libp2p::bytes::BytesMut;
use libp2p::core::upgrade;
use serde::{Deserialize, Serialize};

use crate::peer_manager::data::ReputationChange;

/// Opaque identifier for an incoming connection. Allocated by the network.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct IncomingIndex(u64);

impl From<u64> for IncomingIndex {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

/// Reputation value of the node, between `i32::MIN` (we hate that node) and
/// `i32::MAX` (we love that node).
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Reputation(i32);

impl Reputation {
    pub fn initial() -> Self {
        Self(0)
    }
    pub fn apply(&self, change: ReputationChange) -> Self {
        Reputation(self.0 + i32::from(change))
    }
}

impl From<i32> for Reputation {
    fn from(val: i32) -> Self {
        Self(val)
    }
}

/// Identifier of a protocol.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolId(u8);

impl ProtocolId {
    pub const fn from_u8(x: u8) -> Self {
        Self(x)
    }
}

impl From<ProtocolId> for u8 {
    fn from(p: ProtocolId) -> Self {
        p.0
    }
}

impl From<u8> for ProtocolId {
    fn from(val: u8) -> Self {
        Self(val)
    }
}

/// Version of a protocol.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ProtocolVer(pub u8);

impl Default for ProtocolVer {
    fn default() -> Self {
        ProtocolVer(1)
    }
}

impl Ord for ProtocolVer {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

impl PartialOrd for ProtocolVer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<ProtocolVer> for u8 {
    fn from(p: ProtocolVer) -> Self {
        p.0
    }
}

impl From<u8> for ProtocolVer {
    fn from(v: u8) -> Self {
        ProtocolVer(v)
    }
}

/// Tag of a protocol. Consists of ProtocolId + ProtocolVer.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolTag([u8; 3]);

impl Display for ProtocolTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "ProtocolTag(/{}/{})",
            Into::<u8>::into(self.protocol_id()),
            Into::<u8>::into(self.protocol_ver())
        ))
    }
}

impl ProtocolTag {
    pub fn protocol_ver(&self) -> ProtocolVer {
        ProtocolVer::from(self.0[2])
    }

    pub fn protocol_id(&self) -> ProtocolId {
        ProtocolId::from(self.0[1])
    }
}

impl ProtocolTag {
    pub fn new(protocol_id: ProtocolId, protocol_ver: ProtocolVer) -> Self {
        Self([b"/"[0], protocol_id.into(), protocol_ver.into()])
    }
}

impl From<ProtocolTag> for ProtocolVer {
    fn from(p: ProtocolTag) -> Self {
        ProtocolVer::from(p.0[1])
    }
}

impl AsRef<str> for ProtocolTag {
    fn as_ref(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawMessage(Vec<u8>);

impl From<Vec<u8>> for RawMessage {
    fn from(xs: Vec<u8>) -> Self {
        RawMessage(xs)
    }
}

impl From<BytesMut> for RawMessage {
    fn from(xs: BytesMut) -> Self {
        RawMessage(xs.freeze().to_vec())
    }
}

impl From<RawMessage> for Vec<u8> {
    fn from(r: RawMessage) -> Self {
        r.0
    }
}

impl AsRef<[u8]> for RawMessage {
    fn as_ref(&self) -> &[u8] {
        &*self.0
    }
}
