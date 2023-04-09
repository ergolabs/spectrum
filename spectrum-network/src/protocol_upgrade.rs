pub mod combinators;
pub mod handshake;
mod message;
pub(crate) mod substream;

use crate::protocol::ProtocolSpec;
use crate::protocol_upgrade::message::{Approve, APPROVE_SIZE};
use crate::protocol_upgrade::substream::{ProtocolApproveState, ProtocolSubstreamIn, ProtocolSubstreamOut};
use crate::types::{ProtocolId, ProtocolTag, ProtocolVer, RawMessage};
use asynchronous_codec::Framed;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite};
use libp2p::core::{upgrade, UpgradeInfo};
use libp2p::{InboundUpgrade, OutboundUpgrade};
use log::trace;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::{io, vec};
use unsigned_varint::codec::UviBytes;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolHandshakeErr {
    #[error(transparent)]
    IoErr(#[from] io::Error),
    #[error(transparent)]
    PrefixReadErr(#[from] unsigned_varint::io::ReadError),
    #[error("Invalid approve message")]
    InvalidApprove(),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolUpgradeErr {
    #[error(transparent)]
    HandshakeErr(#[from] ProtocolHandshakeErr),
    #[error("Unsupported {0:?}")]
    UnsupportedProtocolVer(ProtocolVer),
}

#[derive(Debug, Clone)]
pub struct InboundProtocolSpec {
    /// Maximum allowed size for a single message.
    max_message_size: usize,
    /// Does the protocol negotiation require a special handshake or not.
    handshake_required: bool,
}

impl From<ProtocolSpec> for InboundProtocolSpec {
    fn from(spec: ProtocolSpec) -> Self {
        Self {
            max_message_size: spec.max_message_size,
            handshake_required: spec.approve_required,
        }
    }
}

/// Upgrade that accepts a substream, sends back a status message, then becomes a unidirectional
/// stream of messages.
#[derive(Debug, Clone)]
pub struct ProtocolUpgradeIn {
    /// Protocol to negotiate.
    protocol_id: ProtocolId,
    /// Protocol versions to negotiate.
    /// The first one is the main name, while the other ones are fall backs.
    supported_versions: BTreeMap<ProtocolVer, InboundProtocolSpec>,
}

impl ProtocolUpgradeIn {
    pub fn new(protocol_id: ProtocolId, supported_versions: Vec<(ProtocolVer, ProtocolSpec)>) -> Self {
        let supported_versions = BTreeMap::from_iter(
            supported_versions
                .into_iter()
                .map(|(ver, spec)| (ver, InboundProtocolSpec::from(spec))),
        );
        Self {
            protocol_id,
            supported_versions,
        }
    }
}

impl UpgradeInfo for ProtocolUpgradeIn {
    type Info = ProtocolTag;
    type InfoIter = vec::IntoIter<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.supported_versions
            .keys()
            .cloned()
            .map(|v| ProtocolTag::new(self.protocol_id, v))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl<Substream> InboundUpgrade<Substream> for ProtocolUpgradeIn
where
    Substream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = InboundProtocolUpgraded<ProtocolSubstreamIn<Substream>>;
    type Error = ProtocolUpgradeErr;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, mut socket: Substream, negotiated_tag: Self::Info) -> Self::Future {
        Box::pin(async move {
            let target = format!("Inbound({})", negotiated_tag);
            trace!(target: &target, "upgrade_inbound()");
            let protocol_ver = negotiated_tag.protocol_ver();
            let pspec = self
                .supported_versions
                .get(&protocol_ver)
                .ok_or(ProtocolUpgradeErr::UnsupportedProtocolVer(protocol_ver))?;
            let mut codec = UviBytes::default();
            codec.set_max_len(pspec.max_message_size);
            let handshake = if pspec.handshake_required {
                trace!(target: &target, "Waiting for handshake");
                let hs = Some(read_handshake(&mut socket, pspec.max_message_size).await?);
                trace!(target: &target, "Received handshake");
                hs
            } else {
                None
            };
            let approve_state = if pspec.handshake_required {
                Some(ProtocolApproveState::NotSent)
            } else {
                trace!(target: &target, "Approval not required");
                None
            };
            let substream = ProtocolSubstreamIn {
                socket: Framed::new(socket, codec),
                approve_state,
            };
            Ok(InboundProtocolUpgraded {
                negotiated_tag,
                handshake,
                substream,
            })
        })
    }
}

#[derive(Debug, Clone)]
pub struct OutboundProtocolSpec {
    /// Maximum allowed size for a single notification.
    max_message_size: usize,
    /// Initial message to send when we start communicating.
    handshake: Option<RawMessage>,
}

impl OutboundProtocolSpec {
    pub fn new(max_message_size: usize, handshake: Option<RawMessage>) -> Self {
        Self {
            max_message_size,
            handshake,
        }
    }
}

/// Upgrade that opens a substream, waits for the remote to accept by sending back a status
/// message, then becomes a unidirectional sink of data.
#[derive(Debug, Clone)]
pub struct ProtocolUpgradeOut {
    /// Protocol to negotiate.
    protocol_id: ProtocolId,
    /// Protocol versions to negotiate.
    /// The first one is the main name, while the other ones are fall backs.
    supported_versions: BTreeMap<ProtocolVer, OutboundProtocolSpec>,
}

impl ProtocolUpgradeOut {
    pub fn new(
        protocol_id: ProtocolId,
        supported_versions: Vec<(ProtocolVer, ProtocolSpec, Option<RawMessage>)>,
    ) -> Self {
        let supported_versions =
            BTreeMap::from_iter(supported_versions.into_iter().map(|(ver, spec, handshake)| {
                (ver, OutboundProtocolSpec::new(spec.max_message_size, handshake))
            }));
        Self {
            protocol_id,
            supported_versions,
        }
    }
}

impl UpgradeInfo for ProtocolUpgradeOut {
    type Info = ProtocolTag;
    type InfoIter = vec::IntoIter<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.supported_versions
            .keys()
            .cloned()
            .map(|v| ProtocolTag::new(self.protocol_id, v))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl<Substream> OutboundUpgrade<Substream> for ProtocolUpgradeOut
where
    Substream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = OutboundProtocolUpgraded<ProtocolSubstreamOut<Substream>>;
    type Error = ProtocolUpgradeErr;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, mut socket: Substream, negotiated_tag: Self::Info) -> Self::Future {
        Box::pin(async move {
            let target = format!("Outbound({})", negotiated_tag);
            trace!(target: &target, "upgrade_outbound()");
            let protocol_ver = negotiated_tag.protocol_ver();
            let pspec = self
                .supported_versions
                .get(&protocol_ver)
                .ok_or(ProtocolUpgradeErr::UnsupportedProtocolVer(protocol_ver))?;
            let mut codec = UviBytes::default();
            codec.set_max_len(pspec.max_message_size);
            if let Some(handshake) = &pspec.handshake {
                trace!(target: &target, "Sending handshake");
                write_handshake(&mut socket, handshake).await?;
                trace!(target: &target, "Handshake sent");
            }
            // Wait for approve in response if required.
            if pspec.handshake.is_some() {
                trace!(target: &target, "Waiting for approve");
                read_approve(&mut socket).await?;
                trace!(target: &target, "Approved");
            };
            let substream = ProtocolSubstreamOut {
                socket: Framed::new(socket, codec),
            };
            Ok(OutboundProtocolUpgraded {
                negotiated_tag,
                substream,
            })
        })
    }
}

pub struct InboundProtocolUpgraded<Substream> {
    /// ProtocolTag negotiated with the peer.
    pub negotiated_tag: ProtocolTag,
    /// Handshake sent by the peer.
    pub handshake: Option<RawMessage>,
    pub substream: Substream,
}

pub struct OutboundProtocolUpgraded<Substream> {
    /// ProtocolTag negotiated with the peer.
    pub negotiated_tag: ProtocolTag,
    pub substream: Substream,
}

async fn read_handshake<Substream: AsyncRead + Unpin>(
    socket: &mut Substream,
    max_size: usize,
) -> Result<RawMessage, ProtocolHandshakeErr> {
    let handshake = upgrade::read_length_prefixed(socket, max_size).await?;
    Ok(RawMessage::from(handshake))
}

async fn read_approve<Substream: AsyncRead + Unpin>(
    socket: &mut Substream,
) -> Result<(), ProtocolHandshakeErr> {
    let mut buf = vec![0u8; APPROVE_SIZE];
    socket.read_exact(&mut buf).await?;
    if buf == Approve::bytes() {
        Ok(())
    } else {
        Err(ProtocolHandshakeErr::InvalidApprove())
    }
}

async fn write_handshake<Substream: AsyncWrite + Unpin>(
    socket: &mut Substream,
    msg: &RawMessage,
) -> Result<(), ProtocolHandshakeErr> {
    upgrade::write_length_prefixed(socket, msg).await?;
    Ok(())
}
