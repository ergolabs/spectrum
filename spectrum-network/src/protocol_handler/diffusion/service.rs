use std::sync::Arc;

use spectrum_ledger::block::{BlockId, BlockSectionType};
use spectrum_ledger::{ModifierId, ModifierType, SerializedModifier, SlotNo};
use spectrum_view::history::LedgerHistoryReadAsync;

use crate::protocol_handler::diffusion::message::{
    DiffusionHandshake, DiffusionSpec, HandshakeV1, SyncStatus,
};
use crate::types::ProtocolVer;

/// Peer chain in comparison to the local one.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum RemoteChainCmp {
    Equal,
    /// Remote chain is longer than local one.
    /// `Some(wanted_suffix)` when we can immediately determine the diff.
    Longer(/*wanted_suffix*/ Option<Vec<BlockId>>),
    /// Remote chain is shorter than local one.
    /// We also determine best block on remote.
    Shorter(/*remote_best_block*/ BlockId),
    /// Remote is on fork relatevely to the local chain.
    /// `Some(BlockId)` when we are able to determine branching point.
    Fork(/*intersection_at*/ Option<BlockId>),
    /// We can't compare local chain to the remote one. Most likely remote is byzantine.
    Nonsense,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct SyncState {
    /// Max slot in remote's chain
    pub height: SlotNo,
    pub cmp: RemoteChainCmp,
}

pub(super) struct RemoteSync<THistory> {
    history: Arc<THistory>,
}

impl<THistory> Clone for RemoteSync<THistory> {
    fn clone(&self) -> Self {
        Self {
            history: self.history.clone(),
        }
    }
}

impl<THistory> RemoteSync<THistory>
where
    THistory: LedgerHistoryReadAsync,
{
    pub fn new(history: Arc<THistory>) -> Self {
        Self { history }
    }

    pub async fn local_status(&self) -> SyncStatus {
        let tail = self.history.get_tail(SYNC_HEADERS).await;
        let height = tail.last().slot;
        let mut tail = Vec::from(self.history.get_tail(SYNC_HEADERS).await);
        tail.reverse(); // newer blocks first
        SyncStatus {
            height,
            last_blocks: tail.into_iter().map(|h| h.id).collect(),
        }
    }

    pub async fn make_poly_handshake(&self) -> Vec<(ProtocolVer, Option<DiffusionHandshake>)> {
        vec![(
            DiffusionSpec::v1(),
            Some(DiffusionHandshake::HandshakeV1(HandshakeV1(
                self.local_status().await,
            ))),
        )]
    }

    pub async fn remote_state(&self, peer_status: SyncStatus) -> SyncState {
        SyncState {
            height: peer_status.height,
            cmp: self.compare_remote(peer_status).await,
        }
    }

    pub async fn extension(&self, remote_tip: BlockId, cap: usize) -> Vec<BlockId> {
        self.history.follow(remote_tip, cap).await
    }

    pub async fn get_modifiers(
        &self,
        mod_type: ModifierType,
        modifiers: Vec<ModifierId>,
    ) -> Vec<SerializedModifier> {
        match mod_type {
            ModifierType::BlockHeader => {
                self.history
                    .multi_get_raw(BlockSectionType::Header, modifiers)
                    .await
            } // ModifierType::BlockBody => {
              //     self.history
              //         .multi_get_raw(BlockSectionType::Body, modifiers)
              //         .await
              // }
              // ModifierType::Transaction => {
              //     todo!("Go to mempool")
              // }
        }
    }

    /// Compare remote chain with the local one.
    async fn compare_remote(&self, peer_status: SyncStatus) -> RemoteChainCmp {
        let local_tip = self.history.get_tip().await;
        let peer_height = peer_status.height;

        let peer_tail = peer_status.last_blocks;

        if peer_tail.is_empty() {
            RemoteChainCmp::Shorter(BlockId::ORIGIN)
        } else {
            let delta = <u64>::from(peer_height).saturating_sub(<u64>::from(local_tip.slot));
            let num_shared_blocks = peer_tail.len() as u64;
            if delta > num_shared_blocks {
                RemoteChainCmp::Longer(None)
            } else {
                // Trying to find common point using only the tip of our local chain
                let mut optimistic_common_point = None;
                let peer_tip = peer_tail[0];
                for blk in &peer_tail {
                    if *blk == local_tip.id {
                        optimistic_common_point = Some(*blk);
                        break;
                    }
                }
                match optimistic_common_point {
                    Some(common_sl) if peer_height >= local_tip.slot =>
                    // Equal | Longer(Some(wanted_suffix))
                    {
                        if common_sl == peer_tip {
                            RemoteChainCmp::Equal
                        } else {
                            let mut wanted_suffix = vec![];
                            for blk in peer_tail {
                                if blk == common_sl {
                                    break;
                                }
                                wanted_suffix.push(blk);
                            }
                            wanted_suffix.reverse();
                            RemoteChainCmp::Longer(Some(wanted_suffix))
                        }
                    }
                    None => {
                        if let Some(common_point) = self.common_point(&peer_tail).await {
                            if common_point == peer_tip {
                                RemoteChainCmp::Shorter(common_point)
                            } else {
                                RemoteChainCmp::Fork(Some(common_point))
                            }
                        } else {
                            RemoteChainCmp::Fork(None)
                        }
                    }
                    _ => RemoteChainCmp::Nonsense,
                }
            }
        }
    }

    /// Find the point where remote chain intersects local one.
    async fn common_point(&self, remote_tail: &Vec<BlockId>) -> Option<BlockId> {
        for blk in remote_tail {
            if self.history.member(blk).await {
                return Some(blk.clone());
            }
        }
        None
    }
}

const SYNC_HEADERS: usize = 256;

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use nonempty::NonEmpty;

    use spectrum_ledger::block::{
        BlockHeader, BlockId, BlockSection, BlockSectionId, BlockSectionType, BlockVer,
    };
    use spectrum_ledger::{ModifierId, SerializedModifier, SlotNo};
    use spectrum_view::history::LedgerHistoryReadAsync;

    use crate::protocol_handler::diffusion::message::SyncStatus;
    use crate::protocol_handler::diffusion::service::{RemoteChainCmp, RemoteSync};

    pub struct EphemeralHistory {
        pub db: HashMap<BlockId, BlockSection>,
    }

    #[async_trait::async_trait]
    impl LedgerHistoryReadAsync for EphemeralHistory {
        async fn member(&self, id: &BlockId) -> bool {
            self.db.contains_key(id)
        }

        async fn contains(&self, id: &ModifierId) -> bool {
            self.db.contains_key(&<ModifierId as Into<BlockId>>::into(*id))
        }

        async fn get_section(&self, id: &BlockSectionId) -> Option<BlockSection> {
            match id {
                BlockSectionId::Header(id) | BlockSectionId::Payload(id) => self.db.get(id).cloned(),
            }
        }

        async fn get_tip(&self) -> BlockHeader {
            self.db
                .values()
                .filter_map(|s| match s {
                    BlockSection::Header(bh) => Some(bh),
                    _ => None,
                })
                .max_by_key(|hd| hd.slot)
                .cloned()
                .unwrap_or(BlockHeader::ORIGIN)
        }

        async fn get_tail(&self, n: usize) -> NonEmpty<BlockHeader> {
            let mut headers = self
                .db
                .values()
                .filter_map(|s| match s {
                    BlockSection::Header(bh) => Some(bh),
                    _ => None,
                })
                .collect::<Vec<_>>();
            headers.sort_by_key(|hd| hd.slot);
            NonEmpty::collect(
                headers[headers.len().saturating_sub(n)..]
                    .into_iter()
                    .map(|&hd| hd.clone()),
            )
            .unwrap_or(NonEmpty::singleton(BlockHeader::ORIGIN))
        }

        async fn follow(&self, pre_start: BlockId, cap: usize) -> Vec<BlockId> {
            let mut headers = self
                .db
                .values()
                .filter_map(|s| match s {
                    BlockSection::Header(bh) => Some(bh),
                    _ => None,
                })
                .collect::<Vec<_>>();
            headers.sort_by_key(|hd| hd.slot);
            let blocks = headers.into_iter().map(|hd| hd.id).collect::<Vec<_>>();
            let pos = blocks.iter().position(|blk| *blk == pre_start).unwrap();
            blocks[pos + 1..].to_vec()
        }

        async fn multi_get_raw(
            &self,
            sec_type: BlockSectionType,
            ids: Vec<ModifierId>,
        ) -> Vec<SerializedModifier> {
            todo!()
        }
    }

    #[async_std::test]
    async fn equal_chains() {
        let local_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let mut remote_chain = local_chain
            .clone()
            .into_iter()
            .map(|blk| blk.id)
            .collect::<Vec<_>>();
        remote_chain.reverse();
        let remote_ss = SyncStatus {
            height: SlotNo::from(31),
            last_blocks: remote_chain,
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(service.compare_remote(remote_ss).await, RemoteChainCmp::Equal);
    }

    #[async_std::test]
    async fn shorter_chain() {
        let local_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let mut remote_chain = local_chain.clone()[..30]
            .into_iter()
            .map(|blk| blk.id)
            .collect::<Vec<_>>();
        remote_chain.reverse();
        let remote_ss = SyncStatus {
            height: SlotNo::from(29),
            last_blocks: remote_chain.clone(),
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(
            service.compare_remote(remote_ss).await,
            RemoteChainCmp::Shorter(remote_chain[0])
        );
    }

    #[async_std::test]
    async fn nonsense() {
        let local_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let mut remote_chain = local_chain
            .clone()
            .into_iter()
            .map(|blk| blk.id)
            .collect::<Vec<_>>();
        remote_chain.reverse();
        let remote_ss = SyncStatus {
            height: SlotNo::from(25),
            last_blocks: remote_chain,
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(service.compare_remote(remote_ss).await, RemoteChainCmp::Nonsense);
    }

    #[async_std::test]
    async fn unresolved_fork() {
        let local_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let mut remote_chain = (0..16).map(|_| BlockId::random()).collect::<Vec<_>>();
        remote_chain.reverse();
        let remote_ss = SyncStatus {
            height: SlotNo::from(33),
            last_blocks: remote_chain,
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(
            service.compare_remote(remote_ss).await,
            RemoteChainCmp::Fork(None)
        );
    }

    #[async_std::test]
    async fn resolved_fork() {
        let remote_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let mut local_chain = remote_chain.clone()[..25].to_vec();
        let pre_fork_hdr = local_chain[24].id;
        let fork_hdr = BlockHeader {
            id: BlockId::random(),
            slot: SlotNo::from(25),
            version: BlockVer::INITIAL,
        };
        local_chain.push(fork_hdr.clone());
        let mut remote_chain_rev = remote_chain.clone();
        remote_chain_rev.reverse();
        println!(
            "Rem: {:?}",
            remote_chain.iter().map(|h| h.id).enumerate().collect::<Vec<_>>()
        );
        println!(
            "Loc: {:?}",
            local_chain.iter().map(|h| h.id).enumerate().collect::<Vec<_>>()
        );
        let remote_ss = SyncStatus {
            height: SlotNo::from(31),
            last_blocks: remote_chain_rev
                .clone()
                .into_iter()
                .map(|blk| blk.id)
                .collect::<Vec<_>>(),
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(
            service.compare_remote(remote_ss).await,
            RemoteChainCmp::Fork(Some(pre_fork_hdr))
        );
    }

    #[async_std::test]
    async fn significantly_longer_chain() {
        let local_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let mut remote_chain = (0..16).map(|_| BlockId::random()).collect::<Vec<_>>();
        remote_chain.reverse();
        let remote_ss = SyncStatus {
            height: SlotNo::from(133),
            last_blocks: remote_chain,
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(
            service.compare_remote(remote_ss).await,
            RemoteChainCmp::Longer(None)
        );
    }

    #[async_std::test]
    async fn longer_chain() {
        let remote_chain = (0..32)
            .map(|i| BlockHeader {
                id: BlockId::random(),
                slot: SlotNo::from(i as u64),
                version: BlockVer::INITIAL,
            })
            .collect::<Vec<_>>();
        let local_chain = remote_chain.clone()[..25].to_vec();
        let mut remote_chain_rev = remote_chain.clone();
        remote_chain_rev.reverse();
        println!(
            "Rem: {:?}",
            remote_chain.iter().map(|h| h.id).enumerate().collect::<Vec<_>>()
        );
        println!(
            "Loc: {:?}",
            local_chain.iter().map(|h| h.id).enumerate().collect::<Vec<_>>()
        );
        let remote_ss = SyncStatus {
            height: SlotNo::from(31),
            last_blocks: remote_chain_rev
                .clone()
                .into_iter()
                .map(|blk| blk.id)
                .collect::<Vec<_>>(),
        };
        let history = EphemeralHistory {
            db: local_chain
                .into_iter()
                .map(|hdr| (hdr.id, BlockSection::Header(hdr)))
                .collect(),
        };
        let service = RemoteSync {
            history: Arc::new(history),
        };
        assert_eq!(
            service.compare_remote(remote_ss).await,
            RemoteChainCmp::Longer(Some(
                remote_chain[25..].into_iter().map(|hdr| hdr.id.clone()).collect()
            ))
        );
    }
}
