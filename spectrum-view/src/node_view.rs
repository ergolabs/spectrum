use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use futures::channel::mpsc::Receiver;

use spectrum_ledger::Modifier;

use crate::history::LedgerHistoryWrite;
use crate::state::{Cells, LedgerStateWrite};

#[derive(Clone, Debug)]
pub enum NodeViewIn {
    ApplyModifier(Modifier),
}

pub trait ErrorHandler {
    fn on_invalid_modifier(&self, err: InvalidModifier);
}

#[derive(Eq, PartialEq, Debug, thiserror::Error)]
pub enum InvalidModifier {
    #[error("Modifier is invalid")]
    InvalidSection(),
}

pub struct NodeView<TState, THistory, TMempool, TErrh> {
    state: TState,
    history: THistory,
    mempool: TMempool,
    err_handler: TErrh,
    inbox: Receiver<NodeViewIn>,
}

impl<TState, THistory, TMempool, TErrh> NodeView<TState, THistory, TMempool, TErrh>
where
    TState: Cells + LedgerStateWrite,
    THistory: LedgerHistoryWrite,
    TErrh: ErrorHandler,
{
    fn on_event(&self, event: NodeViewIn) {
        match event {
            NodeViewIn::ApplyModifier(md) => {
                self.apply_modifier(&md)
                    .unwrap_or_else(|e| self.err_handler.on_invalid_modifier(e));
            }
        }
    }

    fn apply_modifier(&self, modifier: &Modifier) -> Result<(), InvalidModifier> {
        match modifier {
            Modifier::BlockHeader(hd) => { // validate(hd) -> VR<Valid<HD>, RuleViol>
                todo!()
            }
            Modifier::BlockBody(blk) => {
                todo!()
            }
            Modifier::Transaction(_) => {
                todo!()
            }
        }
    }
}

impl<TState, THistory, TMempool, TErrh> Stream for NodeView<TState, THistory, TMempool, TErrh>
where
    TState: Cells + LedgerStateWrite + Unpin,
    THistory: LedgerHistoryWrite + Unpin,
    TMempool: Unpin,
    TErrh: ErrorHandler + Unpin,
{
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            match self.inbox.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => {
                    self.on_event(event);
                    continue;
                }
                Poll::Pending => {}
                Poll::Ready(None) => unreachable!(),
            }
            return Poll::Pending;
        }
    }
}

#[async_trait::async_trait]
pub trait NodeViewWriteAsync: Send + Sync + Clone {
    async fn apply_modifier(&mut self, modifier: Modifier);
}
