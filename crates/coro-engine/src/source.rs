//! In-process [`ReplicaSource`] backed by a [`SingleSequencer`].
//!
//! Useful for tests and single-binary demos. Production replicas should use
//! an HTTP source (`coro-demo`) instead.

use async_trait::async_trait;
use bytes::Bytes;
use commonware_runtime::{Clock, Storage};
use coro::{
    backend::Backend,
    single_sequencer::{
        Application, BatchCursor, BatchNumber, ReplicaSource, SequencerError, SingleSequencer,
    },
};
use std::sync::Arc;

/// Serves canonical head/cursor/payload queries straight off a sequencer.
pub struct SequencerSource<C, B, A>
where
    A: Application,
{
    sequencer: Arc<SingleSequencer<C, B, A>>,
    /// When `false`, payload queries return `None`, forcing replicas to fetch
    /// payload bytes from Celestia (exercises the DA fallback path).
    serve_payloads: bool,
}

impl<C, B, A> SequencerSource<C, B, A>
where
    A: Application,
{
    /// Creates a source that serves payloads from the sequencer archive.
    pub const fn new(sequencer: Arc<SingleSequencer<C, B, A>>) -> Self {
        Self {
            sequencer,
            serve_payloads: true,
        }
    }

    /// Creates a source that withholds payloads so replicas read them from DA.
    pub const fn cursors_only(sequencer: Arc<SingleSequencer<C, B, A>>) -> Self {
        Self {
            sequencer,
            serve_payloads: false,
        }
    }
}

fn to_core<E: std::fmt::Display>(err: SequencerError<E>) -> coro::Error {
    match err {
        SequencerError::Core(err) => err,
        SequencerError::Application(err) => coro::Error::Decode(err.to_string()),
    }
}

#[async_trait]
impl<C, B, A> ReplicaSource for SequencerSource<C, B, A>
where
    C: Clock + Storage + Send + Sync + 'static,
    B: Backend,
    A: Application + Send + Sync,
{
    async fn head(&self) -> coro::Result<Option<BatchNumber>> {
        self.sequencer.head().await.map_err(to_core)
    }

    async fn cursor(&self, sequence: BatchNumber) -> coro::Result<Option<BatchCursor>> {
        self.sequencer.batch_cursor(sequence).await.map_err(to_core)
    }

    async fn payload(&self, sequence: BatchNumber) -> coro::Result<Option<Bytes>> {
        if !self.serve_payloads {
            return Ok(None);
        }
        self.sequencer.payload(sequence).await.map_err(to_core)
    }
}
