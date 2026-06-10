//! Replica-side block replay.

use crate::chain::{BlockMeta, ChainState, CoroBlock, EngineError, decode_error};
use async_trait::async_trait;
use bytes::Bytes;
use commonware_codec::Decode;
use commonware_cryptography::sha256::Sha256;
use constantinople_application::executor;
use constantinople_primitives::{BlockCfg, Sealable};
use coro::single_sequencer::{BatchNumber, ReplicaApplication, replica::AppliedBatch};
use std::sync::{Arc, Mutex};
use tracing::info;

/// Replays canonical blocks fetched by [`coro::single_sequencer::Replica`].
///
/// Payload hashes are already verified against the canonical cursor before
/// this is invoked. The applier re-executes every transfer against its own
/// account state and rejects the block if the resulting state root (or parent
/// linkage) does not match the header, so replicas never diverge silently
/// from what the sequencer committed to DA.
pub struct BlockApplier {
    chain: Arc<Mutex<ChainState>>,
}

impl BlockApplier {
    /// Creates an applier starting from genesis state.
    ///
    /// The genesis allocation must match the sequencer's, otherwise the first
    /// block fails with a state-root mismatch.
    pub fn new(genesis: ChainState) -> Self {
        Self {
            chain: Arc::new(Mutex::new(genesis)),
        }
    }

    /// Returns a handle to the chain state for inspection (head, balances).
    pub fn chain(&self) -> Arc<Mutex<ChainState>> {
        self.chain.clone()
    }
}

#[async_trait]
impl ReplicaApplication for BlockApplier {
    type Metadata = BlockMeta;
    type Output = BlockMeta;
    type Error = EngineError;

    async fn apply_batch(
        &mut self,
        sequence: BatchNumber,
        payload: Bytes,
    ) -> Result<AppliedBatch<BlockMeta, BlockMeta>, EngineError> {
        let block = CoroBlock::decode_cfg(payload, &BlockCfg::default()).map_err(decode_error)?;

        let mut chain = self.chain.lock().expect("chain state lock poisoned");
        let expected_height = chain.height + 1;
        if block.header.height != expected_height || sequence.0 + 1 != expected_height {
            return Err(EngineError::HeightMismatch {
                chain: chain.height,
                got: block.header.height,
            });
        }
        if block.header.parent != chain.parent {
            return Err(EngineError::ParentMismatch);
        }

        let mut transactions = Vec::with_capacity(block.body.len());
        let mut transfers = Vec::with_capacity(block.body.len());
        for lazy in &block.body {
            let transaction = lazy.get().ok_or(EngineError::MalformedTransaction)?;
            let transfer =
                executor::prepare_transfer(transaction).ok_or(EngineError::MalformedTransaction)?;
            transactions.push(transaction.clone());
            transfers.push(transfer);
        }
        let changeset = executor::execute::<Sha256>(&chain.accounts, &transfers)
            .ok_or(EngineError::InvalidTransfer)?;
        for (key, account) in changeset {
            chain.accounts.insert(key, account);
        }

        let state_root = chain.state_root();
        if state_root != block.header.state_root {
            return Err(EngineError::StateRootMismatch);
        }
        let rebuilt_header = chain.next_header(
            block.header.context.leader.clone(),
            &transactions,
            state_root,
        );
        if rebuilt_header != block.header {
            return Err(EngineError::ParentMismatch);
        }

        let tx_count = transactions.len() as u64;
        let sealed = block.seal(&mut Sha256::default());
        chain.advance(*sealed.seal(), tx_count);

        info!(
            height = expected_height,
            txs = tx_count,
            state_root = ?state_root,
            "applied block"
        );
        let meta = BlockMeta {
            height: expected_height,
            state_root,
            tx_count,
        };
        Ok(AppliedBatch {
            metadata: meta,
            output: meta,
        })
    }
}
