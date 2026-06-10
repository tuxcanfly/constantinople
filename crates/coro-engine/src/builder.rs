//! Sequencer-side block production.

use crate::chain::{BlockMeta, ChainState, CoroBlock, EngineError, Tx};
use async_trait::async_trait;
use bytes::Bytes;
use commonware_codec::Encode;
use commonware_cryptography::{ed25519, sha256::Sha256};
use constantinople_application::executor;
use constantinople_primitives::Sealable;
use coro::single_sequencer::{Application, BatchNumber, ExecutedBatch};
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

/// Builds, executes, and encodes blocks for [`coro::single_sequencer::SingleSequencer`].
///
/// Each coro batch becomes one block: transactions are filtered and executed
/// with the `constantinople-application` executor, the post-state root is
/// committed in the header, and the encoded block is the DA blob payload.
///
/// Transactions that fail static or account checks (bad nonce, insufficient
/// balance) are dropped from the block, mirroring the validator's
/// proposal-side filtering. Signature verification is expected to happen
/// upstream (the mempool webserver in real wiring).
pub struct BlockBuilder {
    chain: Arc<Mutex<ChainState>>,
    leader: ed25519::PublicKey,
}

impl BlockBuilder {
    /// Creates a builder starting from genesis state.
    pub fn new(genesis: ChainState, leader: ed25519::PublicKey) -> Self {
        Self {
            chain: Arc::new(Mutex::new(genesis)),
            leader,
        }
    }

    /// Returns a handle to the chain state for inspection (head, balances).
    pub fn chain(&self) -> Arc<Mutex<ChainState>> {
        self.chain.clone()
    }
}

#[async_trait]
impl Application for BlockBuilder {
    type Tx = Tx;
    type Metadata = BlockMeta;
    type Error = EngineError;

    async fn execute_batch(
        &mut self,
        sequence: BatchNumber,
        txs: Vec<Tx>,
    ) -> Result<ExecutedBatch<BlockMeta>, EngineError> {
        let mut chain = self.chain.lock().expect("chain state lock poisoned");
        let expected_height = chain.height + 1;
        if sequence.0 + 1 != expected_height {
            return Err(EngineError::HeightMismatch {
                chain: chain.height,
                got: sequence.0 + 1,
            });
        }

        let output = executor::propose(&chain.accounts, txs);
        if !output.invalid.is_empty() {
            debug!(
                dropped = output.invalid.len(),
                height = expected_height,
                "dropped inapplicable transactions from block"
            );
        }
        for (key, account) in &output.changeset {
            chain.accounts.insert(key.clone(), *account);
        }
        let state_root = chain.state_root();

        let header = chain.next_header(self.leader.clone(), &output.valid, state_root);
        let tx_count = output.valid.len() as u64;
        let block = CoroBlock::new(header, output.valid);
        let payload = Bytes::from(block.encode().to_vec());
        let sealed = block.seal(&mut Sha256::default());
        chain.advance(*sealed.seal(), tx_count);

        info!(
            height = expected_height,
            txs = tx_count,
            payload_bytes = payload.len(),
            state_root = ?state_root,
            "built block"
        );
        Ok(ExecutedBatch {
            payload,
            metadata: BlockMeta {
                height: expected_height,
                state_root,
                tx_count,
            },
        })
    }
}
