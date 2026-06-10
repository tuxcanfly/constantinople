//! Shared chain state and block construction rules.
//!
//! The sequencer ([`crate::BlockBuilder`]) and replicas ([`crate::BlockApplier`])
//! must derive identical headers from identical history, so everything that
//! feeds the header lives here: the account map, parent digest, cumulative
//! transaction count, and the state/transactions root functions.

use bytes::{Buf, BufMut};
use commonware_codec::{Encode, FixedSize, Read, ReadExt, Write};
use commonware_consensus::{
    simplex::types::Context,
    types::{Epoch, Round, View},
};
use commonware_cryptography::{
    Hasher as _, ed25519,
    sha256::{Digest, Sha256},
};
use commonware_utils::non_empty_range;
use constantinople_application::executor::State;
use constantinople_primitives::{Account, AccountKey, Block, Header, SignedTransaction};

/// The transaction type carried in coro batches.
pub type Tx = SignedTransaction<Sha256>;

/// The block type published to DA.
///
/// This is the same `Block` the simplex engine uses; the consensus context is
/// synthesized by the sequencer (leader = sequencer key, view = height) so
/// existing tooling keeps decoding it.
pub type CoroBlock = Block<Digest, ed25519::PublicKey, Sha256>;

/// Errors shared by the sequencer and replica applications.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("block decode failed: {0}")]
    Decode(String),
    #[error("block height {got} does not extend chain height {chain}")]
    HeightMismatch { chain: u64, got: u64 },
    #[error("block parent does not match chain head")]
    ParentMismatch,
    #[error("block contains a malformed transaction")]
    MalformedTransaction,
    #[error("block contains an inapplicable transfer")]
    InvalidTransfer,
    #[error("re-executed state root does not match block header")]
    StateRootMismatch,
}

/// Batch metadata stored alongside each archived/applied block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Block height (sequence + 1; genesis is height 0).
    pub height: u64,
    /// Full-state root after applying the block.
    pub state_root: Digest,
    /// Number of transactions included in the block.
    pub tx_count: u64,
}

impl Write for BlockMeta {
    fn write(&self, buf: &mut impl BufMut) {
        self.height.write(buf);
        self.state_root.write(buf);
        self.tx_count.write(buf);
    }
}

impl FixedSize for BlockMeta {
    const SIZE: usize = u64::SIZE + Digest::SIZE + u64::SIZE;
}

impl Read for BlockMeta {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        Ok(Self {
            height: u64::read(buf)?,
            state_root: Digest::read(buf)?,
            tx_count: u64::read(buf)?,
        })
    }
}

/// Mutable chain state shared between block production and replay.
#[derive(Clone, Debug)]
pub struct ChainState {
    /// Full account state.
    pub accounts: State,
    /// Height of the last applied block (0 = genesis, no blocks applied).
    pub height: u64,
    /// Digest of the last applied block (genesis digest before any block).
    pub parent: Digest,
    /// Cumulative count of transactions across all applied blocks.
    pub total_txs: u64,
}

impl ChainState {
    /// Creates genesis state from an account allocation.
    pub fn genesis(alloc: impl IntoIterator<Item = (AccountKey, Account)>) -> Self {
        let mut hasher = Sha256::default();
        hasher.update(b"constantinople-coro-genesis");
        Self {
            accounts: alloc.into_iter().collect(),
            height: 0,
            parent: hasher.finalize(),
            total_txs: 0,
        }
    }

    /// Deterministic root over the full account state.
    ///
    /// This is a demo-grade commitment (hash of the sorted account map), not
    /// the QMDB MMR root the validator engine maintains.
    pub fn state_root(&self) -> Digest {
        let mut keys: Vec<&AccountKey> = self.accounts.keys().collect();
        keys.sort_unstable();
        let mut hasher = Sha256::default();
        for key in keys {
            hasher.update(key.as_ref());
            hasher.update(&self.accounts[key].encode());
        }
        hasher.finalize()
    }

    /// Builds the header for the next block given its body and post-state root.
    pub fn next_header(
        &self,
        leader: ed25519::PublicKey,
        transactions: &[Tx],
        state_root: Digest,
    ) -> Header<Digest, Digest, ed25519::PublicKey> {
        let height = self.height + 1;
        let mut hasher = Sha256::default();
        for transaction in transactions {
            hasher.update(transaction.message_digest().as_ref());
        }
        let transactions_root = hasher.finalize();
        let total_after = self.total_txs + transactions.len() as u64;
        Header {
            context: Context {
                round: Round::new(Epoch::zero(), View::new(height)),
                leader,
                parent: (View::new(self.height), self.parent),
            },
            parent: self.parent,
            height,
            timestamp: height,
            state_root,
            state_range: non_empty_range!(0, self.accounts.len() as u64),
            transactions_root,
            transactions_range: non_empty_range!(0, total_after.max(1)),
        }
    }

    /// Advances the chain head after a block is produced or applied.
    pub const fn advance(&mut self, block_digest: Digest, tx_count: u64) {
        self.height += 1;
        self.parent = block_digest;
        self.total_txs += tx_count;
    }
}

/// Returns an `EngineError::Decode` from any displayable error.
pub(crate) fn decode_error(err: impl std::fmt::Display) -> EngineError {
    EngineError::Decode(err.to_string())
}
