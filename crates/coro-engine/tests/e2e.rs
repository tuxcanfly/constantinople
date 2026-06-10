//! End-to-end sequencer + replica flow over coro's `MockBackend`.
//!
//! The sequencer builds three blocks of account transfers and publishes them
//! as blobs; a replica with the same genesis replays them. The replica source
//! withholds payload bytes so every payload is fetched through the (mock) DA
//! read path, exercising the same fallback a live replica uses when the
//! sequencer is unreachable.

use commonware_cryptography::{Signer as _, ed25519, sha256::Sha256};
use commonware_runtime::{Runner, Supervisor as _, deterministic};
use constantinople_coro_engine::{BlockApplier, BlockBuilder, ChainState, SequencerSource, Tx};
use constantinople_primitives::{
    Account, AccountKey, Nonce, TRANSACTION_NAMESPACE, Transaction, TransactionPublicKey,
};
use coro::{
    ChainConfig, NamespaceId, PublisherConfig, ReaderConfig, RetryConfig, VerificationMode,
    backend::MockBackend,
    single_sequencer::{
        BatchNumber, BatchPolicy, Replica, ReplicaConfig, SequencerConfig, SingleSequencer,
    },
};
use std::{num::NonZeroU64, sync::Arc, time::Duration};

const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

fn namespace() -> NamespaceId {
    let mut ns = [0u8; 29];
    ns[19..].copy_from_slice(b"coro-const");
    NamespaceId(ns)
}

fn publisher_config() -> PublisherConfig {
    PublisherConfig {
        chain: ChainConfig {
            namespace: namespace(),
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        },
        partition: "e2e-publisher".into(),
        retry: RetryConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(8),
            max_attempts: Some(5),
        },
        readback_timeout: Duration::from_secs(1),
        tx: Default::default(),
    }
}

fn reader_config() -> ReaderConfig {
    ReaderConfig {
        chain: ChainConfig {
            namespace: namespace(),
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        },
        verification: VerificationMode::RpcIncluded,
        read_timeout: Duration::from_secs(1),
    }
}

fn sequencer_config() -> SequencerConfig {
    SequencerConfig {
        partition: "e2e-sequencer".into(),
        batch_policy: BatchPolicy {
            max_txs: 2,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            max_delay: Duration::from_secs(1),
        },
        max_tx_bytes: 1024,
        max_metadata_bytes: 64,
    }
}

fn replica_config() -> ReplicaConfig {
    ReplicaConfig {
        partition: "e2e-replica".into(),
        max_payload_bytes: MAX_PAYLOAD_BYTES,
        max_metadata_bytes: 64,
        max_output_bytes: 64,
    }
}

struct Actor {
    signer: ed25519::PrivateKey,
    key: AccountKey,
}

impl Actor {
    fn new(seed: u64) -> Self {
        let signer = ed25519::PrivateKey::from_seed(seed);
        let key = AccountKey::from_public_key(&TransactionPublicKey::ed25519(signer.public_key()));
        Self { signer, key }
    }

    fn transfer(&self, to: &Self, value: u64, nonce: u64) -> Tx {
        Transaction::new(
            TransactionPublicKey::ed25519(self.signer.public_key()),
            TransactionPublicKey::ed25519(to.signer.public_key()),
            NonZeroU64::new(value).expect("transfer value must be non-zero"),
            nonce,
        )
        .seal_and_sign(&self.signer, TRANSACTION_NAMESPACE, &mut Sha256::default())
    }
}

fn genesis(actors: &[&Actor]) -> ChainState {
    ChainState::genesis(actors.iter().map(|actor| {
        (
            actor.key.clone(),
            Account {
                balance: 1_000,
                nonce: Nonce::new(0, 0),
            },
        )
    }))
}

#[test]
fn sequencer_blocks_replay_on_replica_via_da() {
    let runner = deterministic::Runner::default();
    runner.start(|context| async move {
        let alice = Actor::new(1);
        let bob = Actor::new(2);
        let leader = ed25519::PrivateKey::from_seed(0).public_key();
        let backend = MockBackend::new();

        let builder = BlockBuilder::new(genesis(&[&alice, &bob]), leader);
        let sequencer_chain = builder.chain();
        let sequencer = Arc::new(SingleSequencer::new(
            context.child("sequencer"),
            backend.clone(),
            publisher_config(),
            reader_config(),
            sequencer_config(),
            builder,
        ));

        // Block 1: two transfers.
        sequencer.submit(alice.transfer(&bob, 5, 0)).await.unwrap();
        sequencer.submit(bob.transfer(&alice, 3, 0)).await.unwrap();
        let cursor = sequencer.flush().await.unwrap().expect("block 1 cursor");
        assert_eq!(cursor.sequence, BatchNumber(0));

        // Block 2: two transfers.
        sequencer.submit(alice.transfer(&bob, 7, 1)).await.unwrap();
        sequencer.submit(bob.transfer(&alice, 2, 1)).await.unwrap();
        sequencer.flush().await.unwrap().expect("block 2 cursor");

        // Block 3: one transfer plus one inapplicable transaction (nonce 0 was
        // consumed in block 1) that the sequencer must drop.
        sequencer.submit(alice.transfer(&bob, 1, 2)).await.unwrap();
        sequencer.submit(bob.transfer(&alice, 9, 0)).await.unwrap();
        sequencer.flush().await.unwrap().expect("block 3 cursor");

        {
            let chain = sequencer_chain.lock().unwrap();
            assert_eq!(chain.height, 3);
            assert_eq!(
                chain.accounts[&alice.key].balance,
                1_000 - 5 + 3 - 7 + 2 - 1
            );
            assert_eq!(chain.accounts[&bob.key].balance, 1_000 + 5 - 3 + 7 - 2 + 1);
        }

        // Replica: same genesis, cursors from the sequencer, payloads forced
        // through the DA read path.
        let applier = BlockApplier::new(genesis(&[&alice, &bob]));
        let replica_chain = applier.chain();
        let replica = Replica::new(
            context.child("replica"),
            backend,
            reader_config(),
            replica_config(),
            applier,
        );
        let source = SequencerSource::cursors_only(sequencer.clone());
        let batches = replica.catch_up(&source).await.unwrap();
        assert_eq!(batches.len(), 3);

        // Replica state roots match what the sequencer committed per block.
        for batch in &batches {
            let archived = sequencer
                .archived_batch(batch.sequence)
                .await
                .unwrap()
                .expect("sequencer retains archived batch");
            assert_eq!(batch.output.state_root, archived.metadata.state_root);
            assert_eq!(batch.output.height, batch.sequence.0 + 1);
        }

        let sequencer_chain = sequencer_chain.lock().unwrap();
        let replica_chain = replica_chain.lock().unwrap();
        assert_eq!(replica_chain.height, 3);
        assert_eq!(replica_chain.parent, sequencer_chain.parent);
        assert_eq!(replica_chain.state_root(), sequencer_chain.state_root());
        assert_eq!(
            replica_chain.accounts[&alice.key],
            sequencer_chain.accounts[&alice.key]
        );
    });
}

#[test]
fn replica_rejects_tampered_genesis() {
    let runner = deterministic::Runner::default();
    runner.start(|context| async move {
        let alice = Actor::new(1);
        let bob = Actor::new(2);
        let leader = ed25519::PrivateKey::from_seed(0).public_key();
        let backend = MockBackend::new();

        let builder = BlockBuilder::new(genesis(&[&alice, &bob]), leader);
        let sequencer = Arc::new(SingleSequencer::new(
            context.child("sequencer"),
            backend.clone(),
            publisher_config(),
            reader_config(),
            sequencer_config(),
            builder,
        ));
        sequencer.submit(alice.transfer(&bob, 5, 0)).await.unwrap();
        sequencer.flush().await.unwrap().expect("block 1 cursor");

        // Replica seeded with a different allocation must reject block 1.
        let mut tampered = genesis(&[&alice, &bob]);
        tampered.accounts.insert(
            alice.key.clone(),
            Account {
                balance: 9_999,
                nonce: Nonce::new(0, 0),
            },
        );
        let replica = Replica::new(
            context.child("replica"),
            backend,
            reader_config(),
            replica_config(),
            BlockApplier::new(tampered),
        );
        let source = SequencerSource::new(sequencer);
        assert!(replica.catch_up(&source).await.is_err());
    });
}
