//! Live sequencer + replica round trip against a Celestia network.
//!
//! Requires a celestia-node JSON-RPC endpoint whose keyring holds a funded
//! account:
//!
//! ```bash
//! export CELESTIA_NODE_RPC=https://...        # celestia-node JSON-RPC URL
//! export CELESTIA_AUTH_TOKEN=...              # optional bearer token
//! export CELESTIA_NAMESPACE=0000008e5f679bf7116c  # 10-byte hex namespace suffix
//! export CELESTIA_GAS=75000                   # optional, gas limit per submit
//! export CELESTIA_GAS_PRICE=0.004             # optional, utia per gas
//! cargo test -p constantinople-coro-engine --test live_mocha -- --ignored --nocapture
//! ```

use commonware_cryptography::{Signer as _, ed25519, sha256::Sha256};
use commonware_runtime::{Runner, Supervisor as _, tokio as runtime_tokio};
use constantinople_coro_engine::{
    BlockApplier, BlockBuilder, ChainState, GasConfig, NodeRpcBackend, SequencerSource, Tx,
};
use constantinople_primitives::{
    Account, AccountKey, Nonce, TRANSACTION_NAMESPACE, Transaction, TransactionPublicKey,
};
use coro::{
    ChainConfig, NamespaceId, PublisherConfig, ReaderConfig, RetryConfig, VerificationMode,
    single_sequencer::{BatchPolicy, Replica, ReplicaConfig, SequencerConfig, SingleSequencer},
};
use std::{num::NonZeroU64, sync::Arc, time::Duration};

const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Builds the 29-byte v0 namespace from a hex-encoded 10-byte user suffix.
fn namespace_from_env() -> Option<NamespaceId> {
    let hex = env("CELESTIA_NAMESPACE")?;
    let suffix: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("CELESTIA_NAMESPACE is hex"))
        .collect();
    assert_eq!(
        suffix.len(),
        10,
        "CELESTIA_NAMESPACE must be 10 bytes of hex"
    );
    let mut ns = [0u8; 29];
    ns[19..].copy_from_slice(&suffix);
    Some(NamespaceId(ns))
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

#[ignore = "requires CELESTIA_NODE_RPC (+ funded node account) and CELESTIA_NAMESPACE"]
#[test]
fn live_block_publish_and_replica_sync() {
    let Some(url) = env("CELESTIA_NODE_RPC") else {
        eprintln!("set CELESTIA_NODE_RPC and CELESTIA_NAMESPACE to run");
        return;
    };
    let namespace = namespace_from_env().expect("CELESTIA_NAMESPACE must be set");
    let gas = GasConfig {
        gas: Some(
            env("CELESTIA_GAS")
                .map(|v| v.parse().expect("CELESTIA_GAS must be u64"))
                .unwrap_or(75_000),
        ),
        gas_price: Some(
            env("CELESTIA_GAS_PRICE")
                .map(|v| v.parse().expect("CELESTIA_GAS_PRICE must be f64"))
                .unwrap_or(0.004),
        ),
    };
    let backend = NodeRpcBackend::new(url, env("CELESTIA_AUTH_TOKEN"), gas);

    let chain_config = ChainConfig {
        namespace,
        max_payload_bytes: MAX_PAYLOAD_BYTES,
    };
    let publisher_config = PublisherConfig {
        chain: chain_config.clone(),
        partition: "live-coro-publisher".into(),
        retry: RetryConfig {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            max_attempts: Some(5),
        },
        readback_timeout: Duration::from_secs(60),
        tx: Default::default(),
    };
    let reader_config = ReaderConfig {
        chain: chain_config,
        verification: VerificationMode::RpcIncluded,
        read_timeout: Duration::from_secs(60),
    };
    let sequencer_config = SequencerConfig {
        partition: "live-coro-sequencer".into(),
        batch_policy: BatchPolicy {
            max_txs: 4,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            max_delay: Duration::from_secs(1),
        },
        max_tx_bytes: 1024,
        max_metadata_bytes: 64,
    };
    let replica_config = ReplicaConfig {
        partition: "live-coro-replica".into(),
        max_payload_bytes: MAX_PAYLOAD_BYTES,
        max_metadata_bytes: 64,
        max_output_bytes: 64,
    };

    let runner = runtime_tokio::Runner::default();
    runner.start(|context| async move {
        let alice = Actor::new(1);
        let bob = Actor::new(2);
        let leader = ed25519::PrivateKey::from_seed(0).public_key();

        let builder = BlockBuilder::new(genesis(&[&alice, &bob]), leader);
        let sequencer = Arc::new(SingleSequencer::new(
            context.child("sequencer"),
            backend.clone(),
            publisher_config,
            reader_config.clone(),
            sequencer_config,
            builder,
        ));

        // Build one block with two transfers and publish it to Celestia.
        sequencer.submit(alice.transfer(&bob, 5, 0)).await.unwrap();
        sequencer.submit(bob.transfer(&alice, 3, 0)).await.unwrap();
        let cursor = sequencer
            .flush()
            .await
            .expect("block publication failed")
            .expect("batch cursor");
        println!(
            "published block 1: celestia height {}, commitment {}",
            cursor.blob_ref.height,
            cursor
                .blob_ref
                .commitment
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        );

        // Replica: cursors from the sequencer, payload forced through a live
        // Celestia exact-ref read.
        let applier = BlockApplier::new(genesis(&[&alice, &bob]));
        let replica_chain = applier.chain();
        let replica = Replica::new(
            context.child("replica"),
            backend,
            reader_config,
            replica_config,
            applier,
        );
        let source = SequencerSource::cursors_only(sequencer.clone());
        let batches = replica
            .catch_up(&source)
            .await
            .expect("replica sync failed");
        assert_eq!(batches.len(), 1);

        let archived = sequencer
            .archived_batch(batches[0].sequence)
            .await
            .unwrap()
            .expect("archived batch");
        assert_eq!(batches[0].output.state_root, archived.metadata.state_root);

        let replica_chain = replica_chain.lock().unwrap();
        assert_eq!(replica_chain.height, 1);
        assert_eq!(replica_chain.accounts[&alice.key].balance, 998);
        assert_eq!(replica_chain.accounts[&bob.key].balance, 1_002);
        println!(
            "replica synced block 1 from DA: state root {:?}",
            replica_chain.state_root(),
        );
    });
}
