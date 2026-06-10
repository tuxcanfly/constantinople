//! DA-backed Constantinople sequencer and replica binary.

use axum::Router;
use clap::{Parser, Subcommand};
use commonware_codec::{Decode as _, ReadExt as _};
use commonware_consensus::{
    Reporter as _,
    marshal::Update,
    simplex::types::Context,
    types::{Epoch, Round, View},
};
use commonware_cryptography::{
    Hasher as _, Signer as _, ed25519,
    sha256::{Digest, Sha256},
};
use commonware_formatting::from_hex;
use commonware_parallel::Sequential;
use commonware_runtime::{Runner as _, Supervisor as _, tokio as runtime_tokio};
use commonware_utils::{Acknowledgement as _, acknowledgement::Exact, non_empty_range};
use constantinople_coro_engine::{
    BlockApplier, BlockBuilder, ChainState, GasConfig, NodeRpcBackend,
};
use constantinople_mempool::{
    TransactionSource as _,
    webserver::{self, AccountReader, Mailbox},
};
use constantinople_primitives::{
    Account, AccountKey, BlockCfg, Header, Nonce, Sealable as _, TransactionPublicKey,
};
use coro::{
    ChainConfig, NamespaceId, PublisherConfig, ReaderConfig, RetryConfig, VerificationMode,
    single_sequencer::{BatchPolicy, Replica, ReplicaConfig, SingleSequencer},
};
use coro_demo::{HistoryServerConfig, HttpReplicaSource};
use futures::future::{BoxFuture, FutureExt as _};
use serde::Deserialize;
use std::{
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::time;
use tracing::{error, info, warn};

const MEMPOOL_MAILBOX_SIZE: usize = 65_536;
const DEFAULT_MAX_POOL_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_TX_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_METADATA_BYTES: usize = 64;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 64;

#[derive(Debug, Parser)]
#[command(name = "constantinople-sequencer")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the write-capable sequencer: mempool HTTP plus coro history HTTP.
    Run { config: PathBuf },
    /// Run a read-only replica: coro sync plus explorer account endpoints.
    Replica { config: PathBuf },
}

#[derive(Debug, Deserialize)]
struct SequencerFile {
    #[serde(default = "default_storage_dir")]
    storage_dir: PathBuf,
    #[serde(default = "default_partition_prefix")]
    partition_prefix: String,
    #[serde(default = "default_worker_threads")]
    worker_threads: usize,
    #[serde(default = "default_log_level")]
    log_level: String,
    #[serde(default = "default_mempool_listen")]
    mempool_listen: SocketAddr,
    #[serde(default = "default_history_listen")]
    history_listen: SocketAddr,
    #[serde(default = "default_signer_seed")]
    signer_seed: u64,
    #[serde(default)]
    private_key: Option<String>,
    #[serde(default)]
    genesis: GenesisConfig,
    #[serde(default)]
    batch: BatchConfig,
    #[serde(default)]
    celestia: CelestiaConfig,
    #[serde(default = "default_serve_payloads")]
    serve_payloads: bool,
}

#[derive(Debug, Deserialize)]
struct ReplicaFile {
    #[serde(default = "default_replica_storage_dir")]
    storage_dir: PathBuf,
    #[serde(default = "default_replica_partition_prefix")]
    partition_prefix: String,
    #[serde(default = "default_worker_threads")]
    worker_threads: usize,
    #[serde(default = "default_log_level")]
    log_level: String,
    #[serde(default = "default_replica_listen")]
    listen: SocketAddr,
    sequencer_url: String,
    #[serde(default)]
    genesis: GenesisConfig,
    #[serde(default)]
    batch: BatchConfig,
    #[serde(default)]
    celestia: CelestiaConfig,
    #[serde(default = "default_sync_interval_ms")]
    sync_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct CelestiaConfig {
    rpc_url: String,
    #[serde(default)]
    auth_token: Option<String>,
    #[serde(default = "default_auth_token_env")]
    auth_token_env: Option<String>,
    namespace: String,
    #[serde(default)]
    gas: Option<u64>,
    #[serde(default)]
    gas_price: Option<f64>,
}

impl Default for CelestiaConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:26658".to_string(),
            auth_token: None,
            auth_token_env: default_auth_token_env(),
            namespace: "00000000000000000000".to_string(),
            gas: Some(75_000),
            gas_price: Some(0.004),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BatchConfig {
    #[serde(default = "default_max_txs")]
    max_txs: usize,
    #[serde(default = "default_max_payload_bytes")]
    max_payload_bytes: usize,
    #[serde(default = "default_max_tx_bytes")]
    max_tx_bytes: usize,
    #[serde(default = "default_max_metadata_bytes")]
    max_metadata_bytes: usize,
    #[serde(default = "default_max_output_bytes")]
    max_output_bytes: usize,
    #[serde(default = "default_max_delay_ms")]
    max_delay_ms: u64,
    #[serde(default = "default_readback_timeout_ms")]
    readback_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    read_timeout_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_txs: default_max_txs(),
            max_payload_bytes: default_max_payload_bytes(),
            max_tx_bytes: default_max_tx_bytes(),
            max_metadata_bytes: default_max_metadata_bytes(),
            max_output_bytes: default_max_output_bytes(),
            max_delay_ms: default_max_delay_ms(),
            readback_timeout_ms: default_readback_timeout_ms(),
            read_timeout_ms: default_read_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GenesisConfig {
    #[serde(default = "default_genesis_accounts")]
    accounts: u32,
    #[serde(default = "default_genesis_seed_offset")]
    seed_offset: u64,
    #[serde(default = "default_genesis_balance")]
    balance: u64,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        Self {
            accounts: default_genesis_accounts(),
            seed_offset: default_genesis_seed_offset(),
            balance: default_genesis_balance(),
        }
    }
}

#[derive(Clone)]
struct ChainAccountReader {
    chain: Arc<std::sync::Mutex<ChainState>>,
}

impl AccountReader for ChainAccountReader {
    fn get<'a>(&'a self, public_key: TransactionPublicKey) -> BoxFuture<'a, Option<Account>> {
        async move {
            let key = AccountKey::from_public_key(&public_key);
            self.chain
                .lock()
                .expect("chain state lock poisoned")
                .accounts
                .get(&key)
                .copied()
        }
        .boxed()
    }
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Run { config } => run_sequencer(config),
        Command::Replica { config } => run_replica(config),
    };
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run_sequencer(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let config: SequencerFile = load_yaml(&path)?;
    init_tracing(&config.log_level);
    let runtime = runtime_tokio::Runner::new(
        runtime_tokio::Config::new()
            .with_storage_directory(&config.storage_dir)
            .with_worker_threads(config.worker_threads),
    );

    runtime.start(|context| async move {
        let signer = signer(&config.private_key, config.signer_seed);
        let chain_config = chain_config(&config.celestia, &config.batch);
        let backend = backend(&config.celestia);
        let builder = BlockBuilder::new(genesis(&config.genesis), signer.public_key());
        let chain = builder.chain();
        let sequencer = Arc::new(SingleSequencer::new(
            context.child("sequencer"),
            backend,
            publisher_config(&config.partition_prefix, &chain_config, &config.batch),
            reader_config(&chain_config, &config.batch),
            sequencer_config(&config.partition_prefix, &config.batch),
            builder,
        ));
        let recovery = sequencer
            .recover()
            .await
            .expect("sequencer recovery failed");
        info!(
            finalized = recovery.finalized_batches.len(),
            pending = recovery.pending_batches.len(),
            "sequencer recovered"
        );

        let (mailbox, receiver) = Mailbox::channel(MEMPOOL_MAILBOX_SIZE);
        let account_reader: Arc<OnceLock<Arc<dyn AccountReader>>> = Arc::new(OnceLock::new());
        let reader: Arc<dyn AccountReader> = Arc::new(ChainAccountReader {
            chain: chain.clone(),
        });
        let _ = account_reader.set(reader);
        let mempool = webserver::Actor::new(
            context.child("mempool"),
            webserver::Config {
                max_pool_bytes: DEFAULT_MAX_POOL_BYTES,
                max_propose_bytes: config.batch.max_payload_bytes,
                namespace: constantinople_primitives::TRANSACTION_NAMESPACE,
                drop_grace_blocks: 2,
                signature_strategy: Sequential,
                hash_strategy: Sequential,
            },
            mailbox.clone(),
            receiver,
            account_reader,
        );
        let mempool_listener = tokio::net::TcpListener::bind(config.mempool_listen)
            .await
            .expect("failed to bind mempool HTTP listener");
        let mempool_handle = mempool.start(mempool_listener);
        info!(listen = %config.mempool_listen, "mempool HTTP listening");

        let history = sequencer.clone() as Arc<dyn coro_demo::SequencerHistory>;
        let history_config = HistoryServerConfig {
            serve_payloads: config.serve_payloads,
        };
        let history_listen = config.history_listen;
        let history_handle = tokio::spawn(async move {
            coro_demo::serve(history, history_config, history_listen)
                .await
                .expect("coro history server exited");
        });
        info!(listen = %history_listen, "coro history HTTP listening");

        let loop_handle = tokio::spawn(run_sequencer_loop(
            sequencer.clone(),
            mailbox,
            chain,
            signer.public_key(),
        ));

        tokio::select! {
            _ = mempool_handle => error!("mempool HTTP task exited"),
            result = history_handle => error!(?result, "history HTTP task exited"),
            result = loop_handle => error!(?result, "sequencer loop exited"),
        }
    });
    Ok(())
}

fn run_replica(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let config: ReplicaFile = load_yaml(&path)?;
    init_tracing(&config.log_level);
    let runtime = runtime_tokio::Runner::new(
        runtime_tokio::Config::new()
            .with_storage_directory(&config.storage_dir)
            .with_worker_threads(config.worker_threads),
    );

    runtime.start(|context| async move {
        let chain_config = chain_config(&config.celestia, &config.batch);
        let backend = backend(&config.celestia);
        let applier = BlockApplier::new(genesis(&config.genesis));
        let chain = applier.chain();
        let replica = Arc::new(Replica::new(
            context.child("replica"),
            backend,
            reader_config(&chain_config, &config.batch),
            replica_config(&config.partition_prefix, &config.batch),
            applier,
        ));
        let recovery = replica.recover().await.expect("replica recovery failed");
        info!(
            next_sequence = recovery.next_sequence.0,
            "replica recovered"
        );

        let reader: Arc<dyn AccountReader> = Arc::new(ChainAccountReader {
            chain: chain.clone(),
        });
        let round_chain = chain.clone();
        let round = Arc::new(move || {
            round_chain
                .lock()
                .expect("chain state lock poisoned")
                .height
        });
        let app = webserver::read_only_router(reader, round);
        let listen = config.listen;
        let read_only_handle = tokio::spawn(serve_axum(app, listen));
        info!(%listen, "replica read-only HTTP listening");

        let source = HttpReplicaSource::new(config.sequencer_url);
        let sync_interval = Duration::from_millis(config.sync_interval_ms);
        let sync_handle = tokio::spawn(run_replica_loop(replica, source, sync_interval));

        tokio::select! {
            result = read_only_handle => error!(?result, "replica read-only HTTP task exited"),
            result = sync_handle => error!(?result, "replica sync task exited"),
        }
    });
    Ok(())
}

async fn run_sequencer_loop(
    sequencer: Arc<SingleSequencer<runtime_tokio::Context, NodeRpcBackend, BlockBuilder>>,
    mut mailbox: Mailbox<Digest, ed25519::PublicKey, Sha256>,
    chain: Arc<std::sync::Mutex<ChainState>>,
    leader: ed25519::PublicKey,
) {
    loop {
        let parent = parent_header(&chain, leader.clone());
        let context = proposal_context(&parent, leader.clone());
        let txs = mailbox.propose(&parent, &context).await;
        if txs.is_empty() {
            match sequencer.process_ready().await {
                Ok(Some(cursor)) => {
                    report_published(&sequencer, &mut mailbox, cursor.sequence).await;
                }
                Ok(None) => time::sleep(Duration::from_millis(100)).await,
                Err(error) => {
                    warn!(error = %error, "sequencer process_ready failed");
                    recover_and_report(&sequencer, &mut mailbox).await;
                }
            }
            continue;
        }

        for tx in txs {
            if let Err(error) = sequencer.submit(tx).await {
                warn!(error = %error, "sequencer rejected transaction");
            }
        }

        loop {
            match sequencer.flush().await {
                Ok(Some(cursor)) => {
                    report_published(&sequencer, &mut mailbox, cursor.sequence).await;
                }
                Ok(None) => break,
                Err(error) => {
                    warn!(error = %error, "sequencer flush failed");
                    recover_and_report(&sequencer, &mut mailbox).await;
                    break;
                }
            }
        }
    }
}

/// Re-runs sequencer recovery after a failed flush/process so batches that
/// were archived but never published to DA are re-published immediately
/// instead of waiting for a process restart (a later batch publishing first
/// would leave a cursor gap that stalls replicas).
async fn recover_and_report(
    sequencer: &Arc<SingleSequencer<runtime_tokio::Context, NodeRpcBackend, BlockBuilder>>,
    mailbox: &mut Mailbox<Digest, ed25519::PublicKey, Sha256>,
) {
    loop {
        time::sleep(Duration::from_secs(1)).await;
        match sequencer.recover().await {
            Ok(report) => {
                for sequence in report.resumed_batches {
                    report_published(sequencer, mailbox, sequence).await;
                }
                return;
            }
            Err(error) => warn!(error = %error, "sequencer recovery failed"),
        }
    }
}

async fn run_replica_loop(
    replica: Arc<Replica<runtime_tokio::Context, NodeRpcBackend, BlockApplier>>,
    source: HttpReplicaSource,
    interval: Duration,
) {
    loop {
        match replica.catch_up(&source).await {
            Ok(batches) if batches.is_empty() => {}
            Ok(batches) => {
                if let Some(last) = batches.last() {
                    info!(
                        applied = batches.len(),
                        height = last.output.height,
                        state_root = ?last.output.state_root,
                        "replica caught up"
                    );
                }
            }
            Err(error) => warn!(error = %error, "replica catch-up failed"),
        }
        time::sleep(interval).await;
    }
}

async fn report_published(
    sequencer: &SingleSequencer<runtime_tokio::Context, NodeRpcBackend, BlockBuilder>,
    mailbox: &mut Mailbox<Digest, ed25519::PublicKey, Sha256>,
    sequence: coro::BatchNumber,
) {
    let archived = match sequencer.archived_batch(sequence).await {
        Ok(Some(batch)) => batch,
        Ok(None) => {
            warn!(
                sequence = sequence.0,
                "published batch missing from archive"
            );
            return;
        }
        Err(error) => {
            warn!(sequence = sequence.0, error = %error, "failed to load published batch");
            return;
        }
    };
    let block = match constantinople_coro_engine::CoroBlock::decode_cfg(
        archived.payload,
        &BlockCfg::default(),
    ) {
        Ok(block) => block,
        Err(error) => {
            warn!(sequence = sequence.0, error = %error, "failed to decode published block");
            return;
        }
    };
    let sealed = block.seal(&mut Sha256::default());
    let (acknowledgement, waiter) = Exact::handle();
    let _ = mailbox.report(Update::Block(sealed, acknowledgement));
    if let Err(error) = waiter.await {
        warn!(sequence = sequence.0, error = ?error, "mempool report acknowledgement failed");
    }
}

async fn serve_axum(app: Router, listen: SocketAddr) {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .expect("failed to bind HTTP listener");
    axum::serve(listener, app)
        .await
        .expect("HTTP server exited");
}

fn parent_header(
    chain: &Arc<std::sync::Mutex<ChainState>>,
    leader: ed25519::PublicKey,
) -> Header<Digest, Digest, ed25519::PublicKey> {
    let chain = chain.lock().expect("chain state lock poisoned");
    Header {
        context: Context {
            round: Round::new(Epoch::zero(), View::new(chain.height)),
            leader,
            parent: (View::new(chain.height.saturating_sub(1)), chain.parent),
        },
        parent: chain.parent,
        height: chain.height,
        timestamp: chain.height,
        state_root: chain.state_root(),
        state_range: non_empty_range!(0, (chain.accounts.len() as u64).max(1)),
        transactions_root: Sha256::hash(b"coro-sequencer-parent"),
        transactions_range: non_empty_range!(0, chain.total_txs.max(1)),
    }
}

const fn proposal_context(
    parent: &Header<Digest, Digest, ed25519::PublicKey>,
    leader: ed25519::PublicKey,
) -> Context<Digest, ed25519::PublicKey> {
    Context {
        round: Round::new(Epoch::zero(), View::new(parent.height + 1)),
        leader,
        parent: (View::new(parent.height), parent.parent),
    }
}

fn genesis(config: &GenesisConfig) -> ChainState {
    let accounts = (0..config.accounts).map(|index| {
        let key = ed25519::PrivateKey::from_seed(config.seed_offset + u64::from(index));
        (
            AccountKey::from_public_key(&TransactionPublicKey::ed25519(key.public_key())),
            Account {
                balance: config.balance,
                nonce: Nonce::default(),
            },
        )
    });
    ChainState::genesis(accounts)
}

fn signer(private_key: &Option<String>, seed: u64) -> ed25519::PrivateKey {
    let Some(private_key) = private_key else {
        return ed25519::PrivateKey::from_seed(seed);
    };
    let bytes = from_hex(private_key).expect("private_key must be hex");
    ed25519::PrivateKey::read(&mut &bytes[..]).expect("private_key must decode as ed25519")
}

fn backend(config: &CelestiaConfig) -> NodeRpcBackend {
    NodeRpcBackend::new(
        config.rpc_url.clone(),
        auth_token(config),
        GasConfig {
            gas: config.gas,
            gas_price: config.gas_price,
        },
    )
}

fn auth_token(config: &CelestiaConfig) -> Option<String> {
    config.auth_token.clone().or_else(|| {
        let name = config.auth_token_env.as_ref()?;
        std::env::var(name).ok().filter(|value| !value.is_empty())
    })
}

fn chain_config(config: &CelestiaConfig, batch: &BatchConfig) -> ChainConfig {
    ChainConfig {
        namespace: namespace(&config.namespace),
        max_payload_bytes: batch.max_payload_bytes,
    }
}

fn namespace(value: &str) -> NamespaceId {
    let bytes = from_hex(value).expect("namespace must be hex");
    let mut namespace = [0u8; 29];
    match bytes.len() {
        10 => namespace[19..].copy_from_slice(&bytes),
        29 => namespace.copy_from_slice(&bytes),
        other => {
            panic!("namespace must be 10-byte suffix or full 29-byte namespace, got {other} bytes")
        }
    }
    NamespaceId(namespace)
}

fn publisher_config(prefix: &str, chain: &ChainConfig, batch: &BatchConfig) -> PublisherConfig {
    PublisherConfig {
        chain: chain.clone(),
        partition: format!("{prefix}-publisher"),
        retry: RetryConfig {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            max_attempts: Some(5),
        },
        readback_timeout: Duration::from_millis(batch.readback_timeout_ms),
        tx: Default::default(),
    }
}

fn reader_config(chain: &ChainConfig, batch: &BatchConfig) -> ReaderConfig {
    ReaderConfig {
        chain: chain.clone(),
        verification: VerificationMode::RpcIncluded,
        read_timeout: Duration::from_millis(batch.read_timeout_ms),
    }
}

fn sequencer_config(prefix: &str, batch: &BatchConfig) -> coro::SequencerConfig {
    coro::SequencerConfig {
        partition: format!("{prefix}-sequencer"),
        batch_policy: BatchPolicy {
            max_txs: batch.max_txs,
            max_payload_bytes: batch.max_payload_bytes,
            max_delay: Duration::from_millis(batch.max_delay_ms),
        },
        max_tx_bytes: batch.max_tx_bytes,
        max_metadata_bytes: batch.max_metadata_bytes,
    }
}

fn replica_config(prefix: &str, batch: &BatchConfig) -> ReplicaConfig {
    ReplicaConfig {
        partition: format!("{prefix}-replica"),
        max_payload_bytes: batch.max_payload_bytes,
        max_metadata_bytes: batch.max_metadata_bytes,
        max_output_bytes: batch.max_output_bytes,
    }
}

fn load_yaml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Box<dyn Error>> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&raw)?)
}

fn init_tracing(level: &str) {
    let level = level.parse().unwrap_or(tracing::Level::INFO);
    let _ = tracing_subscriber::fmt().with_max_level(level).try_init();
}

fn default_storage_dir() -> PathBuf {
    PathBuf::from("local/coro-sequencer")
}

fn default_replica_storage_dir() -> PathBuf {
    PathBuf::from("local/coro-replica")
}

fn default_partition_prefix() -> String {
    "coro-sequencer".to_string()
}

fn default_replica_partition_prefix() -> String {
    "coro-replica".to_string()
}

const fn default_worker_threads() -> usize {
    2
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_mempool_listen() -> SocketAddr {
    "127.0.0.1:8080".parse().expect("valid socket")
}

fn default_history_listen() -> SocketAddr {
    "127.0.0.1:8081".parse().expect("valid socket")
}

fn default_replica_listen() -> SocketAddr {
    "127.0.0.1:8082".parse().expect("valid socket")
}

const fn default_signer_seed() -> u64 {
    0
}

const fn default_genesis_accounts() -> u32 {
    10
}

const fn default_genesis_seed_offset() -> u64 {
    1000
}

const fn default_genesis_balance() -> u64 {
    1_000
}

const fn default_max_txs() -> usize {
    10_000
}

const fn default_max_payload_bytes() -> usize {
    DEFAULT_MAX_PAYLOAD_BYTES
}

const fn default_max_tx_bytes() -> usize {
    DEFAULT_MAX_TX_BYTES
}

const fn default_max_metadata_bytes() -> usize {
    DEFAULT_MAX_METADATA_BYTES
}

const fn default_max_output_bytes() -> usize {
    DEFAULT_MAX_OUTPUT_BYTES
}

const fn default_max_delay_ms() -> u64 {
    1_000
}

const fn default_readback_timeout_ms() -> u64 {
    60_000
}

const fn default_read_timeout_ms() -> u64 {
    60_000
}

const fn default_sync_interval_ms() -> u64 {
    500
}

const fn default_serve_payloads() -> bool {
    true
}

fn default_auth_token_env() -> Option<String> {
    Some("CELESTIA_AUTH_TOKEN".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ten_byte_namespace_suffix() {
        let namespace = namespace("0000008e5f679bf7116c");
        assert_eq!(
            &namespace.0[19..],
            &[0, 0, 0, 0x8e, 0x5f, 0x67, 0x9b, 0xf7, 0x11, 0x6c]
        );
    }

    #[test]
    fn default_genesis_matches_spammer_seed_range() {
        let genesis = genesis(&GenesisConfig::default());
        let signer = ed25519::PrivateKey::from_seed(1000);
        let key = AccountKey::from_public_key(&TransactionPublicKey::ed25519(signer.public_key()));
        assert_eq!(genesis.accounts[&key].balance, 1_000);
    }
}
