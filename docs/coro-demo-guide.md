# Running the Coro/Celestia DA Demo

A step-by-step guide to running the DEVX-01 demo: a Constantinople chain whose
blocks are published to Celestia instead of ordered by the p2p validator
network. One sequencer accepts transactions through the existing mempool HTTP
API and posts blocks as blobs; replicas sync with nothing but the sequencer's
HTTP URL and a Celestia RPC endpoint — no peer lists, no bootstrappers, no DKG.

See [devx-01-coro-da.md](./devx-01-coro-da.md) for the design; this document
is purely operational.

```text
spammer ──> mempool HTTP (:8080) ──> sequencer loop ──> Celestia (mocha-4)
                                          │
                                          v
                              history HTTP (:8081)
                                          │
                                          v
explorer ──> read-only HTTP (:8082) <── replica (re-executes every block)
```

## Prerequisites

- Rust stable (workspace pins `rust-version = "1.88"`).
- A Celestia **node RPC** endpoint on mocha-4 (a celestia-node light node or a
  hosted provider such as QuickNode). The demo speaks node JSON-RPC
  (`blob.Submit` / `blob.Get`) with bearer-token auth — gRPC is not used.
- The node's signing account must be funded. Get mocha TIA from the
  [Celestia faucet](https://docs.celestia.org/how-to-guides/mocha-testnet#mocha-testnet-faucet).
  Public nodes enforce a minimum gas price of `0.004` utia/gas; a 10-tx demo
  block costs roughly 300 utia in fees.
- A namespace for your chain: any 10-byte hex suffix, e.g. generated with
  `openssl rand -hex 10`.

## Configuration

Two YAML files at the repo root (both gitignored — they reference your
endpoint, so keep them out of commits):

`sequencer.yaml`:

```yaml
storage_dir: local/mocha-sequencer
partition_prefix: mocha-sequencer
mempool_listen: 127.0.0.1:8080
history_listen: 127.0.0.1:8081

celestia:
  rpc_url: https://your-celestia-node.example.com/
  auth_token_env: CELESTIA_AUTH_TOKEN
  namespace: 0000008e5f679bf7116c   # your 10-byte hex suffix
  gas: 75000
  gas_price: 0.004

genesis:
  accounts: 10
  seed_offset: 1000
  balance: 1000
```

`replica.yaml`:

```yaml
storage_dir: local/mocha-replica-1
partition_prefix: mocha-replica-1
sequencer_url: http://127.0.0.1:8081
listen: 127.0.0.1:8082

celestia:
  rpc_url: https://your-celestia-node.example.com/
  auth_token_env: CELESTIA_AUTH_TOKEN
  namespace: 0000008e5f679bf7116c   # must match the sequencer

genesis:
  accounts: 10                       # must match the sequencer
  seed_offset: 1000
  balance: 1000
```

**Genesis must cover every key the spammer uses.** The demo executor does
not materialize missing accounts (unlike the validator engine) — transfers
whose sender *or recipient* is unfunded are silently dropped. The spammer
derives keys from `seed_offset`, `accounts` keys per submitter, so:
`genesis.accounts >= spammer accounts × relayer_submitters`, same
`seed_offset`. The defaults above match the spammer's defaults exactly.

## Build

```bash
cargo build --bin constantinople-sequencer --bin constantinople-spammer
```

## Run

**Storage must be fresh on every restart.** The chain state lives in memory
and restarts from genesis, but coro's batch archive is durable — restarting
over old archives wedges immediately with height mismatches:

```bash
rm -rf local/mocha-sequencer local/mocha-replica-1
```

Then, in three terminals (or tmux panes, below):

```bash
# 1. Sequencer: mempool HTTP on :8080, history HTTP on :8081
./target/debug/constantinople-sequencer run sequencer.yaml

# 2. Replica: syncs from :8081, serves explorer endpoints on :8082
./target/debug/constantinople-sequencer replica replica.yaml

# 3. Spammer: submits transfer batches and waits for finalization
./target/debug/constantinople-spammer --relayer-url http://127.0.0.1:8080
```

Expected steady state: the sequencer logs `built block height=N txs=10` every
~5–6s (one Celestia block time per batch), the replica logs `applied block`
with the **same state root**, and the spammer reports `finalized=... errors=0`
at roughly 2 TPS — each batch genuinely waits for Celestia inclusion.

## Endpoints

| Endpoint | Server | Purpose |
|---|---|---|
| `POST /transactions` | sequencer `:8080` | submit a batch, wait for DA finalization |
| `POST /transactions/ingest` | sequencer `:8080` | fast relayer ingestion |
| `GET /account/{public_key}` | both `:8080` / `:8082` | balance + nonce (explorer-compatible) |
| `GET /consensus/round` | both `:8080` / `:8082` | latest block height |
| `GET /archived-head` | sequencer `:8081` | highest batch archived locally |
| `GET /head`, `/cursor/{seq}`, `/payload/{seq}` | sequencer `:8081` | coro replica source |

The watch pane shows the demo headline — all three views in lockstep:

```text
sequencer round   {"round":14}
replica round     {"round":14}
DA archived head  {"head":14}
```

Point the explorer at the **replica** (`http://127.0.0.1:8082`) to show
account state served by a node that has no connection to the spammer or the
mempool — everything it knows arrived via the sequencer's history server (or
Celestia directly when the sequencer is down).

## Troubleshooting

- **`HeightMismatch` immediately after start** — stale storage from a
  previous run. Wipe `storage_dir` (both sides) and restart.
- **Replica state-root mismatch on block 1** — the replica's `genesis`
  section differs from the sequencer's. They must be identical.
- **`insufficient funds` from `blob.Submit`** — the Celestia node's account
  is out of mocha TIA; top up from the faucet. Fee per block ≈
  `gas × gas_price` (75 000 × 0.004 = 300 utia with the defaults).
- **Account endpoint returns 404** — the key was never funded at genesis and
  never received a transfer. Remember the demo has no default-account
  materialization (see the genesis rule in Configuration).
- **`GET :8081/head` hangs or times out** — expected while a blob submission
  is in flight: `/head` contends with the sequencer's writer lock, which is
  held for the duration of the (blocking) Celestia submit. Use
  `/archived-head` for quick polling; the replica's sync client just waits it
  out.
- **Spammer `errors > 0` at startup** — it raced the sequencer's listener;
  it recovers on its own once `:8080` is up.
