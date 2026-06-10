# DEVX-01: Plug Coro into Constantinople

**Status:** Implemented (library + tests + `bin/sequencer` CLI)
**Branch:** `feat/coro-engine`
**Crate:** [`crates/coro-engine`](../crates/coro-engine)
**Demo guide:** [coro-demo-guide.md](./coro-demo-guide.md)

Demo Celestia data availability (via [`coro`](../../coro)) as a quick
alternative to bootstrapping a p2p validator network. Per the issue notes,
this stays deliberately small:

> All this demo should be is, a crate that implements the consensus trait
> that posts blobs to DA, existing network. We don't need to over complicate
> this.

## TL;DR

Add one new crate, `crates/coro-engine` (`constantinople-coro-engine`), that is a
drop-in alternative to `constantinople-engine`. Instead of threshold-simplex +
marshal + erasure-coded shards + seven p2p channels, it runs:

- **one sequencer node** that pulls transactions from the existing mempool
  webserver, executes them with the existing `constantinople-application`
  executor, and publishes encoded `SealedBlock`s to Celestia through coro's
  `SingleSequencer`, and
- **N replica nodes** that need *zero p2p bootstrapping*: they replay
  canonical history through coro's `Replica`, fetching cursors from the
  sequencer's HTTP control plane (`coro-demo`) and falling back to Celestia
  exact-ref blob reads when the sequencer is unreachable.

The headline for the demo: a replica syncs the chain with nothing but a
Celestia light-node endpoint and one HTTP URL — no peer lists, no
bootstrappers, no DKG output, no discovery network.

## What exists today

`constantinople-engine` assembles (see `crates/engine/src/engine.rs`):

| Component | Purpose | Needed for DA demo? |
|---|---|---|
| simplex (threshold BLS) | BFT ordering | no — sequencer orders |
| marshal + coded shards | finalized block availability | no — Celestia is availability |
| 8 p2p channels + discovery | votes, certs, backfill, state sync | no |
| QMDB state / transaction dbs | execution state | **yes** |
| `constantinople-application` executor | account transitions | **yes** |
| mempool webserver | tx ingress + account reads | **yes** (sequencer only) |

`coro` provides the inverse half:

| coro layer | Provides |
|---|---|
| `Publisher` / `Reader` | opaque blob publish + verified exact-ref reads |
| `single_sequencer::SingleSequencer` | durable ingress, deterministic batching, local archive, DA publication, `BatchNumber -> BlobRef` mapping, restart recovery |
| `single_sequencer::Replica` | canonical replay from a `ReplicaSource`, DA fallback, hash verification, apply-progress persistence |
| `coro-demo` | HTTP `ReplicaSource` (`/head`, `/cursor/:seq`, `/payload/:seq`, ...) |

The integration is two trait impls and a wiring binary mode.

## Design

### Block format

Reuse `constantinople_primitives::Block` as the batch payload —
one DA blob per block, `payload = Block::encode()`. The simplex-specific
header fields (`round`, `leader`, parent view) are synthesized by the
sequencer (`leader` = sequencer key, `round` = height) so the explorer,
indexer, and codec keep working unchanged. Batch metadata (coro's
`Application::Metadata`) carries `{ height, state_root, tx_count }` for
cheap status/inspection without decoding payloads.

### Sequencer node

```text
client -> mempool webserver (existing, unchanged)
            |
            v  TransactionSource::propose()
   coro-engine sequencer loop (new, ~1 actor)
            |  executor::propose / execute  -> QMDB commit
            v
   coro SingleSequencer.submit + flush  -> local archive -> Celestia blob
            |
            v
   coro-demo HTTP control plane (/head, /cursor, /payload)
```

The sequencer loop implements coro's `single_sequencer::Application`:

```rust
#[async_trait]
impl coro::single_sequencer::Application for BlockBuilder<..> {
    type Tx = SignedTransaction<Sha256>;
    type Metadata = BlockMeta; // height, state_root, tx_count
    type Error = EngineError;

    async fn execute_batch(
        &mut self,
        sequence: BatchNumber,
        txs: Vec<Self::Tx>,
    ) -> Result<ExecutedBatch<BlockMeta>, EngineError> {
        // 1. verify + execute against QMDB (reuses application executor)
        // 2. build Header { height: sequence.0 + 1, parent: prev digest, .. }
        // 3. seal block, commit dbs
        Ok(ExecutedBatch { payload: sealed.encode().into(), metadata })
    }
}
```

Soft confirmations come for free: coro's `Archived` status is the local
soft-confirm stage, `Published(BatchCursor)` is replica-visible canonical
history. The mempool's finalized reporting hook is invoked on `Published`.

### Replica node

No p2p stack at all. The replica implements coro's `ReplicaApplication`:

```rust
#[async_trait]
impl coro::single_sequencer::ReplicaApplication for BlockApplier<..> {
    async fn apply_batch(&mut self, batch: ReplicaBatch) -> Result<...> {
        let block = Block::decode(batch.payload)?; // payload hash already verified
        // verify txs + parent linkage, execute against local QMDB, commit
    }
}
```

Source order of preference (already coro semantics, nothing to build):

1. sequencer HTTP `/payload/:seq` (fast path)
2. Celestia exact-ref read via `Reader` (sequencer down / payload pruned)

Replicas trust sequencer ordering (single-sequencer trust model) but verify
payload hashes against canonical cursors and re-execute every transaction, so
state roots are still checked locally.

### Binary wiring

Smallest possible blast radius: keep `bin/validator` untouched and add
`bin/sequencer` with two subcommands —

- `constantinople-sequencer run sequencer.yaml` — mempool webserver +
  coro-engine sequencer + coro-demo control plane.
- `constantinople-sequencer replica replica.yaml` — coro-engine replica
  syncing from the sequencer's coro-demo URL, with explorer-compatible
  read-only mempool endpoints (`/account/:public_key`,
  `/consensus/round`).

Config needs only: signer key, Celestia RPC/gRPC endpoints + signer env,
namespace id, storage partition, and (replicas) the sequencer URL. Notably
absent: peer lists, bootstrappers, DKG output, threshold shares.

## Dependency plan

`coro` depends on crates.io `commonware-* = "2026.5.0"`; constantinople pins
the monorepo git rev `156ceea8`, which *is* version `2026.5.0`. Cargo treats
those as distinct sources, so without alignment we would get two
`commonware-runtime`s and incompatible `Clock`/`Storage` contexts. Fix is one
of:

1. **(preferred)** add coro as a git dependency and add a `[patch.crates-io]`
   section in constantinople's workspace mapping `commonware-*` to the same
   git rev — versions match, so the patch is honest; or
2. switch coro itself to the git rev (requires a coro PR).

`celestia-client`/`celestia-grpc` come along transitively; only the new
`coro-engine` crate and `bin/sequencer` depend on coro, so the existing
validator path is unaffected.

## Milestones (few-day scope)

1. **Day 1:** workspace plumbing (coro dep + patch), `coro-engine` crate
   skeleton, `BlockBuilder: coro::Application` over a `MockBackend`,
   deterministic-runtime test producing a 3-block chain.
2. **Day 2:** replica side (`BlockApplier: ReplicaApplication`), coro-demo
   control plane wiring, end-to-end test: sequencer + 2 replicas over
   `MockBackend`, replicas converge on the same state root; kill the HTTP
   source mid-replay and confirm DA fallback.
3. **Day 3:** `bin/sequencer` CLI + YAML config, run against a live Celestia
   endpoint (mocha testnet), point the existing explorer/spammer at it,
   record the demo. Write the dev-led blog/tweet draft.

## Out of scope (explicitly)

- Celestia-as-consensus, fork choice from namespace scans
- multi-sequencer / rotation / based sequencing
- state sync for replicas (they replay from genesis; chains are short demos)
- indexer/exoware upload path in DA mode
- proof-required verification (`VerificationMode::RpcIncluded` is fine for
  the demo; flag `ProofRequired` as a follow-up)

## Open questions

1. Reuse `Block` with synthesized simplex context (implemented) vs. a new
   minimal DA header type? Reuse keeps explorer/spammer compatibility and is
   the "switch out the backend" story from the issue; a new header is cleaner
   but more code.
2. Genesis: reuse `genesis_block_with_parent` with the sequencer as
   `genesis_leader` (proposed) or a dedicated DA genesis?
