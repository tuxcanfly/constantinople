# constantinople-coro-engine

A Celestia data-availability backend for Constantinople, built on
[`coro`](https://github.com/celestiaorg/coro)'s single-sequencer stack. It
replaces threshold-simplex consensus, the marshal, and all p2p channels with:

- a **sequencer** ([`BlockBuilder`]) that executes transactions with the
  existing `constantinople-application` executor and publishes encoded
  `Block`s to Celestia as blobs, and
- **replicas** ([`BlockApplier`]) that replay canonical history with zero p2p
  bootstrapping: cursors come from any [`coro::single_sequencer::ReplicaSource`]
  and payloads fall back to verified Celestia exact-ref reads.

[`NodeRpcBackend`] implements coro's `Backend` trait against a celestia-node
JSON-RPC endpoint (`blob.Submit` / `blob.Get` with bearer-token auth), so the
demo runs against hosted mocha endpoints without a local light node or signing
key.

## Trust model

Single-sequencer: the sequencer is trusted to order and pre-validate
transactions (signature verification happens in the mempool upstream).
Replicas re-execute every transfer and check the resulting state root against
the block header, and verify payload hashes against canonical cursors, so a
faulty sequencer cannot silently corrupt replica state.
