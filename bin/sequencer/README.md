# constantinople-sequencer

Runs the Coro/Celestia-backed Constantinople demo stack without the p2p
validator network.

## Sequencer

```bash
cargo run --bin constantinople-sequencer -- run sequencer.yaml
```

Minimal config:

```yaml
celestia:
  rpc_url: http://127.0.0.1:26658
  auth_token_env: CELESTIA_AUTH_TOKEN
  namespace: 0000008e5f679bf7116c

mempool_listen: 127.0.0.1:8080
history_listen: 127.0.0.1:8081

genesis:
  accounts: 10
  seed_offset: 1000
  balance: 1000
```

Point the spammer/explorer mempool URL at `mempool_listen`. Replicas point at
`history_listen`.

The Celestia auth token is read from the environment variable named by
`auth_token_env` (default `CELESTIA_AUTH_TOKEN`); never put the token itself
in the config file.

Unlike the validator engine, the demo executor does not materialize missing
accounts with a default balance: transfers whose sender *or recipient* is not
in the genesis allocation are silently dropped. The spammer derives keys from
`seed_offset`, using `accounts` keys per submitter, so size genesis to cover
every submitter: `genesis.accounts >= spammer accounts × relayer_submitters`
starting at the same `seed_offset`.

## Replica

```bash
cargo run --bin constantinople-sequencer -- replica replica.yaml
```

Minimal config:

```yaml
celestia:
  rpc_url: http://127.0.0.1:26658
  auth_token_env: CELESTIA_AUTH_TOKEN
  namespace: 0000008e5f679bf7116c

sequencer_url: http://127.0.0.1:8081
listen: 127.0.0.1:8082

genesis:
  accounts: 10
  seed_offset: 1000
  balance: 1000
```

The replica exposes read-only explorer-compatible endpoints:

- `GET /account/{transaction_public_key_hex}`
- `GET /consensus/round`
