//! [`coro::backend::Backend`] over the celestia-node JSON-RPC API.
//!
//! coro's bundled `CelestiaClientBackend` signs PayForBlobs transactions
//! locally and needs gRPC access plus a funded private key. Hosted endpoints
//! (e.g. QuickNode) instead expose the celestia-node JSON-RPC API
//! (`blob.Submit` / `blob.Get`) with bearer-token auth and sign with the
//! node-side keyring. This backend targets that API so a demo needs nothing
//! but a URL and a token.
//!
//! Semantics differ from the two-phase broadcast/confirm gRPC flow:
//! `blob.Submit` blocks until inclusion and returns the inclusion height, so
//! `broadcast` completes the whole submission and `confirm` just reports the
//! stashed result. Restart recovery of in-flight submissions is therefore
//! weaker than the gRPC backend (an interrupted submit is re-submitted by
//! coro's publisher retry, deduplicated by Celestia's tx cache).
//!
//! `VerificationMode::ProofRequired` is not supported; reads validate that
//! returned bytes match the requested blob commitment (`RpcIncluded`).

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use celestia_client::tx::TxConfig;
use celestia_client::types::{Blob as CelestiaBlob, nmt::Namespace};
use commonware_cryptography::{Hasher as _, sha256::Sha256};
use coro::{
    BlobCommitment, BlobRef, Error, NamespaceId, Verification, VerificationMode,
    backend::{Backend, BroadcastedSubmission, SubmittedBlob},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tracing::debug;

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Gas options applied to every `blob.Submit`.
#[derive(Clone, Copy, Debug, Default)]
pub struct GasConfig {
    /// Explicit gas limit. `None` lets the node estimate.
    pub gas: Option<u64>,
    /// Explicit gas price in utia/gas. `None` lets the node estimate.
    pub gas_price: Option<f64>,
}

/// celestia-node JSON-RPC backend with bearer-token auth.
#[derive(Clone)]
pub struct NodeRpcBackend {
    client: reqwest::Client,
    url: String,
    auth_token: Option<String>,
    gas: GasConfig,
    /// Completed submissions keyed by the pseudo tx hash handed to coro.
    submitted: Arc<Mutex<HashMap<[u8; 32], u64>>>,
}

impl NodeRpcBackend {
    /// Creates a backend for a celestia-node JSON-RPC endpoint.
    ///
    /// The configured `gas` options are used for every submission; the
    /// per-request [`TxConfig`] coro passes through is ignored because its
    /// gRPC-oriented fields do not map onto the JSON-RPC `TxConfig` object.
    pub fn new(url: impl Into<String>, auth_token: Option<String>, gas: GasConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            auth_token,
            gas,
            submitted: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let mut request = self.client.post(&self.url).json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }));
        if let Some(token) = &self.auth_token {
            request = request.bearer_auth(token);
        }
        let response: Value = request
            .send()
            .await
            .map_err(|err| format!("{method} request failed: {err}"))?
            .json()
            .await
            .map_err(|err| format!("{method} response decode failed: {err}"))?;
        if let Some(error) = response.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            return Err(format!("{method} error: {message}"));
        }
        response
            .get("result")
            .cloned()
            .ok_or_else(|| format!("{method} response missing result"))
    }

    fn tx_options(&self) -> Value {
        let mut options = serde_json::Map::new();
        if let Some(gas) = self.gas.gas {
            options.insert("gas".into(), json!(gas));
        }
        if let Some(gas_price) = self.gas.gas_price {
            options.insert("gas_price".into(), json!(gas_price));
            options.insert("is_gas_price_set".into(), json!(true));
        }
        Value::Object(options)
    }
}

/// Computes the share commitment for a blob locally.
fn compute_commitment(namespace: NamespaceId, payload: &[u8]) -> Result<BlobCommitment, Error> {
    let namespace = Namespace::from_raw(&namespace.0).map_err(|_| Error::InvalidNamespace)?;
    let blob = CelestiaBlob::new(namespace, payload.to_vec(), None)
        .map_err(|err| Error::Decode(err.to_string()))?;
    Ok(BlobCommitment(*blob.commitment.hash()))
}

/// Derives the pseudo tx hash coro tracks a submission under.
///
/// `blob.Submit` does not expose the underlying PayForBlobs tx hash, so the
/// backend keys completed submissions by a hash of the inclusion result.
fn pseudo_tx_hash(height: u64, commitment: &BlobCommitment) -> [u8; 32] {
    let mut hasher = Sha256::default();
    hasher.update(b"coro-engine.node-rpc.v1");
    hasher.update(&height.to_be_bytes());
    hasher.update(&commitment.0);
    hasher
        .finalize()
        .as_ref()
        .try_into()
        .expect("sha256 is 32 bytes")
}

#[async_trait]
impl Backend for NodeRpcBackend {
    async fn broadcast(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        _tx_config: TxConfig,
    ) -> Result<BroadcastedSubmission, Error> {
        let commitment = compute_commitment(namespace, payload.as_ref())?;
        let params = json!([
            [{
                "namespace": BASE64.encode(namespace.0),
                "data": BASE64.encode(&payload),
                "share_version": 0,
            }],
            self.tx_options(),
        ]);
        let height = self
            .call("blob.Submit", params)
            .await
            .map_err(Error::CelestiaSubmit)?
            .as_u64()
            .ok_or_else(|| Error::CelestiaSubmit("blob.Submit returned non-u64 height".into()))?;
        debug!(height, "blob submitted via node RPC");

        let tx_hash = pseudo_tx_hash(height, &commitment);
        self.submitted
            .lock()
            .expect("submission map lock poisoned")
            .insert(tx_hash, height);
        Ok(BroadcastedSubmission {
            tx_hash,
            tx_bytes: Bytes::new(),
        })
    }

    async fn confirm(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        _tx_config: TxConfig,
        submission: &BroadcastedSubmission,
    ) -> Result<Option<SubmittedBlob>, Error> {
        let height = self
            .submitted
            .lock()
            .expect("submission map lock poisoned")
            .get(&submission.tx_hash)
            .copied()
            .ok_or_else(|| {
                Error::CelestiaSubmit("unknown submission; re-broadcast required".into())
            })?;
        Ok(Some(SubmittedBlob {
            blob_ref: BlobRef {
                height,
                namespace,
                commitment: compute_commitment(namespace, payload.as_ref())?,
            },
            tx_hash: submission.tx_hash,
        }))
    }

    async fn get(&self, blob_ref: BlobRef) -> Result<Bytes, Error> {
        let params = json!([
            blob_ref.height,
            BASE64.encode(blob_ref.namespace.0),
            BASE64.encode(blob_ref.commitment.0),
        ]);
        let result = self.call("blob.Get", params).await.map_err(|err| {
            if err.contains("not found") {
                Error::NotFound
            } else {
                Error::CelestiaRead(err)
            }
        })?;
        let data = result
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::CelestiaRead("blob.Get response missing data".into()))?;
        let payload = BASE64
            .decode(data)
            .map_err(|err| Error::CelestiaRead(format!("blob data decode failed: {err}")))?;
        if compute_commitment(blob_ref.namespace, &payload)? != blob_ref.commitment {
            return Err(Error::CommitmentMismatch);
        }
        Ok(Bytes::from(payload))
    }

    async fn verify(
        &self,
        _blob_ref: BlobRef,
        mode: VerificationMode,
    ) -> Result<Verification, Error> {
        match mode {
            VerificationMode::RpcIncluded => Ok(Verification::RpcIncluded),
            VerificationMode::ProofRequired => Err(Error::UnsupportedVerificationMode),
        }
    }
}
