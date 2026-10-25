//! Agent logic shared by the `teapot` binary and, later, the mobile app.

pub mod agent;
pub mod control;
pub mod exit;
pub mod key;
pub mod net;
pub mod paths;
pub mod proxy;

use iroh::{Endpoint, SecretKey, endpoint::presets};

/// Binds an endpoint with the n0 preset: n0 relays plus DNS address lookup,
/// so peers can be dialed by endpoint id alone.
pub async fn bind(secret_key: SecretKey) -> anyhow::Result<Endpoint> {
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await?;
    Ok(endpoint)
}
