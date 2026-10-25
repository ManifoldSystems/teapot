use std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use iroh::{EndpointId, protocol::Router};
use teapot_core::{
    exit::{Allow, ExitNode, ExitPolicy},
    key,
    proxy::{self, ProxyClient},
};
use teapot_proto::ALPN_PROXY;
use tracing::info;

#[derive(Parser)]
#[command(version, about = "Personal mesh on iroh")]
struct Cli {
    /// Secret key file; the endpoint id is derived from it.
    #[arg(long, global = true, env = "TEAPOT_KEY")]
    key: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print this device's endpoint id.
    Id,
    /// Act as an exit node: dial the internet for allowed clients.
    Exit {
        /// Endpoint id allowed to use this exit node. Repeatable.
        #[arg(long = "allow", value_name = "ENDPOINT_ID")]
        allow: Vec<EndpointId>,
        /// Allow every endpoint. Only for local experiments.
        #[arg(long, conflicts_with = "allow")]
        allow_any: bool,
        /// Let clients reach this machine's LAN and loopback.
        #[arg(long)]
        allow_lan: bool,
    },
    /// Run a local HTTP proxy that exits through another device.
    Proxy {
        /// Endpoint id of the exit node.
        exit: EndpointId,
        #[arg(long, default_value = "127.0.0.1:1080")]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "teapot=info,teapot_core=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let key_path = match cli.key {
        Some(path) => path,
        None => dirs::config_dir()
            .context("no config directory on this platform; pass --key")?
            .join("teapot")
            .join("secret.key"),
    };
    let secret_key = key::load_or_create(&key_path)?;

    match cli.cmd {
        Cmd::Id => println!("{}", secret_key.public()),
        Cmd::Exit {
            allow,
            allow_any,
            allow_lan,
        } => {
            let allow = if allow_any {
                Allow::Any
            } else if allow.is_empty() {
                bail!("pass --allow <ENDPOINT_ID> (see `teapot id` on the client) or --allow-any");
            } else {
                Allow::Only(allow.into_iter().collect::<HashSet<_>>())
            };
            let endpoint = teapot_core::bind(secret_key).await?;
            let router = Router::builder(endpoint)
                .accept(ALPN_PROXY, ExitNode::new(ExitPolicy { allow, allow_lan }))
                .spawn();
            router.endpoint().online().await;
            info!(id = %router.endpoint().id(), allow_lan, "exit node ready");
            println!(
                "\nOn the client run:\n  teapot proxy {}\n",
                router.endpoint().id()
            );
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        Cmd::Proxy { exit, listen } => {
            let endpoint = teapot_core::bind(secret_key).await?;
            info!(id = %endpoint.id(), "client ready");
            let client = Arc::new(ProxyClient::new(endpoint.clone(), exit));
            tokio::select! {
                res = proxy::serve(listen, client) => res?,
                _ = tokio::signal::ctrl_c() => {}
            }
            endpoint.close().await;
        }
    }
    Ok(())
}
