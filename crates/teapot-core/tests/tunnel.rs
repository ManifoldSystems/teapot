//! Exit node and local proxy wired together in one process, over iroh with
//! no relay and no DNS: addresses come from an in-memory lookup.

use std::{collections::HashSet, sync::Arc, time::Duration};

use iroh::{
    Endpoint, EndpointId, RelayMode, SecretKey, address_lookup::MemoryLookup, endpoint::presets,
    protocol::Router,
};
use teapot_core::{
    exit::{Allow, ExitNode, ExitPolicy},
    paths::PathBook,
    proxy::{self, ProxyClient},
};
use teapot_proto::ALPN_PROXY;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const TIMEOUT: Duration = Duration::from_secs(20);

async fn endpoint(lookup: &MemoryLookup) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .relay_mode(RelayMode::Disabled)
        .address_lookup(lookup.clone())
        .bind()
        .await
        .unwrap()
}

/// Exit node on its own endpoint; returns its id and the router keeping it up.
async fn exit_node(lookup: &MemoryLookup, allow: Allow, allow_lan: bool) -> (EndpointId, Router) {
    let ep = endpoint(lookup).await;
    let router = Router::builder(ep)
        .accept(
            ALPN_PROXY,
            ExitNode::new(ExitPolicy { allow, allow_lan }, PathBook::default()),
        )
        .spawn();
    lookup.add_endpoint_info(router.endpoint().addr());
    (router.endpoint().id(), router)
}

/// Local proxy pointed at `exit`; returns its address and the client.
async fn proxy(client_ep: Endpoint, exit: EndpointId) -> (String, Arc<ProxyClient>) {
    let client = Arc::new(ProxyClient::new(client_ep, Some(exit), PathBook::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(proxy::serve_listener(listener, client.clone()));
    (addr, client)
}

/// Answers every connection with a fixed HTTP response, or echoes bytes back
/// when the first line is not HTTP.
async fn target() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                if buf[..n].starts_with(b"GET ") {
                    let _ = sock
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")
                        .await;
                } else {
                    let _ = sock.write_all(&buf[..n]).await;
                }
            });
        }
    });
    addr
}

async fn send(proxy: &str, request: &str) -> String {
    tokio::time::timeout(TIMEOUT, async {
        let mut sock = TcpStream::connect(proxy).await.unwrap();
        sock.write_all(request.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        sock.read_to_end(&mut out).await.unwrap();
        String::from_utf8_lossy(&out).into_owned()
    })
    .await
    .expect("proxy answered in time")
}

#[tokio::test]
async fn tunnels_http_and_connect() {
    let lookup = MemoryLookup::new();
    let client_ep = endpoint(&lookup).await;
    let allow = Allow::Only(HashSet::from([client_ep.id()]));
    let (exit, _router) = exit_node(&lookup, allow, true).await;
    let (proxy_addr, _) = proxy(client_ep, exit).await;
    let target = target().await;

    let res = send(
        &proxy_addr,
        &format!("GET http://{target}/ HTTP/1.1\r\nHost: {target}\r\n\r\n"),
    )
    .await;
    assert!(
        res.starts_with("HTTP/1.1 200 OK") && res.ends_with("hello"),
        "{res}"
    );

    let mut sock = TcpStream::connect(&proxy_addr).await.unwrap();
    sock.write_all(format!("CONNECT {target} HTTP/1.1\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut head = [0u8; 39];
    tokio::time::timeout(TIMEOUT, sock.read_exact(&mut head))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&head, b"HTTP/1.1 200 Connection Established\r\n\r\n");
    sock.write_all(b"ping through the tunnel").await.unwrap();
    let mut echo = [0u8; 23];
    tokio::time::timeout(TIMEOUT, sock.read_exact(&mut echo))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echo, b"ping through the tunnel");
}

#[tokio::test]
async fn lan_is_blocked_by_default() {
    let lookup = MemoryLookup::new();
    let client_ep = endpoint(&lookup).await;
    let (exit, _router) = exit_node(&lookup, Allow::Any, false).await;
    let (proxy_addr, _) = proxy(client_ep, exit).await;
    let target = target().await;

    let res = send(
        &proxy_addr,
        &format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\n\r\n"),
    )
    .await;
    assert!(
        res.starts_with("HTTP/1.1 502") && res.contains("allow_lan is off"),
        "{res}"
    );
}

#[tokio::test]
async fn unlisted_client_is_refused() {
    let lookup = MemoryLookup::new();
    let client_ep = endpoint(&lookup).await;
    let someone_else = SecretKey::generate().public();
    let (exit, _router) =
        exit_node(&lookup, Allow::Only(HashSet::from([someone_else])), true).await;
    let (proxy_addr, _) = proxy(client_ep, exit).await;
    let target = target().await;

    let res = send(
        &proxy_addr,
        &format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\n\r\n"),
    )
    .await;
    assert!(
        res.starts_with("HTTP/1.1 502") && res.contains("not allowed"),
        "{res}"
    );
}

#[tokio::test]
async fn switches_exit_and_reports_none() {
    let lookup = MemoryLookup::new();
    let client_ep = endpoint(&lookup).await;
    let (first, _r1) = exit_node(&lookup, Allow::Any, true).await;
    let (second, _r2) = exit_node(&lookup, Allow::Any, true).await;
    let (proxy_addr, client) = proxy(client_ep, first).await;
    let target = target().await;
    let get = format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\n\r\n");

    assert!(send(&proxy_addr, &get).await.ends_with("hello"));

    assert!(client.set_exit(Some(second)).await);
    assert_eq!(client.connection().await.unwrap().remote_id(), second);
    assert!(send(&proxy_addr, &get).await.ends_with("hello"));

    assert!(client.set_exit(None).await);
    let res = send(&proxy_addr, &get).await;
    assert!(
        res.starts_with("HTTP/1.1 502") && res.contains("no exit node online"),
        "{res}"
    );
}
