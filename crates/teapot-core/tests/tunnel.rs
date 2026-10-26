//! Exit node and local proxy wired together in one process, over iroh with
//! no relay and no DNS: addresses come from an in-memory lookup.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr,
    address_lookup::MemoryLookup, endpoint::presets, protocol::Router,
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
    proxy_with(client_ep, exit, PathBook::default()).await
}

async fn proxy_with(
    client_ep: Endpoint,
    exit: EndpointId,
    paths: PathBook,
) -> (String, Arc<ProxyClient>) {
    let client = Arc::new(ProxyClient::new(client_ep, Some(exit), paths));
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

fn peer(name: &str, id: EndpointId) -> teapot_core::control::Peer {
    teapot_core::control::Peer {
        node_id: id.to_string(),
        name: Some(name.into()),
        platform: None,
        online: true,
        exit: true,
        public_ipv4: None,
    }
}

#[tokio::test]
async fn proxy_username_picks_the_exit() {
    let lookup = MemoryLookup::new();
    let client_ep = endpoint(&lookup).await;
    let (one, _r1) = exit_node(&lookup, Allow::Any, true).await;
    let (two, _r2) = exit_node(&lookup, Allow::Any, true).await;
    let paths = PathBook::default();
    let (proxy_addr, client) = proxy_with(client_ep, one, paths.clone()).await;
    client.set_peers(vec![peer("one", one), peer("two", two)]);
    let target = target().await;

    // base64("two:") — what `http://two@127.0.0.1:1080` makes clients send.
    let res = send(
        &proxy_addr,
        &format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\nProxy-Authorization: Basic dHdvOg==\r\n\r\n"),
    )
    .await;
    assert!(res.ends_with("hello"), "{res}");
    let used: Vec<String> = paths.traffic().into_iter().map(|t| t.peer).collect();
    assert_eq!(
        used,
        [two.to_string()],
        "request went through the named exit"
    );

    // No username: the default exit.
    assert!(
        send(
            &proxy_addr,
            &format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\n\r\n")
        )
        .await
        .ends_with("hello")
    );
    assert_eq!(paths.traffic().len(), 2);

    // Unknown name: 502 with the reason, never a silent fallback.
    let res = send(
        &proxy_addr,
        &format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\nProxy-Authorization: Basic bm9wZTo=\r\n\r\n"),
    )
    .await;
    assert!(
        res.starts_with("HTTP/1.1 502") && res.contains("no exit node matches"),
        "{res}"
    );

    let listing = send(
        &proxy_addr,
        "GET /_teapot/exits HTTP/1.1\r\nHost: x\r\n\r\n",
    )
    .await;
    let body = listing.split("\r\n\r\n").nth(1).unwrap();
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    let names: Vec<&str> = json["exits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["one", "two"]);
    assert_eq!(json["exits"][0]["default"], true);
}

/// Exit node on 127.0.0.1 only, with a fixed key, so it can be restarted as
/// "the same device" the way the phone app restarts.
async fn exit_node_at(lookup: &MemoryLookup, key: SecretKey) -> Router {
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .relay_mode(RelayMode::Disabled)
        .address_lookup(lookup.clone())
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    Router::builder(ep)
        .accept(
            ALPN_PROXY,
            ExitNode::new(
                ExitPolicy {
                    allow: Allow::Any,
                    allow_lan: true,
                },
                PathBook::default(),
            ),
        )
        .spawn()
}

/// UDP relay in front of `to` that can be cut, so a connection through it
/// goes silent instead of closing: what a client sees when the exit's
/// process dies or its phone changes network.
async fn cuttable_relay(to: std::net::SocketAddr) -> (std::net::SocketAddr, Arc<AtomicBool>) {
    let sock = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = sock.local_addr().unwrap();
    let cut = Arc::new(AtomicBool::new(false));
    let flag = cut.clone();
    tokio::spawn(async move {
        let mut client = None;
        let mut buf = vec![0u8; 65536];
        while let Ok((n, from)) = sock.recv_from(&mut buf).await {
            if flag.load(Ordering::SeqCst) {
                continue;
            }
            let dest = if from == to {
                client
            } else {
                client = Some(from);
                Some(to)
            };
            if let Some(dest) = dest {
                let _ = sock.send_to(&buf[..n], dest).await;
            }
        }
    });
    (addr, cut)
}

#[tokio::test]
async fn restarted_exit_is_redialed_without_failing_the_request() {
    let lookup = MemoryLookup::new();
    let key = SecretKey::generate();
    let exit = key.public();
    let first = exit_node_at(&lookup, key.clone()).await;
    let (relay, cut) = cuttable_relay(first.endpoint().bound_sockets()[0]).await;
    lookup.set_endpoint_info(EndpointAddr::from_parts(exit, [TransportAddr::Ip(relay)]));
    let (proxy_addr, _client) = proxy(endpoint(&lookup).await, exit).await;
    let target = target().await;
    let get = format!("GET http://{target}/ HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(send(&proxy_addr, &get).await.ends_with("hello"));

    // The old process dies without closing; a new one with the same key comes
    // up elsewhere (dropping a Router aborts its endpoint ungracefully).
    cut.store(true, Ordering::SeqCst);
    drop(first);
    let second = exit_node_at(&lookup, key).await;
    lookup.set_endpoint_info(second.endpoint().addr());

    let started = std::time::Instant::now();
    let res = send(&proxy_addr, &get).await;
    assert!(res.ends_with("hello"), "{res}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
}
