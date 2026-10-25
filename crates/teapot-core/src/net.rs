//! Outbound dialing from the exit node, with the LAN guard.

use std::net::{IpAddr, SocketAddr};

use anyhow::bail;
use tokio::net::TcpStream;

/// Addresses that belong to the exit node's own network rather than the
/// internet: loopback, RFC 1918, link-local, CGNAT, ULA, and friends.
pub fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || (a == 100 && (b & 0xc0) == 64) // 100.64.0.0/10
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_local(IpAddr::V4(v4));
            }
            let first = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (first & 0xfe00) == 0xfc00 // fc00::/7
                || (first & 0xffc0) == 0xfe80 // fe80::/10
        }
    }
}

/// Resolves `target` on this machine and connects to the first address the
/// policy allows. The check runs on resolved IPs, not the hostname, because
/// any public name can point at 192.168.x.x.
pub async fn dial(target: &str, allow_lan: bool) -> anyhow::Result<TcpStream> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(target).await?.collect();
    if addrs.is_empty() {
        bail!("{target} did not resolve");
    }
    let allowed: Vec<SocketAddr> = addrs
        .into_iter()
        .filter(|a| allow_lan || !is_local(a.ip()))
        .collect();
    if allowed.is_empty() {
        bail!("{target} is a local address and allow_lan is off");
    }

    let mut last_err = None;
    for addr in allowed {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.expect("at least one address was tried").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn local_addresses() {
        for s in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:192.168.1.1",
        ] {
            assert!(is_local(ip(s)), "{s} should be local");
        }
    }

    #[test]
    fn public_addresses() {
        for s in ["1.1.1.1", "8.8.8.8", "100.128.0.1", "2606:4700:4700::1111"] {
            assert!(!is_local(ip(s)), "{s} should be public");
        }
    }

    #[tokio::test]
    async fn dial_blocks_localhost_by_default() {
        let err = dial("localhost:9", false).await.unwrap_err();
        assert!(err.to_string().contains("allow_lan"), "{err}");
    }
}
