# Teapot protocol

Single source of truth for every implementation (Rust agent, Flutter app).
Change this file first, then the code.

## Identity

A device is its iroh endpoint id: the public half of a 32-byte Ed25519 secret
key stored on the device (`secret.key`, mode 0600). Regenerating the key makes
a new device.

## Proxy: `teapot/proxy/0`

Device-to-device, over iroh. The client (A) keeps one long-lived connection to
the exit node (B). Every proxied TCP connection is one bi-directional stream.

```
A -> B   CONNECT <host>:<port>\n
B -> A   OK\n                      then raw bytes in both directions
     or  ERR <reason>\n            then B finishes the stream
```

- Header lines are UTF-8, `\n`-terminated, at most 1024 bytes.
- `<host>` is a DNS name, IPv4, or bracketed IPv6 (`[::1]`). B resolves it, so
  DNS happens at the exit node.
- B checks resolved IPs, not the name, against its LAN policy. Loopback,
  RFC 1918, link-local, CGNAT `100.64/10`, ULA `fc00::/7`, `fe80::/10`, and
  IPv4-mapped forms of those are refused unless `allow_lan` is on.
- B closes connections from ids outside its allowlist with application code
  `1`, reason `not allowed`.
- Either side ending its half of the stream ends that direction of the TCP
  connection.

## Local HTTP proxy (A)

What apps on A talk to. Not part of the wire protocol; listed so mobile behaves
the same.

- `CONNECT host:port HTTP/1.1` → `200 Connection Established`, then tunnel.
- `GET http://host[:port]/path` (absolute form) → rewritten to origin form,
  `Proxy-Connection`/`Connection` dropped, `Connection: close` added, one
  request per stream.
- Exit node refusal or connection failure → `502 Bad Gateway` with the reason
  as the body.

## Control

Not built yet. Planned: HTTPS + WebSocket to `teapot-control` (Cloudflare
Workers), device authenticated by signing a server nonce with its secret key,
API under `/v1/`.
