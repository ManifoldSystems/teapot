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

HTTPS + WebSocket to `teapot-control` (Cloudflare Workers). Responses are
`{success, data, ...}`; errors `{success: false, message, error}`. Clients
send their own `User-Agent` (`teapot/<version>`): Cloudflare rejects library
defaults like `Python-urllib` with `403` / error `1010`.

`POST /login` is limited to 10 requests a minute per IP and `/v1/auth/*` to
60; over the limit answers `429`.

### Auth

```
POST /v1/auth/challenge {node_id}
  → data: {challenge, expires_at}
POST /v1/auth/verify {node_id, challenge, signature}
  → data: {token, expires_at, status: "pending" | "approved"}
```

- `node_id`: 64 lowercase hex chars (the endpoint id).
- `challenge`: opaque string, valid 60 s, bound to `node_id`.
- `signature`: hex of the 64-byte Ed25519 signature over the UTF-8 bytes of
  `challenge`, made with the device secret key.
- `token`: Bearer for everything below, valid 24 h. First login creates the
  device as `pending`; the owner approves it in the dashboard.

### Device API

| Method | Path | Body / result |
|---|---|---|
| GET | `/v1/devices/me` | device row |
| PUT | `/v1/devices/me` | `{name?, platform?, can_exit?}`; omitted fields are kept. `can_exit` = the device serves exit right now; the owner's exit toggle is stored separately and survives restarts |
| GET | `/v1/peers` | `[{node_id, name, platform, online, exit, public_ipv4}]`; `403 PENDING` until approved. Clients skip exits whose `public_ipv4` equals their own |
| GET | `/v1/exit/allowlist` | `{exit_enabled, allow: [node_id]}`; `403 PENDING` until approved |
| GET | `/v1/presence` | WebSocket upgrade; token as Bearer or `?token=` |

`401` means authenticate again (expired token, or the device was removed).

### Presence socket

Online = the socket is open. One socket per device.

```
client → "ping"                        server → "pong"
client → {"type":"status",
          "paths":[{"peer":<node_id>,"kind":"direct"|"relay","rtt_ms":n}],
          "traffic":[{"peer":<node_id>,"tx":bytes,"rx":bytes}],
          "ips":{"v4":"a.b.c.d"?,"v6":"x:y::z"?}}
         traffic = tunnel payload since the agent started, from the sender's
         side (tx went to the peer, rx came from it)
         ips = this device's public addresses from its own iroh endpoint
         addresses (LAN/loopback/CGNAT/ULA dropped); control only sees the
         one family the socket uses
server → {"type":"peers_changed"}      approval, exit, or online state changed:
                                       refetch /v1/peers and /v1/exit/allowlist
close 4000  replaced by a newer connection of the same device
close 4001  removed or revoked from the dashboard
```
