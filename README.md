# Teapot

Personal mesh on [iroh](https://iroh.computer). First feature: **exit node** —
send traffic from device A and have the internet see device B's IP.

```
app on A ──► teapot proxy (localhost:1080) ══ iroh ══► teapot exit on B ──► internet
                                       direct, or n0 relay when hole punching fails
```

Status: stage 1, peer to peer only. Devices find each other by endpoint id
through iroh's DNS lookup. There is no control plane yet; the allowlist is a
command-line flag.

## Build

```bash
cargo build --release
```

Binary: `target/release/teapot`.

## Use

On the client (A), get its id:

```bash
teapot id
```

On the exit node (B), allow that id:

```bash
teapot exit --allow <A_ID>
```

B prints its own id. On A:

```bash
teapot proxy <B_ID>
```

Then point apps at `http://127.0.0.1:1080`:

```bash
curl -x http://127.0.0.1:1080 https://ifconfig.me
```

It prints B's public IP. A and B on the same network share one IP, so test
across networks (laptop on Wi-Fi, B on a phone hotspot or a VPS).

## Options

| Flag | Where | Meaning |
|---|---|---|
| `--key <path>` / `TEAPOT_KEY` | all | Secret key file. Default: `<config dir>/teapot/secret.key` |
| `--allow <id>` | exit | Endpoint id allowed to use this exit node; repeatable |
| `--allow-any` | exit | Any endpoint may use it. Local experiments only |
| `--allow-lan` | exit | Let clients reach B's loopback and LAN. Off by default |
| `--listen <addr>` | proxy | Local proxy address. Default `127.0.0.1:1080` |
| `RUST_LOG` | all | e.g. `teapot_core=debug` for per-stream logs |

The proxy logs which path it uses (`path: relay ...` or `path: direct ...`)
whenever it changes.

## Layout

```
crates/teapot-proto   wire format, ALPN; see docs/protocol.md
crates/teapot-core    exit node, local proxy, key storage, LAN guard
crates/teapot         CLI
```

## Not yet

- Control plane (`teapot-control`): registry, presence, allowlist from a dashboard
- SOCKS5
- Running as a service (systemd / launchd)
