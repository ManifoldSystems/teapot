# Teapot

Personal mesh on [iroh](https://iroh.computer). First feature: **exit node** —
send traffic from device A and have the internet see device B's IP.

```
app on A ──► teapot proxy (localhost:1080) ══ iroh ══► teapot exit on B ──► internet
                                       direct, or n0 relay when hole punching fails

            both log in to teapot-control (Cloudflare Workers):
            approval, exit allowlist, presence, dashboard
```

Devices find each other by endpoint id through iroh's DNS lookup. Proxy traffic
goes device to device; [teapot-control](https://github.com/ManifoldSystems/teapot-control)
only decides who may talk to whom.

## Build

Agents from 0.2.0 report their application version and presence protocol on
connect. Reconnects use randomized exponential backoff and log the control
version/generation handshake. See `teapot-control/README.md` for generation-based
deployment and retirement recovery, including legacy Android clients.

```bash
cargo build --release
```

Binary: `target/release/teapot`.

## Use

Deploy [teapot-control](https://github.com/ManifoldSystems/teapot-control)
first, then point each device at your deployment:

```bash
export TEAPOT_CONTROL=https://teapot.example.com  # replace with your URL
teapot login              # opens the approval page; confirm "Add this device?"
teapot service install    # runs `teapot up` in the background, restarts it if it dies
```

`teapot up` keeps one endpoint online and does two jobs:

- **exit node** — serves other devices once you turn **exit** on for this
  device in the dashboard (`--no-exit` to never offer it);
- **local proxy** on `127.0.0.1:1080` — exits through another device: the one
  named with `--exit <name>` while it is online, otherwise any online exit
  node. When the current exit goes offline — or turns out to leave with this
  device's own public IPv4, e.g. a laptop whose hotspot was switched off and
  joined the same Wi-Fi — the proxy moves to another one within seconds and
  comes back to `--exit` when it is useful again (`--no-proxy` to skip the
  proxy).

Options given to `teapot service install` are passed to `up`, e.g.
`teapot service install --exit phone-exit`. The control URL is saved in the
installed service; keep `TEAPOT_CONTROL` set for interactive commands.
`teapot service status` and
`teapot service uninstall` do what they say. macOS uses a launchd agent
(runs while you are logged in); Linux uses a systemd user unit (add
`sudo loginctl enable-linger $USER` to run without a login session).

`teapot exit` and `teapot proxy` are `up --no-proxy` and `up --no-exit`.

**Whole-Mac exit (`--system-proxy`, macOS).** Points the system HTTP/HTTPS
proxy at `127.0.0.1:1080` while an exit node is available, so browsers and
most apps go out through it without per-app settings. With no exit node
online it switches the proxy back off, so the Mac never loses internet
because of teapot. The original settings are saved before anything changes
and restored on shutdown (Ctrl+C or the service stopping), on the next start
after a crash, by `teapot service uninstall`, or by hand with
`teapot system-proxy off`. Needs an admin account (`networksetup`).

**Exit nodes require current authorization from control.** A failed refresh
clears the allowlist and disconnects peers. Restarts begin with no authorized
peers; the snapshot next to the key is diagnostic only. Refreshes run every
30 seconds with a 20-second deadline, so control outages interrupt exit traffic.

Point apps at `http://127.0.0.1:1080`:

```bash
curl -x http://127.0.0.1:1080 https://ifconfig.me
```

It prints B's public IP. Name a different exit for a single request with the
proxy username, and list the choices from the proxy itself:

```bash
curl -x http://home-exit@127.0.0.1:1080 https://ifconfig.me     # this request via home-exit
curl http://127.0.0.1:1080/_teapot/exits                         # what can be named
```

The Python library [`teapot-py`](https://github.com/ManifoldSystems/teapot-py)
wraps both. A and B on the same network share one IP, so test
across networks (laptop on Wi-Fi, B on a phone hotspot or a VPS).

`teapot status` shows this device's approval, exit state, public IPv4/IPv6
(reported by the agent from its iroh addresses), and its peers.

Revoking or removing a device in the dashboard disconnects it from every exit
node within a second or two.

### Without control

```bash
teapot id                                  # on A
teapot exit --allow <A_ID>                 # on B, prints B's id
teapot proxy --no-control <B_ID>           # on A
```

## Options

| Flag | Where | Meaning |
|---|---|---|
| `--key <path>` / `TEAPOT_KEY` | all | Secret key file. Default: `<config dir>/teapot/secret.key` |
| `--control <url>` / `TEAPOT_CONTROL` | all | Your control plane. Default: `http://127.0.0.1:8787` (local development only) |
| `--name <name>` / `TEAPOT_NAME` | all | Name in the dashboard. Default: hostname |
| `--allow <id>` | exit | Static allowlist, skips control; repeatable |
| `--allow-any` | exit | Any endpoint, skips control. Local experiments only |
| `--allow-lan` | exit | Let clients reach B's loopback and LAN. Off by default |
| `--exit <name>` / `[EXIT]` | up / proxy | Preferred exit node by name, id, or id prefix (≥ 8 chars) |
| `--no-proxy` / `--no-exit` | up | Skip the local proxy / never offer exit |
| `--system-proxy` | up | macOS: route the whole Mac through the proxy while an exit is available |
| `--listen <addr>` | proxy | Local proxy address. Default `127.0.0.1:1080` |
| `--no-control` | proxy | Skip control; `EXIT` must be a full id |
| `--no-browser` | login | Print the approval link instead of opening it |
| `RUST_LOG` | all | e.g. `teapot_core=debug` for per-stream logs |

The proxy logs which path it uses (`path: relay ...` or `path: direct ...`)
whenever it changes. Every device reports its paths and tunnel bytes per peer
(since the agent started) to the dashboard.

## Logs

Every agent keeps local JSONL history beside its key (`secret.log.jsonl`, plus
one rotated file; roughly 2 MiB each) and batches diagnostic events to control
every five seconds. Terminal / launchd / systemd output remains available.
The disk writer and upload queue are bounded so logging cannot stall tunnels.

```bash
teapot logs                         # latest local events; works offline
teapot logs --follow                 # follow local history
teapot logs phone-exit --follow      # watch another approved device
teapot logs --all --follow           # all devices + control
teapot logs control --level warn     # control warnings/errors
teapot logs phone-exit --json        # JSONL for tooling
RUST_LOG=teapot=debug,teapot_core=debug teapot up  # include tunnel details
```

The dashboard's **Mesh logs** page (`/admin/logs`) has live device/level/search
filters, pause/resume, and copy. Device detail pages link to their own logs.
The Flutter app has the same local/remote viewer from Status and Mesh; tap a
peer to watch its logs. Viewing uses a separate connection and does not take
over the running agent's endpoint or presence socket.

Only approved devices and the dashboard admin can view mesh logs. Control
retains the latest 10,000 events globally, for at most seven days. Reconnect
resumes from a server cursor; upload retries are deduplicated. Local upload
queues keep up to 1,000 events in memory (not across process restarts); oldest
events may be dropped under load or prolonged outages. Logs are diagnostics,
not a lossless audit trail. See `docs/protocol.md` for the shared contract.

## Layout

```
crates/teapot-proto   wire format, ALPN; see docs/protocol.md
crates/teapot-core    exit node, local proxy, control client, exit selection, path book, key, LAN guard
                      tests/tunnel.rs: exit + proxy in one process, no relay or DNS
crates/teapot         CLI, service install (launchd / systemd)
```

## Not yet

- SOCKS5
- `--system-proxy` tried on a real Mac (written and unit-tested; it changes
  system network settings, so run it yourself first)
- systemd unit tried on a Raspberry Pi (runs on Ubuntu 24.04 x86_64)

## License

MIT — see [LICENSE](LICENSE).

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for development checks and pull requests,
and [SECURITY.md](SECURITY.md) for vulnerability reporting and deployment boundaries.
