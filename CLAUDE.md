# teapot

Personal mesh on [iroh](https://iroh.computer): devices dial each other by
endpoint id; first feature is an **exit node** — traffic from one device leaves
the internet with another device's IP. Rust agent + CLI. The control plane is a
separate repo, `ManifoldSystems/teapot-control` (Hono on Cloudflare Workers,
live at `https://teapot.fannn.tech`).

Owner: Eric (algonacci). Private, single-tenant: one deployment per owner;
buyers get a clone. **Scope stays small** — only what the owner actually uses.
The broad hub-less mesh (exec, flows, ingress, IoT) is Rin's (r17x) `fpx`; Rin
allows reusing fpx code with attribution, but only pull in what fits this scope.

Design history and lessons live in `~/Documents/GitHub/second-brain/Teapot/`
(read its README first). This file is the operating manual.

## Layout

```
crates/teapot-proto   wire format + ALPN `teapot/proxy/0`; spec in docs/protocol.md
crates/teapot-core    agent logic, reusable by a future mobile app
  agent.rs            join control, live allowlist sync (+ disk cache), exit selection
                      (pick_exit / follow_exits), public IP reporting
  control.rs          control client: Ed25519 challenge login, device API,
                      presence WebSocket (ping, status with paths/traffic/ips)
  exit.rs             exit node: allowlist (static or live), LAN guard, byte counting
  proxy.rs            local HTTP proxy (CONNECT + absolute-form) → one iroh
                      connection per exit, one bi-stream per TCP connection;
                      exit per request via proxy username; GET /_teapot/exits
  paths.rs            per-peer path (direct/relay, rtt) and Counted<T> byte counters
  net.rs              dial with LAN/loopback/CGNAT/ULA guard on resolved IPs
  key.rs              secret key file (0600); endpoint id = public key
  tests/tunnel.rs     exit + proxy in one process, no relay/DNS (MemoryLookup)
crates/teapot         CLI: id, login, status, up, exit, proxy, service, system-proxy
  service.rs          launchd agent (macOS) / systemd user unit (Linux)
  sysproxy.rs         macOS system proxy on/off via networksetup, restorable
docs/protocol.md      single source of truth for every wire format — change it first
```

## Commands

```bash
cargo build --release        # target/release/teapot
cargo test                   # unit + tests/tunnel.rs (no network needed)
cargo clippy --all-targets   # must be clean; also clean on Linux (see below)
cargo fmt
```

CLI essentials (`teapot --help` for all):

```bash
teapot login                         # opens /approve/<id>; waits for "Add device"
teapot service install [--exit NAME] # runs `teapot up` under launchd/systemd
teapot service status|uninstall
teapot status                        # approval, exit, public IPs, peers
teapot up --exit quantumz            # exit node + local proxy 127.0.0.1:1080
teapot system-proxy off              # undo a stuck --system-proxy
```

Global flags baked into the service: `--key`, `--control`
(default `https://teapot.fannn.tech`), `--name` (default hostname).

## Conventions

- Rust 2024, tokio. Errors: `anyhow` in core/CLI. Keep `clippy` warning-free on
  **both** macOS and Linux — gate macOS-only helpers with
  `#[cfg(target_os = "macos")]`.
- Match surrounding comment density; comments explain *why*.
- A wire change touches `docs/protocol.md`, this repo, and teapot-control
  together. The Flutter app (`teapot-app`, not started) will read the same spec.
- Commit messages end with the Co-Authored-By line the session provides.

## Rollout to the fleet

Machines and their roles are listed in second-brain `Teapot/README.md`
(fleet table). macOS binaries are built here; Linux binaries are built on
`algo-apps` (Ubuntu 24.04, glibc 2.39) and copied to the other Ubuntu hosts —
cross-compiling from macOS fails because `ring` needs a C cross-compiler.

```bash
cargo build --release && install -m 755 target/release/teapot ~/.local/bin/teapot
rsync -az --exclude target --exclude .git ./ algo-apps:teapot-src/
ssh algo-apps 'cd ~/teapot-src && ~/.cargo/bin/cargo build --release'
scp -3 algo-apps:teapot-src/target/release/teapot <host>:/tmp/teapot   # then install
<host>: ~/.local/bin/teapot [--name NAME] service install [--exit NAME]
```

`service install` restarts the service and must be given the same flags again
(name, exit) — it rewrites the unit/plist from its own arguments.

If the control plane change touches the Durable Object (`presence.ts`), stop
**every** agent for ~80 s before restarting them, or the old DO code keeps
running (see teapot-control CLAUDE.md).

## Gotchas learned the hard way

- **One key, one process.** Two processes with the same key fight over the
  endpoint id. That is why `up` runs exit + proxy together.
- **Cloudflare blocks library user agents** (403 / error 1010). The agent always
  sends `teapot/<version>`.
- **launchd `Bootstrap failed: 5`** if you bootstrap right after bootout;
  `service.rs` waits until `launchctl print` fails first.
- **SIGTERM**, not only Ctrl+C, must trigger cleanup (launchd/systemd stop).
- **Presence needs a silence timeout.** A phone stayed "connected" to nothing
  for 8 h (process alive, socket dead). 45 s without any message from control
  → reconnect (`SILENCE_LIMIT`).
- **A restarted exit leaves a silent connection, not a closed one.** Same id,
  new process: the cached connection never errors until QUIC's idle timeout.
  Reused connections get 5 s; no datagram at all from the peer → close, redial,
  retry once (`REUSED_ANSWER_TIMEOUT`, test `restarted_exit_is_redialed…`).
- **Never hold the proxy state lock across a dial** — failover would wait for a
  dead exit to time out.
- **Paths via Tailscale.** All fleet machines share a tailnet; iroh often picks
  `100.x` addresses as "direct". Check the address before claiming NAT
  traversal.
- **zsh does not word-split `$VAR`.** `for h in $HOSTS` runs once with the whole
  string; write the list inline or use an array. `path` is a zsh special
  (tied to `PATH`) — never use it as a variable name.
- `curl` ignores the macOS system proxy; browsers follow it.
- Exit choice skips peers whose reported IPv4 equals this device's own (same
  network → same IP → pointless). Unknown IPs never exclude.
- `--system-proxy` changes macOS network settings — do not run it on the
  owner's machines without them; they test it.

## Safety

- Admin password for the dashboard: `~/Documents/GitHub/teapot-control/.admin-token`
  (gitignored). Never print it.
- Several fleet hosts are production (nginx/docker): lb-sumopod-tencent,
  server-indovm, lentera. Do not touch their other services; teapot runs as an
  unprivileged user unit there. sudo needs the owner (except where noted).
- Changing system settings on any host (linger, journald groups, proxy) is the
  owner's call: print the command, let them run it.
