# router-relay

**English** | [Русский](README.ru.md)

A local, dependency-free, OpenAI-compatible (`/v1`) relay that makes a cheap router usable
from streaming clients such as the Cline CLI and the Hermes agent: it repairs broken SSE
streams, fails over between the direct path and a local proxy, and keeps every turn inside
the client's timeout.

```
client ──SSE──▶ 127.0.0.1:8765 ──stream:false (replay) or SSE (passthrough)──▶ router
                               ◀── JSON or SSE ── relay ── valid SSE ── client
```

## Why this exists

The upstream router (`router.cheap` / `direct.router-cheap.com`) accepts streaming
requests but never finishes them: it sends content chunks and then drops the connection
without a `finish_reason` chunk and without `data: [DONE]`.

Cline always streams (it calls `streamText` and has no non-streaming fallback), so it
reports:

```
Error: Failed to process successful response: TypeError: The socket connection was
closed unexpectedly. ... (ECONNRESET)
```

The same router answers perfectly when asked with `"stream": false`: the full message, a
proper `finish_reason` and the `usage` block all arrive in one JSON body.

## What it does

The relay listens on `127.0.0.1`, forces `"stream": false` on the upstream request, and
then hands the finished answer back to the client in whichever shape the client expects:

| Client sent | Relay answers with |
|---|---|
| `"stream": true` | A **valid SSE stream**: `role` delta, `reasoning_content` deltas, `content` deltas, `tool_calls` deltas, a final chunk carrying `finish_reason` (and `usage` when present), then `data: [DONE]`. |
| `"stream": false`, or the field omitted | A **single JSON object** (`object: "chat.completion"`). |

Both modes are required. A streaming client (Cline, the Hermes agent turn) needs SSE,
while a non-streaming client (Hermes auxiliary calls — smart approvals, title
generation, compression, vision) parses the body as one JSON object and breaks on an SSE
stream with `AttributeError: 'str' object has no attribute 'choices'`. Only an explicit
`"stream": true` is treated as streaming, because an OpenAI-compatible client that omits
the field is asking for the API default, which is non-streaming.

The model is taken from the incoming request, so the client may switch models freely
(use `--model` to pin one instead). `stream_options` is stripped before the upstream
call because it is meaningless without streaming.

## Requirements

- **Node.js 18 or newer** — the relay uses the `node:` module prefix, keep-alive agents and
  `AbortSignal`. There are no npm dependencies.
- **No external tools.** The proxy tunnel is built with Node's own `CONNECT` support, so
  `curl` is not needed (it was, in earlier versions).
- Windows PowerShell 5.1+ or PowerShell 7+ for the optional launchers.

## Quick start

```powershell
# 1. Create the local config from the template and fill in your values.
Copy-Item conf.example.txt conf.txt

# 2. Start the relay.
node router-relay.js --port 8765

# 3. Verify it is alive (reports the failover routes and any cooling route).
curl http://127.0.0.1:8765/health

# 4. Stop it again.
.\Stop-Relay.ps1 -Port 8765
```

Then point any OpenAI-compatible client at `http://127.0.0.1:8765/v1` with any
placeholder API key — the relay always authenticates upstream with the key from
`conf.txt` and ignores whatever the client sends.

A one-off check without a client:

```powershell
curl.exe -s http://127.0.0.1:8765/v1/chat/completions `
  -H "content-type: application/json" `
  -d '{"model":"your-model-id","messages":[{"role":"user","content":"Say OK"}]}'
```

## Configuration

`conf.txt` sits next to `router-relay.js` and is read line by line as `key=value`.
Blank lines and lines starting with `#` or `;` are ignored.

| Key | Meaning |
|---|---|
| `base_url` | Main upstream endpoint, including `/v1` (e.g. `https://router.cheap/v1`). |
| `base_url_reserved` | Spare hostname, used as an extra failover route. |
| `api_key` | Bearer token sent upstream. |
| `model` | Read by `Start-Cline.ps1` to build the isolated profile. The relay itself takes the model from the request unless `--model` is passed. |
| `proxy` | Optional local HTTP proxy (e.g. `http://127.0.0.1:10809`). Empty = direct. The tunnel is built with `CONNECT` by the relay itself, so proxied requests stream exactly like direct ones. Requires an `https://` `base_url`. |
| `port` | TCP port on `127.0.0.1`. Empty or absent = `8765`. `0` lets the OS pick a free port (printed at startup and reported by `/health`); any other number runs several relays side by side. **`--port` overrides this.** |
| `stream` | `off` (default), `on` or `auto` — see [Streaming](#streaming). **`--stream` overrides this.** |
| `route_order` | `direct-first` (default) or `proxy-first` — which path is tried first. **`--route-order` overrides this.** |
| `timeout` | Seconds one upstream attempt may take. Default `120`. **`--timeout` overrides this.** |
| `budget` | Seconds the whole request may take, across every route and both passes. Default `240`. **`--budget` overrides this.** |
| `probe` | `on` (default) or `off` — re-probe cooling routes in the background. **`--probe` overrides this.** |

> **`conf.txt` is git-ignored.** Only `conf.example.txt` is committed. See
> [Security](#security).

### Streaming

The relay can either **replay** a finished answer as SSE or **pass the upstream stream through**.
Which one is right depends on the upstream, so it is a setting rather than a guess:

| `stream=` | Behaviour | Use when |
|---|---|---|
| `off` | Asks the upstream for `"stream": false`, reads the complete JSON answer, then replays it as a valid SSE stream (role delta, content deltas, tool-call deltas, `finish_reason`, `[DONE]`). | **Default.** The upstream never terminates its stream — the case this relay was written for. |
| `on` | Passes the upstream SSE bytes through untouched: real incremental output, lower latency, no re-serialisation. | The upstream terminates properly (`finish_reason` + `[DONE]`). Selectel AI Gateway does. |
| `auto` | Tries the passthrough; if a route's stream ends without `[DONE]`, that route is remembered and buffered from then on. | An upstream whose behaviour you are unsure of. |

Two invariants hold in every mode:

- A client that does **not** set `"stream": true` always gets one JSON object, never SSE. Hermes's
  auxiliary calls (approvals, titles, compression) depend on this.
- Passthrough works on direct **and** proxied routes: the transport is Node's own HTTP client,
  which builds a `CONNECT` tunnel for `proxy=`. The old curl transport buffered every body, which
  is why proxied traffic used to be replayed regardless of `stream=`.

Failover is identical in both modes: a bad status or a failed connection is retried on the next
route *before* anything reaches the client, because the response headers are known before the
first body byte is forwarded.

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| `POST` | `/v1/chat/completions` | Translated to the upstream router; SSE or JSON as described above. |
| `GET` | `/v1/models` | Passed through to the upstream router (30 s timeout). |
| `GET` | `/health`, `/` | Local liveness check. Returns `ok`, `port`, `stream`, `route_order`, `timeout_ms`, `budget_ms`, `probe`, `upstream`, `upstream_reserved`, `proxy`, `routes`, `cooling` and `pid`. |

`/health` is what the launchers poll before starting a client, so it must answer even
while every upstream route is down.

## CLI reference

```
node router-relay.js [--port 8765] [--stream off|on|auto] [--host URL] [--host-reserved URL]
                     [--key KEY] [--model ID] [--conf path] [--proxy URL] [--log FILE]
                     [--route-order direct-first|proxy-first] [--timeout SEC] [--budget SEC]
                     [--probe on|off] [-v]
```

| Flag | Alias | Default | Meaning |
|---|---|---|---|
| `--port` | `-p` | `conf.txt` `port`, else `8765` | TCP port on `127.0.0.1`. `0` = let the OS pick. |
| `--stream` | `-s` | `conf.txt` `stream`, else `off` | Streaming mode: `off`, `on` or `auto`. |
| `--host` | `-b` | `conf.txt` `base_url` | Override the main upstream URL. |
| `--host-reserved` | `-B` | `conf.txt` `base_url_reserved` | Override the spare upstream URL. |
| `--key` | `-k` | `conf.txt` `api_key` | Override the upstream API key. |
| `--model` | `-m` | request model | Pin the model, ignoring what the client sends. |
| `--conf` | | `<script dir>\conf.txt` | Alternative config file. |
| `--proxy` | | `conf.txt` `proxy` | Override the proxy URL. |
| `--route-order` | `-r` | `conf.txt` `route_order`, else `direct-first` | Which path is tried first. |
| `--timeout` | `-t` | `conf.txt` `timeout`, else `120` | Seconds per upstream attempt. |
| `--budget` | | `conf.txt` `budget`, else `240` | Seconds for the whole request. |
| `--probe` | | `conf.txt` `probe`, else `on` | Background re-probing of cooling routes. |
| `--log` | | none | Append diagnostics to a file. |
| `--verbose` | `-v` | off | Also echo diagnostics to the console. |
| `--help` | `-h` | | Print the header comment of the script. |

The relay refuses to start without a host and a key (exit code `2`), exits with code `2` on an
invalid `--port`, `--stream`, `--route-order`, `--timeout`, `--budget` or `--probe` value, on a
`proxy=` combined with a non-`https://` `base_url`, and exits with code `3` if the port is
already in use.

Running several relays side by side:

```powershell
node router-relay.js --port 8765
node router-relay.js --port 8766 --log relay-8766.log
node router-relay.js --port 0 --log relay-auto.log   # OS picks, see the log line
```

## Resilience

The relay is built to outlive broken requests rather than to be restarted after them.

**Transport and host failover.** The upstream sits behind Cloudflare and can be
unreachable from some regions (block or challenge), while the local proxy can equally be
down. Instead of picking one, every request walks an ordered route list built as
`host × transport`:

```
base_url            direct
base_url            via proxy
base_url_reserved   direct
base_url_reserved   via proxy
```

Direct comes first because it is the fastest path (~80 ms of TLS against ~300 ms through
the proxy) and it does not depend on the proxy being up. The proxy follows immediately
because when Cloudflare blocks a region every direct host fails together — both hostnames
are the same zone. The reserved host is still walked, for the case where a single
hostname is in trouble rather than the whole zone. `route_order=proxy-first` reverses that
within each host, for a machine where the direct path is blocked: there every direct attempt
costs 10–20 s before the tunnel is even tried.

The whole list is walked **twice** (`ROUTE_PASSES = 2`), because the router also flakes
on its own independently of the route.

**What triggers failover.** Transport errors and `403` / `408` / `429` / any `5xx`.
A `400` / `401` / `404` is returned immediately, because a caller-side mistake reproduces
identically on every route and retrying it would only burn the failover budget. If every
route fails with the same edge error, the last real response is mirrored to the client so
it sees the true status instead of a relay-invented failure.

**Cooldown with exponential backoff.** A blocked route does not fail fast: a
Cloudflare-reset connection can take ~19 s to surface, and re-paying that on every request
would be worse than the outage it guards against. A route that fails is therefore put in
the corner — `60 s`, doubling per consecutive failure, capped at `15 min` — and healthy
routes are tried first. A cooling route is never dropped: if the healthy ones also fail,
the cooling ones are still walked. Strikes are cleared on success. A cooling route is also
**re-probed in the background** (`probe=on`, every 5 minutes), so a recovered path comes back
without a user request paying for the discovery.

**Timeouts that add up.** `timeout` (default 120 s) caps one attempt and `budget`
(default 240 s) caps the whole request across every route and both passes. Without the
budget, 4 routes × 2 passes × 10 minutes meant a single turn could hang for 80 minutes while
the client had given up after five — and the router kept billing for answers nobody read.
Keep `budget` below the client's own timeout (Cline uses 300 s).

**Client identity.** The router only serves traffic it recognises as a mainstream coding CLI,
so the client's `User-Agent` is forwarded upstream. A router that answers `200` with a policy
notice instead of a completion is reported as a short `502` error rather than replayed as an
assistant message, so the client does not present it as the model's answer. The notice's own
text is never reproduced — not in the response and not in the log — because it tells the reader
nothing they can act on. In passthrough mode the first few bytes are held back until the answer
actually starts, which is what makes the same check possible before the `200` is committed.

**Client disconnects.** When the client hangs up, the upstream request is aborted instead of
being finished into the void. A disconnect does not put the route into cooldown — the route
is fine, the caller simply left.

**Process-level guards.** `unhandledRejection` and `uncaughtException` are logged instead
of killing the process (Node ≥ 15 would otherwise exit mid-answer, which is exactly how
this used to surface as `ECONNRESET` in Cline). Client sockets that vanish are tolerated,
every request path is wrapped, and a `200` without `choices` is surfaced as a proper
`502` instead of being replayed as an empty answer.

## Launchers (Windows)

Both launchers start the relay when it is not answering `/health`, then hand over to the
client.

### `Start-Cline.ps1`

Runs the Cline CLI against the router on a **separate profile**, so the provider you
normally use in Cline (its model, key and base URL) stays exactly as it is and plain
`cline` keeps using it.

- The profile lives in `<script dir>\cline-router\data` and is passed to the CLI as
  `--data-dir`. Sessions, logs and provider selection stay there.
- The *config* dir stays the default `~\.cline`, so global rules / skills / hooks /
  workflows are still shared. The global MCP server list is shared too, via
  `CLINE_MCP_SETTINGS_PATH`.
- The profile uses its own provider id, `oi-router`, rather than the built-in
  `openai-compatible`, because Cline ignores `models.json` entries for built-in
  providers. Without that, the status bar would show `$0.00` and the context window would
  stay unknown.
- Model metadata (context window, max input/output, prices) is copied from the default
  profile's `models.json` on every run, so it matches what plain `cline` uses for the same
  model. The default profile is only ever read.

| Parameter | Default | Meaning |
|---|---|---|
| `-Workspace <dir>` | current directory | Forwarded to the CLI as `--cwd`. |
| `-RelayPort <port>` | `conf.txt` `port`, else `8765` | Port of the local relay (shared with `Start-Hermes.ps1`). With `port=0` a free port is picked for this run. |
| `-ProfileDir <dir>` | `<script dir>\cline-router` | Root of the isolated profile. |
| `-SyncProvider` | off | Force a rewrite of the profile from `conf.txt` and the default model metadata. Idempotent; run it after editing `conf.txt` or changing `-RelayPort`. |
| `-NoRelay` | off | Point the profile straight at `conf.txt` `base_url`. The router does not terminate SSE streams, so long answers will fail with `ECONNRESET` — debugging only. |
| `-UsePublic` | off | With `-NoRelay`, use `base_url_reserved` instead of `base_url`. |
| `-NoSharedMcp` | off | Do not share the global MCP settings file with the isolated profile. |
| `-Status` | | Print the router profile and the default profile, then exit. Read-only. |
| `-StopRelay` | | Stop the running relay process(es) and exit. |
| `-ClineArgs` | | Remaining arguments are passed to the Cline CLI unchanged. |

```powershell
.\Start-Cline.ps1                          # TUI on the router profile
.\Start-Cline.ps1 -Status                  # show provider, model, context window, prices
.\Start-Cline.ps1 --yolo "run the tests"   # headless one-shot
.\Start-Cline.ps1 -StopRelay               # stop the relay
```

### `Start-Hermes.ps1`

Starts the relay if needed, warns when the local proxy port is not listening, then runs
`hermes.exe` with every argument forwarded verbatim.

| Parameter | Default | Meaning |
|---|---|---|
| `-SkipRelayCheck` | off | Do not touch the relay — just run Hermes. |
| `-RelayPort <port>` | `conf.txt` `port`, else `8765` | Port to verify/start. `port=0` picks a free port for this run. |
| `-HermesArgs` | | Remaining arguments are passed to the Hermes CLI unchanged. |

```powershell
.\Start-Hermes.ps1                      # opens the Hermes TUI
.\Start-Hermes.ps1 doctor               # Hermes' own config/dependency check
.\Start-Hermes.ps1 -z "Reply with exactly: OK"
```

> Both launchers warn if the proxy port (`10809` by default) is not listening, because
> the relay needs it to reach the router when `proxy=` is set.

### `Stop-Relay.ps1`

Stops relay instances. A relay is identified **positively** before anything is killed, because
"whatever listens on 8765" is not the same thing as "the relay":

1. the process command line runs `router-relay.js`, **and**
2. it answers `/health` on the port with a `pid` matching the listener's process id.

If a port is held by something else, the script says so and leaves it running (exit code `1`).
The port of a relay is found from its listening socket, its `/health` answer, or its command line —
so a relay started with no `--port` (port from `conf.txt`) or with `--port 0` is still found.

| Parameter | Default | Meaning |
|---|---|---|
| `-Port <port>[,<port>]` | | Stop the relay(s) on these ports. Omit with `-All` to list instead. |
| `-All` | off | Stop every relay process on the machine. |
| `-Force` | off | Do not ask for confirmation when stopping several at once. |

```powershell
.\Stop-Relay.ps1                  # list running relays (PID, port, stream, upstream)
.\Stop-Relay.ps1 -Port 8765       # stop the relay on 8765
.\Stop-Relay.ps1 -Port 8765,8766  # stop two, after confirmation
.\Stop-Relay.ps1 -All             # stop every relay
.\Stop-Relay.ps1 -All -WhatIf     # dry run: show what would be stopped
```

Processes whose command line cannot be read (another user, no elevation) are reported and never
touched. Exit codes: `0` — done or nothing to do, `1` — a port was refused or a stop failed.

## Repository layout

```
router-relay.js        the relay (single file, no dependencies)
Start-Cline.ps1        Cline CLI launcher on an isolated profile
Start-Hermes.ps1       Hermes Agent CLI launcher
Stop-Relay.ps1         stops relays by port, or all of them
conf.example.txt       config template — copy to conf.txt and fill in
.gitignore             keeps conf.txt, the isolated profile and logs out of git
.gitattributes         LF for code/config, CRLF for PowerShell
README.md              this document (English)
README.ru.md           Russian translation — перевод на русский
LICENSE                MIT
.github/workflows/     CI: secret scan (gitleaks) + repository policy
scripts/               check-repo-policy.sh — the policy checks, runnable locally
```

Generated at runtime and intentionally **not** committed: `conf.txt`,
`cline-router/`, `*.log`.

## CI: no key ever leaves the machine

`.github/workflows/secrets.yml` runs on every push and pull request, on manual
dispatch, and once a week (a key committed months ago is still a leak, and the
gitleaks rule set keeps improving). Two independent jobs:

**`gitleaks`** — installs a pinned gitleaks binary and scans the **entire commit
history** (`gitleaks git`, with `fetch-depth: 0`) plus the working tree
(`gitleaks dir`). It always runs with `--redact`, because printing a detected
secret into a CI log would turn a caught leak into a published one. The scan
fails the build on any finding.

**`policy`** — runs `scripts/check-repo-policy.sh`, which enforces the rules a
generic scanner cannot know about:

- `conf.txt`, `.env`, `cline-router/*`, `*.log`, `*.bak*` and `node_modules/*`
  must not be tracked (catches a `git add -f` that bypassed `.gitignore`);
- the `.gitignore` rules must still be live — checked behaviourally with
  `git check-ignore`, so reformatting the file cannot quietly disable them;
- no credential-shaped string (`sk-…`, `gh*_…`, `github_pat_…`, `AKIA…`,
  `xox*-…`, PEM private keys) may appear in tracked files.

The same script runs locally, so a leak is caught before it is pushed:

```powershell
bash scripts/check-repo-policy.sh
```

## Security

- `conf.txt` contains a live API key and is git-ignored. Never commit it; use
  `conf.example.txt` as the template.
- The isolated Cline profile (`cline-router/data/settings/providers.json`) stores the same
  key in plain text, and chat sessions under `cline-router/data/sessions/` may contain
  request bodies. The whole directory is git-ignored.
- The relay binds to `127.0.0.1` only and does not validate client credentials — anyone
  who can reach the port can spend your upstream quota. Do not expose it.
- If a key was ever committed or shared, rotate it at the provider.

## Notes

- `-SyncProvider` re-reads `conf.txt` and re-applies it (now pointing at the relay URL).
- `-StopRelay` stops the relay and exits; `-RelayPort 9000` uses another port.
- `-NoRelay` (optionally with `-UsePublic`) talks to the router directly — for
  comparison only, since streaming is broken there.

## License

MIT — see [LICENSE](LICENSE). You are free to use, modify, merge, publish,
distribute and sell copies of this software; the only obligations are keeping
the copyright notice and this permission notice in substantial copies, and the
software is provided "as is", without warranty.




