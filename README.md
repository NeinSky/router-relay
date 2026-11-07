# router-relay

**English** | [Русский](README.ru.md)

A local, dependency-free, OpenAI-compatible (`/v1`) relay that makes a router which
cannot terminate an SSE stream usable from streaming clients such as the Cline CLI and
the Hermes agent.

```
client ──SSE──▶ 127.0.0.1:8765 ──JSON (stream:false)──▶ router ──JSON──▶ relay ──valid SSE──▶ client
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

- **Node.js 18 or newer** — the relay uses global `fetch`, `AbortSignal.timeout` and the
  `node:` module prefix. There are no npm dependencies.
- **`curl.exe`** — only when `proxy=` is configured, because Node's own `fetch` genuinely
  ignores `HTTP_PROXY`/`HTTPS_PROXY` and curl is used to build the tunnel. Already
  present on Windows 10 1803+ and most Linux distributions.
- Windows PowerShell 5.1+ or PowerShell 7+ for the optional launchers.

## Quick start

```powershell
# 1. Create the local config from the template and fill in your values.
Copy-Item conf.example.txt conf.txt

# 2. Start the relay.
node router-relay.js --port 8765

# 3. Verify it is alive (reports the failover routes and any cooling route).
curl http://127.0.0.1:8765/health
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
| `proxy` | Optional local HTTP proxy (e.g. `http://127.0.0.1:10809`). Empty = direct. |

> **`conf.txt` is git-ignored.** Only `conf.example.txt` is committed. See
> [Security](#security).

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| `POST` | `/v1/chat/completions` | Translated to the upstream router; SSE or JSON as described above. |
| `GET` | `/v1/models` | Passed through to the upstream router (30 s timeout). |
| `GET` | `/health`, `/` | Local liveness check. Returns `ok`, `upstream`, `upstream_reserved`, `proxy`, `routes`, `cooling` and `pid`. |

`/health` is what the launchers poll before starting a client, so it must answer even
while every upstream route is down.

## CLI reference

```
node router-relay.js [--port 8765] [--host URL] [--host-reserved URL] [--key KEY]
                     [--model ID] [--conf path] [--proxy URL] [--log FILE] [-v]
```

| Flag | Alias | Default | Meaning |
|---|---|---|---|
| `--port` | `-p` | `8765` | TCP port on `127.0.0.1`. |
| `--host` | `-b` | `conf.txt` `base_url` | Override the main upstream URL. |
| `--host-reserved` | `-B` | `conf.txt` `base_url_reserved` | Override the spare upstream URL. |
| `--key` | `-k` | `conf.txt` `api_key` | Override the upstream API key. |
| `--model` | `-m` | request model | Pin the model, ignoring what the client sends. |
| `--conf` | | `<script dir>\conf.txt` | Alternative config file. |
| `--proxy` | | `conf.txt` `proxy` | Override the proxy URL. |
| `--log` | | none | Append diagnostics to a file. |
| `--verbose` | `-v` | off | Also echo diagnostics to the console. |
| `--help` | `-h` | | Print the header comment of the script. |

The relay refuses to start without a host and a key (exit code `2`), and exits with code
`3` if the port is already in use.

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
hostname is in trouble rather than the whole zone.

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
the cooling ones are still walked. Strikes are cleared on success, so a recovered direct
path comes back on its own.

**Process-level guards.** `unhandledRejection` and `uncaughtException` are logged instead
of killing the process (Node ≥ 15 would otherwise exit mid-answer, which is exactly how
this used to surface as `ECONNRESET` in Cline). Client sockets that vanish are tolerated,
every request path is wrapped, and a `200` without `choices` is surfaced as a proper
`502` instead of being replayed as an empty answer.

Per-attempt timeout is 10 minutes, because reasoning models can think for a long time.

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
| `-RelayPort <port>` | `8765` | Port of the local relay (shared with `Start-Hermes.ps1`). |
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
| `-RelayPort <port>` | `8765` | Port to verify/start. |
| `-HermesArgs` | | Remaining arguments are passed to the Hermes CLI unchanged. |

```powershell
.\Start-Hermes.ps1                      # opens the Hermes TUI
.\Start-Hermes.ps1 doctor               # Hermes' own config/dependency check
.\Start-Hermes.ps1 -z "Reply with exactly: OK"
```

> Both launchers warn if the proxy port (`10809` by default) is not listening, because
> the relay needs it to reach the router when `proxy=` is set.

## Repository layout

```
router-relay.js        the relay (single file, no dependencies)
Start-Cline.ps1        Cline CLI launcher on an isolated profile
Start-Hermes.ps1       Hermes Agent CLI launcher
conf.example.txt       config template — copy to conf.txt and fill in
.gitignore             keeps conf.txt, the isolated profile and logs out of git
.gitattributes         LF for code/config, CRLF for PowerShell
README.md              this document (English)
README.ru.md           Russian translation — перевод на русский
```

Generated at runtime and intentionally **not** committed: `conf.txt`,
`cline-router/`, `*.log`.

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

No license file is included. Add one before redistributing.



