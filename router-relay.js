#!/usr/bin/env node
/*
 * router-relay.js - local OpenAI-compatible shim for Cline.
 *
 * WHY THIS EXISTS
 *   The upstream router (router.cheap / direct.router-cheap.com) used to accept SSE requests but
 *   never finish them: it sent content chunks, then dropped the connection without a `finish_reason`
 *   chunk and without `data: [DONE]`. Cline (which always streams - it calls streamText and has no
 *   non-streaming fallback) therefore reported:
 *
 *     Error: Failed to process successful response: TypeError: The socket connection was closed
 *     unexpectedly. ... (ECONNRESET)
 *
 *   The same router answered perfectly when asked with "stream": false: the full message, a proper
 *   finish_reason and the usage block all arrived in one JSON body.
 *
 *   The router has since been fixed and does terminate its streams, so `stream=on` / `stream=auto`
 *   are now real options. The replay path stays the default because it works with either kind of
 *   upstream, and because it is the only mode that survives a router which drops a stream again.
 *
 * WHAT IT DOES
 *   Serves /v1/chat/completions on 127.0.0.1, rewrites the request to "stream": false, calls the
 *   real router, and replays the finished JSON answer as a *valid* SSE stream: role delta, content
 *   deltas, tool_call deltas, a final chunk carrying finish_reason, then `data: [DONE]`.
 *
 *   A client that does not ask for streaming - "stream": false, or the field omitted, which is the
 *   [OI] API default - gets the upstream body back as plain JSON instead. Both modes are required: a
 *   streaming client (Cline, the Hermes agent turn) sets "stream": true and needs SSE, while a
 *   non-streaming client (Hermes auxiliary calls - smart approvals, title generation, compression,
 *   vision) parses the body as a single JSON object and breaks on an SSE stream with
 *   "AttributeError: 'str' object has no attribute 'choices'".
 *
 * USAGE
 *   node router-relay.js [--port 8765] [--stream off|on|auto] [--host URL] [--host-reserved URL]
 *                        [--key KEY] [--model ID] [--conf path] [--proxy URL] [--log FILE]
 *                        [--route-order direct-first|proxy-first] [--timeout SEC] [--budget SEC]
 *                        [--probe on|off] [-v]
 *
 *   Defaults come from conf.txt next to this file (base_url - the main address - then
 *   base_url_reserved; api_key; port; stream; proxy; route_order; timeout; budget; probe). Use
 *   --host to point the relay somewhere else. The model is taken from the incoming request, so
 *   Cline may switch models freely.
 *
 *   PORT. --port wins over conf.txt `port`, which wins over 8765. A port of 0 lets the OS pick a
 *   free one; the real number is printed at startup and reported by /health. This is what makes
 *   several relays coexist on one machine.
 *
 *   STREAMING. `stream=off` (default) asks the upstream for a complete JSON answer and replays it
 *   as a valid SSE stream - the original workaround for routers that never finish a stream.
 *   `stream=on` passes the upstream SSE through untouched: lower latency and genuinely incre-
 *   mental output, but only for an upstream that terminates its stream. `stream=auto` tries the
 *   passthrough and falls back to replay when the stream turns out to be broken. All three work
 *   through the proxy too - the transport is Node's own HTTP client over a CONNECT tunnel.
 *
 *   ROUTE ORDER. `route_order=direct-first` (default) tries the direct path before the proxy
 *   because it is faster. `proxy-first` is for a machine where direct is blocked: there every
 *   direct attempt costs 10-20 s before the tunnel is even tried.
 *
 *   BUDGET. `timeout` caps one attempt, `budget` caps the whole request across every route and
 *   both passes. Together they keep the relay inside the client's own timeout, so a stuck turn
 *   ends with a clear 502 instead of retrying for an hour after the client gave up.
 *
 * ENDPOINTS
 *   POST /v1/chat/completions   translated to the upstream router
 *   GET  /v1/models             passed through to the upstream router
 *   GET  /health                local liveness check (also reports the failover route list)
 *
 * RESILIENCE
 *   The relay is meant to outlive broken requests: every request path is wrapped, client sockets
 *   that vanish are tolerated, and process-level guards keep the relay alive instead of letting an
 *   unhandled rejection kill it mid-answer (that used to surface in Cline as ECONNRESET).
 *
 *   Transport + host failover. The upstream sits behind Cloudflare and can be unreachable from some
 *   regions (a block or challenge), while the local proxy can equally be down. So instead of
 *   picking one, every request walks an ordered route list - direct on the main host, then the
 *   proxy, then the reserved host, and so on - and a second pass re-walks it, because the router
 *   also flakes on its own. Direct is first because it is faster and needs no proxy, unless
 *   `route_order=proxy-first` says otherwise. Failover triggers on transport errors and on
 *   403/408/429/5xx; a 400/401/404 is returned immediately so a caller-side mistake is not retried
 *   across the whole list. A route that fails cools down with exponential backoff (60 s doubling to
 *   a 15-minute ceiling), because a blocked connection can take ~19 s to surface and re-paying that
 *   per request is worse than the outage it guards against. A cooling route is re-probed in the
 *   background, so a recovered path comes back without a user request paying for the discovery.
 *
 *   Client identity. The router only serves traffic it recognises as a mainstream coding CLI, so
 *   the client's User-Agent is forwarded upstream. A router that answers 200 with a policy notice
 *   instead of a completion is reported as a short 502 error rather than replayed as an assistant
 *   message; the notice's own text is never reproduced in the response or the log.
 */

'use strict';

const http = require('node:http');
const https = require('node:https');
const tls = require('node:tls');
const fs = require('node:fs');
const path = require('node:path');
const { Readable } = require('node:stream');

const DEFAULT_PORT = 8765;
// "off" keeps the original behaviour: a complete JSON answer is fetched and replayed as SSE. It is
// the only mode that works with an upstream which never terminates its stream, so it stays the
// default. "on" trusts the upstream to finish properly (finish_reason + [DONE]) and simply relays
// its bytes; "auto" probes passthrough and degrades per route.
const STREAM_MODES = ['off', 'on', 'auto'];
const DEFAULT_STREAM_MODE = 'off';
// How much of the tail of a passthrough stream is inspected for the `[DONE]` sentinel. A few KB is
// plenty - the sentinel is always the last event - and it keeps the detector from buffering a whole
// (possibly huge) answer just to answer "did this stream terminate?".
const PASSTHROUGH_TAIL_CHARS = 2048;
// One attempt must be long enough for a slow reasoning turn, but short enough that the failover
// budget below still fits inside the client's own timeout (Cline uses 5 minutes).
const REQUEST_TIMEOUT_MS = 120000;
// The whole request - every route, both passes - is capped by one deadline. The old code had no
// deadline at all: 4 routes x 2 passes x a 10-minute timeout meant a single turn could hang for up
// to 80 minutes while the client had given up after five and the router kept billing for tokens
// nobody would read.
const REQUEST_BUDGET_MS = 240000;
// A proxy CONNECT is a plain TCP handshake plus a TLS handshake; 15 s is generous and keeps a dead
// tunnel from eating the whole budget.
const CONNECT_TIMEOUT_MS = 15000;
// Failover budget: each route in the list is tried once per pass, and a second pass re-walks the
// whole list because the upstream router also flakes on its own, independently of which route we
// picked. Normal traffic succeeds on route 1 and never pays for the rest.
const ROUTE_PASSES = 2;
// How long a route that failed stays out of the way (60 s, doubling to 15 minutes). A blocked
// route does not fail fast: a Cloudflare-reset connection can take ~19 s to surface, and paying
// that on every request would be worse than the outage it protects against.
const ROUTE_COOLDOWN_MS = 60000;
const ROUTE_COOLDOWN_MAX_MS = 900000;
// A cooling route is re-probed on its own instead of on the back of a user request. Without this
// a recovered route only returned when its cooldown happened to expire in the middle of a turn,
// and the "strike" counters grew forever (they reached 189/75 on a ten-day-old process).
const ROUTE_PROBE_MS = 300000;
// How long a route that was caught dropping a passthrough stream stays buffered. The verdict comes
// from a single answer, and the upstream can be fixed or can have had a bad minute, so it must
// expire. It also never removes the route from the failover pool: it only says "ask this one for a
// complete answer". A permanent verdict that also dropped the route is what once left nothing but
// the blocked direct path in the pool, after which every request died with ECONNRESET.
const PASSTHROUGH_BROKEN_MS = ROUTE_COOLDOWN_MAX_MS;
// Budget for the replay attempt that follows a failed passthrough. The response has already been
// held back and the client is still waiting, so this is a rescue with what is left of the turn,
// not a second full failover walk.
const REPLAY_FALLBACK_BUDGET_MS = 60000;

// How the route list is ordered. "direct-first" is faster and needs no proxy; "proxy-first" is for
// a machine where the direct path is blocked (Cloudflare challenge, ISP filter, region block) -
// there every direct attempt costs 10-20 s before the tunnel is even tried.
const ROUTE_ORDERS = ['direct-first', 'proxy-first'];
const DEFAULT_ROUTE_ORDER = 'direct-first';

function parseArgs(argv) {
	// port and stream stay null until the config file has been consulted, otherwise an explicit
	// `--port 8765` would be indistinguishable from the built-in default.
	const opts = { port: null, stream: null, verbose: false };
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === '--port' || a === '-p') opts.port = argv[++i];
		else if (a === '--stream' || a === '-s') opts.stream = argv[++i];
		else if (a === '--host' || a === '-b') opts.host = argv[++i];
		else if (a === '--host-reserved' || a === '-B') opts.hostReserved = argv[++i];
		else if (a === '--key' || a === '-k') opts.key = argv[++i];
		else if (a === '--model' || a === '-m') opts.model = argv[++i];
		else if (a === '--conf') opts.conf = argv[++i];
		else if (a === '--log') opts.log = argv[++i];
		else if (a === '--proxy') opts.proxy = argv[++i];
		else if (a === '--route-order' || a === '-r') opts.routeOrder = argv[++i];
		else if (a === '--timeout' || a === '-t') opts.timeout = argv[++i];
		else if (a === '--budget') opts.budget = argv[++i];
		else if (a === '--probe') opts.probe = argv[++i];
		else if (a === '--verbose' || a === '-v') opts.verbose = true;
		else if (a === '--help' || a === '-h') opts.help = true;
	}
	return opts;
}

// Everything diagnostic goes to the log file (when --log is given); the console only sees it with
// --verbose. This keeps the hidden launcher window quiet while still leaving a trace on disk.
let fileSink = () => {};

function formatArgs(args) {
	return args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
}

function log(opts, ...args) {
	const line = formatArgs(args);
	if (opts.verbose) console.log(line);
	fileSink(line);
}

function announce(line) {
	console.log(line);
	fileSink(line);
}

function openLogFile(file) {
	if (!file) return () => {};
	try {
		const stream = fs.createWriteStream(file, { flags: 'a' });
		return (line) => {
			try {
				stream.write(new Date().toISOString() + ' ' + line + '\n');
			} catch {
				/* logging must never break the relay */
			}
		};
	} catch {
		return () => {};
	}
}

function readConf(confPath) {
	const values = {};
	if (!confPath || !fs.existsSync(confPath)) return values;
	for (const line of fs.readFileSync(confPath, 'utf8').split(/\r?\n/)) {
		const entry = line.trim();
		if (!entry || entry.startsWith('#') || entry.startsWith(';')) continue;
		const sep = entry.indexOf('=');
		if (sep < 1) continue;
		values[entry.slice(0, sep).trim()] = entry.slice(sep + 1).trim();
	}
	return values;
}

// Port precedence: --port, then conf.txt `port`, then 8765. 0 is valid and means "let the OS pick"
// - that is how several relays live side by side without a fixed numbering plan. A value that is
// not a port is a configuration mistake, so it exits with the same code as a missing host/key (2)
// rather than being silently coerced into the busy-port path (3).
function resolvePort(cliPort, confPort) {
	const raw = cliPort !== null && cliPort !== undefined ? cliPort : confPort;
	if (raw === undefined || raw === null || raw === '') return DEFAULT_PORT;
	const port = Number(raw);
	if (!Number.isInteger(port) || port < 0 || port > 65535) {
		console.error('router-relay: invalid port "' + raw + '" (expected 0-65535, 0 = pick a free one)');
		process.exit(2);
	}
	return port;
}

// Streaming precedence is the same as for the port. An unknown value is a configuration mistake:
// silently falling back to the default would hide a typo that changes the behaviour of every
// single request.
function resolveStreamMode(cliMode, confMode) {
	const raw = cliMode !== null && cliMode !== undefined ? cliMode : confMode;
	if (raw === undefined || raw === null || raw === '') return DEFAULT_STREAM_MODE;
	const mode = String(raw).trim().toLowerCase();
	if (!STREAM_MODES.includes(mode)) {
		console.error('router-relay: invalid stream mode "' + raw + '" (expected one of: ' + STREAM_MODES.join(', ') + ')');
		process.exit(2);
	}
	return mode;
}

// Route order precedence follows the port and the stream mode: command line, then conf.txt, then
// the built-in default. An unknown value is a configuration mistake and exits, exactly like a bad
// stream mode - silently ignoring it would change the behaviour of every request.
function resolveRouteOrder(cliOrder, confOrder) {
	const raw = cliOrder !== null && cliOrder !== undefined ? cliOrder : confOrder;
	if (raw === undefined || raw === null || raw === '') return DEFAULT_ROUTE_ORDER;
	const order = String(raw).trim().toLowerCase();
	if (!ROUTE_ORDERS.includes(order)) {
		console.error('router-relay: invalid route_order "' + raw + '" (expected one of: ' + ROUTE_ORDERS.join(', ') + ')');
		process.exit(2);
	}
	return order;
}

// Timeouts are written in seconds in conf.txt and used in milliseconds. A non-numeric value is a
// typo that would otherwise turn into a mystery hang, so it exits like a missing host.
function resolveSeconds(cliValue, confValue, fallbackMs, name) {
	const raw = cliValue !== null && cliValue !== undefined ? cliValue : confValue;
	if (raw === undefined || raw === null || raw === '') return fallbackMs;
	const seconds = Number(raw);
	if (!Number.isFinite(seconds) || seconds <= 0) {
		console.error('router-relay: invalid ' + name + ' "' + raw + '" (expected a positive number of seconds)');
		process.exit(2);
	}
	return Math.round(seconds * 1000);
}

function resolveProbe(cliValue, confValue) {
	const raw = cliValue !== null && cliValue !== undefined ? cliValue : confValue;
	if (raw === undefined || raw === null || raw === '') return true;
	const value = String(raw).trim().toLowerCase();
	if (value === 'on' || value === 'true' || value === '1') return true;
	if (value === 'off' || value === 'false' || value === '0') return false;
	console.error('router-relay: invalid probe "' + raw + '" (expected on or off)');
	process.exit(2);
}

function sendJson(res, status, obj) {
	const body = Buffer.from(JSON.stringify(obj), 'utf8');
	res.writeHead(status, { 'content-type': 'application/json', 'content-length': body.length });
	res.end(body);
}

function readBody(req) {
	return new Promise((resolve, reject) => {
		const chunks = [];
		req.on('data', (c) => chunks.push(c));
		req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
		req.on('error', reject);
	});
}

// --- transport ------------------------------------------------------------------------------
// The upstream is reached with Node's own HTTP client, either directly or through a local HTTP
// proxy via CONNECT. A native client replaced the original "one curl process per request"
// workaround, which had three costs that all surfaced as "the relay barely works":
//
//   * it buffered every body, so SSE passthrough was impossible through the proxy - the only
//     route that works when the region is blocked - and every answer arrived at once;
//   * it paid a process spawn and a fresh TLS handshake per request (~0.3-1 s on every turn);
//   * on Windows it used Schannel, which intermittently failed inside the HAPP tunnel with
//     "SEC_E_DECRYPT_FAILURE" / "missing close_notify" - a phantom failure on a healthy route.
//
// Keep-alive agents make both paths behave the same, so stream=on and stream=auto now really do
// stream through the proxy. Only HTTPS upstreams are tunnelled; an http:// base_url with proxy= is
// rejected at startup instead of being silently sent in the clear.
const agents = new Map();
const directAgent = new https.Agent({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });
// A plain agent for an http:// upstream (a local test server, a self-hosted gateway). Using the
// https.Agent there fails with ERR_INVALID_PROTOCOL, which used to look like a route failure.
const plainAgent = new http.Agent({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });

// An https.Agent whose sockets are opened with the proxy's CONNECT method and then wrapped in TLS.
// Keep-alive lives in the base class, so one tunnel is reused across turns exactly like a direct
// connection would be.
class ProxyAgent extends https.Agent {
	constructor(proxyUrl) {
		super({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });
		this.proxy = new URL(proxyUrl);
		this.auth = null;
		if (this.proxy.username) {
			const credentials = decodeURIComponent(this.proxy.username) + ':' + decodeURIComponent(this.proxy.password || '');
			this.auth = 'Basic ' + Buffer.from(credentials, 'utf8').toString('base64');
		}
	}

	// The tunnel target is part of the pool key: one proxy serves several upstream hosts.
	getName(options) {
		return 'proxy:' + this.proxy.host + '|' + super.getName(options);
	}

	createConnection(options, cb) {
		const host = options.host || options.servername;
		const port = Number(options.port) || 443;
		const authority = host + ':' + port;
		const headers = { host: authority };
		if (this.auth) headers['proxy-authorization'] = this.auth;

		const library = this.proxy.protocol === 'https:' ? https : http;
		const connectReq = library.request({
			host: this.proxy.hostname,
			port: Number(this.proxy.port) || (this.proxy.protocol === 'https:' ? 443 : 80),
			method: 'CONNECT',
			path: authority,
			headers,
			agent: false,
		});

		let settled = false;
		const fail = (err) => {
			if (settled) return;
			settled = true;
			connectReq.destroy();
			cb(err);
		};

		connectReq.setTimeout(CONNECT_TIMEOUT_MS, () => fail(new Error('proxy CONNECT to ' + authority + ' timed out')));
		connectReq.once('error', fail);
		connectReq.once('connect', (response, socket, head) => {
			if (settled) {
				socket.destroy();
				return;
			}
			if (response.statusCode !== 200) {
				socket.destroy();
				fail(new Error('proxy CONNECT to ' + authority + ' failed with ' + response.statusCode));
				return;
			}
			settled = true;
			// Bytes the proxy sent right after its 200 line belong to the tunnelled stream.
			if (head && head.length) socket.unshift(head);
			cb(null, tls.connect({
				socket,
				servername: options.servername || host,
				rejectUnauthorized: options.rejectUnauthorized !== false,
			}));
		});
		connectReq.end();
	}
}

function agentFor(route, secure) {
	if (route.proxy) {
		let agent = agents.get(route.proxy);
		if (!agent) {
			agent = new ProxyAgent(route.proxy);
			agents.set(route.proxy, agent);
		}
		return agent;
	}
	return secure ? directAgent : plainAgent;
}

// One request on one route, buffered or streaming. The streaming variant resolves as soon as the
// status line and headers are in, which is what lets the relay decide to fail over *before* a byte
// reaches the client; the buffered variant resolves once the body has been read to the end.
function requestOverHttp(opts, route, pathname, options) {
	const url = new URL(route.host.replace(/\/+$/, '') + pathname);
	const secure = url.protocol !== 'http:';
	const library = secure ? https : http;
	const headers = { ...(options.headers || {}) };
	if (options.body !== undefined) headers['content-length'] = Buffer.byteLength(options.body);
	if (options.stream) headers.accept = 'text/event-stream';

	return new Promise((resolve, reject) => {
		let settled = false;
		const settle = (fn, value) => {
			if (settled) return false;
			settled = true;
			fn(value);
			return true;
		};

		const req = library.request({
			host: url.hostname,
			port: url.port || (secure ? 443 : 80),
			path: url.pathname + (url.search || ''),
			method: options.method || (options.stream ? 'POST' : 'GET'),
			headers,
			agent: agentFor(route, secure),
		});

		const timeoutMs = options.timeoutMs || REQUEST_TIMEOUT_MS;
		const timeoutError = () => new Error('upstream timed out after ' + Math.round(timeoutMs / 1000) + 's on ' + describeRoute(route));
		// req.setTimeout only starts once a socket is assigned, so it does not cover a TCP connect
		// that never completes (a non-routable address hangs for ~21 s before the OS gives up). This
		// hard deadline is what makes `budget` an actual guarantee.
		const deadline = setTimeout(() => req.destroy(timeoutError()), timeoutMs);
		if (typeof deadline.unref === 'function') deadline.unref();
		const stopDeadline = () => clearTimeout(deadline);

		req.setTimeout(timeoutMs, () => req.destroy(timeoutError()));
		req.on('error', (err) => {
			stopDeadline();
			// A keep-alive socket the edge closed while it sat idle fails the *next* request with
			// ECONNRESET before a single response byte arrives, and Node does not retry it - so a
			// healthy route looks dead for one attempt. The connection worked a moment ago, so a
			// retry on a fresh socket is safe. Only a reused socket qualifies: on a fresh one the
			// reset is real and retrying would just repeat it.
			if (!settled && req.reusedSocket && err && err.code === 'ECONNRESET' && !options.staleSocketRetried) {
				log(opts, 'stale keep-alive socket reset, retrying once on a fresh socket:', describeRoute(route));
				settle(resolve, requestOverHttp(opts, route, pathname, { ...options, staleSocketRetried: true }));
				return;
			}
			// Carry the route on the error: without it the client only ever saw "relay: read
			// ECONNRESET", which reads like a fault in the relay instead of a reset on one path.
			if (err && typeof err === 'object' && !err.route) err.route = describeRoute(route);
			settle(reject, err);
		});

		// A vanished client must stop the upstream request too: a cheap router still bills for
		// tokens nobody reads, and the old relay happily finished answers Cline had abandoned.
		if (options.signal) {
			const onAbort = () => req.destroy(new Error('client disconnected'));
			if (options.signal.aborted) onAbort();
			else options.signal.addEventListener('abort', onAbort, { once: true });
		}

		req.on('response', (response) => {
			const contentType = response.headers['content-type'] || 'application/json';
			// Headers are in: the connect phase is over. From here the body gets its own idle
			// timeout, so a slow but live stream is not cut off by the attempt deadline.
			stopDeadline();
			if (options.stream) {
				response.setTimeout(timeoutMs, () => response.destroy(timeoutError()));
				settle(resolve, {
					ok: response.statusCode >= 200 && response.statusCode < 300,
					status: response.statusCode,
					contentType,
					body: response,
					route: describeRoute(route),
				});
				return;
			}

			const chunks = [];
			response.on('data', (chunk) => chunks.push(chunk));
			response.on('error', (err) => settle(reject, err));
			response.on('end', () => {
				settle(resolve, {
					ok: response.statusCode >= 200 && response.statusCode < 300,
					status: response.statusCode,
					contentType,
					text: Buffer.concat(chunks).toString('utf8'),
					route: describeRoute(route),
				});
			});
		});

		if (options.body !== undefined) req.end(options.body);
		else req.end();
	});
}

function requestOnce(opts, route, pathname, options) {
	return requestOverHttp(opts, route, pathname, { ...options, stream: false });
}

// Streaming sibling of requestOnce: it resolves with the live response object instead of a body
// that has already been read.
function requestStreamOnce(opts, route, pathname, options) {
	return requestOverHttp(opts, route, pathname, { ...options, stream: true });
}


// Ordered failover routes. Direct comes first by default: it is the fastest path (no tunnel -
// roughly 80 ms of TLS against ~300 ms through the proxy) and it has no dependency on the local
// proxy being up. The proxy follows immediately, because when Cloudflare blocks the region every
// direct host fails together - both hostnames are the same zone, so trying the reserved host direct
// before the proxy only adds a doomed attempt. The reserved host is still walked, for the case where
// a single hostname is having trouble rather than the whole zone.
//
// `route_order=proxy-first` reverses that within each host. It exists because "direct is faster"
// stops being true the moment the direct path is blocked: on such a machine every direct attempt
// costs 10-20 s before the tunnel is even tried, and the tunnel is the only thing that works.
function buildRoutes(opts) {
	const hosts = [];
	if (opts.host) hosts.push(opts.host);
	if (opts.hostReserved && !hosts.includes(opts.hostReserved)) hosts.push(opts.hostReserved);
	const proxies = [''];
	if (opts.proxy) proxies.push(opts.proxy);
	if (opts.proxy && opts.routeOrder === 'proxy-first') proxies.reverse();
	const routes = [];
	for (const host of hosts) {
		for (const proxy of proxies) routes.push({ host, proxy });
	}
	return routes;
}

function describeRoute(route) {
	return route.host + (route.proxy ? ' via ' + route.proxy : ' direct');
}

// Statuses worth trying another route for: a Cloudflare block/challenge (403), a request timeout
// (408), rate limiting (429), and any edge or upstream 5xx (503, 520-527, ...). A 400/401/404 is
// the caller's own problem and reproduces identically on every route, so it is returned at once
// instead of burning the failover budget on it.
function isRetryableStatus(status) {
	return status === 403 || status === 408 || status === 429 || status >= 500;
}





// Routes in the order they should be attempted: healthy ones first (in configured order), then any
// that are cooling down. A cooling route is never dropped - if the healthy ones also fail, the
// cooling ones are still walked, so a request is never abandoned without trying everything.
function routeOrder(opts) {
	const routes = opts.routes && opts.routes.length ? opts.routes : buildRoutes(opts);
	if (!opts.routeCooldown || opts.routeCooldown.size === 0) return routes;
	const now = Date.now();
	const healthy = [];
	const cooling = [];
	for (const route of routes) {
		const entry = opts.routeCooldown.get(describeRoute(route));
		(entry && entry.until > now ? cooling : healthy).push(route);
	}
	return healthy.length ? healthy.concat(cooling) : routes;
}

// Each consecutive failure doubles the wait, so a persistently blocked route is probed rarely while
// a route that recovers is picked up again on the next probe.
function coolDownRoute(opts, route) {
	if (!opts.routeCooldown) opts.routeCooldown = new Map();
	const key = describeRoute(route);
	const entry = opts.routeCooldown.get(key) || { strikes: 0 };
	entry.strikes += 1;
	const wait = Math.min(ROUTE_COOLDOWN_MS * Math.pow(2, entry.strikes - 1), ROUTE_COOLDOWN_MAX_MS);
	entry.until = Date.now() + wait;
	opts.routeCooldown.set(key, entry);
	return { key, wait };
}

function clearCooldown(opts, route) {
	if (opts.routeCooldown) opts.routeCooldown.delete(describeRoute(route));
}

// Routes that were caught dropping a passthrough stream, each with the moment its verdict expires.
// The entry is a hint for the passthrough attempt, not a health verdict: requestUpstream still
// walks the route, it just asks it for a complete answer.
function markPassthroughBroken(opts, route) {
	if (!route) return;
	opts.passthroughBroken = opts.passthroughBroken || new Map();
	opts.passthroughBroken.set(route, Date.now() + PASSTHROUGH_BROKEN_MS);
}

function clearPassthroughBroken(opts, route) {
	if (opts.passthroughBroken) opts.passthroughBroken.delete(route);
}

// Expiry is checked on read, so a stale verdict disappears without a timer and /health never
// reports a route that has already been forgiven.
function isPassthroughBroken(opts, route) {
	if (!opts.passthroughBroken) return false;
	const until = opts.passthroughBroken.get(route);
	if (until === undefined) return false;
	if (until > Date.now()) return true;
	opts.passthroughBroken.delete(route);
	return false;
}
// Transports hide the useful half of a failure in `cause`; the old log only ever said "fetch
// failed", which is why a permanently blocked direct route looked like an unexplained mystery.
function describeError(err) {
	if (!err) return 'unknown error';
	let text = err.message || String(err);
	let cause = err.cause;
	for (let depth = 0; cause && depth < 3; depth++) {
		const part = cause.code || cause.message || String(cause);
		if (part && !text.includes(String(part))) text += ' (' + part + ')';
		cause = cause.cause;
	}
	return text;
}

// One line for the client, carrying the two facts needed to act on it: what failed and where.
function describeFailure(err) {
	const message = err && err.message ? err.message : String(err);
	return err && err.route ? message + ' on ' + err.route : message;
}

// How long the next attempt may take: never longer than one attempt, never past the request
// deadline. 0 means the budget is spent and the caller should stop walking routes - that is what
// keeps the relay inside the client's own timeout instead of retrying long after it gave up.
function attemptTimeout(opts, deadline) {
	const remaining = deadline - Date.now();
	if (remaining < 1000) return 0;
	return Math.min(opts.timeoutMs || REQUEST_TIMEOUT_MS, remaining);
}



async function requestUpstream(opts, pathname, options) {
	// transport + body read + (by the caller) JSON.parse are one retryable unit: the upstream router
	// sometimes drops the connection while the body is still being read, which used to bubble up as
	// an unhandled rejection and kill the process.
	const routes = routeOrder(opts);
	const budgetMs = options.budgetMs || opts.budgetMs || REQUEST_BUDGET_MS;
	const deadline = Date.now() + budgetMs;
	let lastErr = null;
	let lastResponse = null;

	for (let pass = 0; pass < ROUTE_PASSES; pass++) {
		let spent = false;
		for (const route of routes) {
			const timeoutMs = attemptTimeout(opts, deadline);
			if (timeoutMs === 0) {
				spent = true;
				break;
			}

			let result;
			try {
				result = await requestOnce(opts, route, pathname, { ...options, timeoutMs });
			} catch (err) {
				lastErr = err;
				// A client that hung up is not a sick route: stop walking the list instead of
				// cooling down every route and burning the rest of the budget for nobody.
				if (options.signal && options.signal.aborted) throw err;
				const cool = coolDownRoute(opts, route);
				log(opts, 'route failed (pass ' + (pass + 1) + '/' + ROUTE_PASSES + '), cooling ' + Math.round(cool.wait / 1000) + 's (strike ' + opts.routeCooldown.get(cool.key).strikes + '):', cool.key, '-', describeError(err));
				continue;
			}
			result.route = describeRoute(route);

			if (isRetryableStatus(result.status)) {
				lastResponse = result;
				const cool = coolDownRoute(opts, route);
				log(opts, 'route gave retryable status (pass ' + (pass + 1) + '/' + ROUTE_PASSES + '), cooling ' + Math.round(cool.wait / 1000) + 's (strike ' + opts.routeCooldown.get(cool.key).strikes + '):', cool.key, '-', result.status);
				continue;
			}
			clearCooldown(opts, route);
			if (pass > 0 || route !== routes[0]) {
				log(opts, 'route recovered:', describeRoute(route), '-', result.status);
			}
			return result;
		}
		if (spent) {
			lastErr = lastErr || new Error('request budget of ' + Math.round(budgetMs / 1000) + 's exhausted before the answer arrived');
			break;
		}
	}

	// Every route produced the same edge/upstream error: hand the last one back so the client sees
	// the real status instead of a relay-invented failure.
	if (lastResponse) return lastResponse;
	throw lastErr || new Error('relay: every route failed');
}
// Streaming sibling of requestUpstream. Same route walk, same cooldown bookkeeping, same failover
// triggers - the single difference is that a successful attempt returns an open byte stream instead
// of a body that has already been read. Nothing is written to the client here: the caller owns the
// response, so a later failure can still be answered with a clean JSON error.
async function requestUpstreamStream(opts, pathname, options) {
	let routes = routeOrder(opts);
	// In auto mode a route that was caught dropping the stream is asked for a complete answer
	// instead. The route stays in the failover list on purpose: dropping it from the pool is what
	// once left nothing but a blocked direct path, so every request failed with ECONNRESET while a
	// perfectly good proxy route sat unused. If every route is in that state, this request simply
	// takes the replay path - the caller sees streamUnsupported and falls back.
	if (opts.streamMode === 'auto' && opts.passthroughBroken && opts.passthroughBroken.size > 0) {
		const usable = routes.filter((route) => !isPassthroughBroken(opts, describeRoute(route)));
		if (usable.length === 0) {
			const err = new Error('every route is currently buffered - the replay path answers this one');
			err.streamUnsupported = true;
			throw err;
		}
		routes = usable;
	}
	let lastErr = null;
	let lastResponse = null;
	const budgetMs = options.budgetMs || opts.budgetMs || REQUEST_BUDGET_MS;
	const deadline = Date.now() + budgetMs;

	for (let pass = 0; pass < ROUTE_PASSES; pass++) {
		let spent = false;
		for (const route of routes) {
			const timeoutMs = attemptTimeout(opts, deadline);
			if (timeoutMs === 0) {
				spent = true;
				break;
			}

			let result;
			try {
				result = await requestStreamOnce(opts, route, pathname, { ...options, timeoutMs });
			} catch (err) {
				lastErr = err;
				// A client that hung up is not a sick route: stop walking the list instead of
				// cooling down every route and burning the rest of the budget for nobody.
				if (options.signal && options.signal.aborted) throw err;
				// A route that simply cannot do passthrough is not sick, so it must not be punished
				// with a cooldown - that would only make the fallback slower on every request.
				if (err && err.streamUnsupported) {
					log(opts, 'route skipped for streaming:', describeRoute(route), '-', err.message);
					continue;
				}
				const cool = coolDownRoute(opts, route);
				log(opts, 'stream route failed (pass ' + (pass + 1) + '/' + ROUTE_PASSES + '), cooling ' + Math.round(cool.wait / 1000) + 's (strike ' + opts.routeCooldown.get(cool.key).strikes + '):', cool.key, '-', describeError(err));
				continue;
			}
			result.route = describeRoute(route);

			if (isRetryableStatus(result.status)) {
				lastResponse = result;
				// The body is an open socket: dropping the reference without destroying it would
				// leak the tunnel and keep the upstream generating tokens nobody reads.
				try {
					result.body.destroy();
				} catch {
					/* already gone */
				}
				const cool = coolDownRoute(opts, route);
				log(opts, 'stream route gave retryable status (pass ' + (pass + 1) + '/' + ROUTE_PASSES + '), cooling ' + Math.round(cool.wait / 1000) + 's (strike ' + opts.routeCooldown.get(cool.key).strikes + '):', cool.key, '-', result.status);
				continue;
			}
			clearCooldown(opts, route);
			if (pass > 0 || route !== routes[0]) {
				log(opts, 'stream route recovered:', describeRoute(route), '-', result.status);
			}
			return result;
		}
		if (spent) {
			lastErr = lastErr || new Error('request budget of ' + Math.round(budgetMs / 1000) + 's exhausted before the answer arrived');
			break;
		}
	}

	// The last response is only returned when it is a real upstream answer; its body has already
	// been destroyed, so the caller sees the status and mirrors the error text itself.
	if (lastResponse) return { ...lastResponse, text: lastResponse.text || '' };
	throw lastErr || new Error('relay: every route failed');
}

// Sends a JSON error response if that is still possible, otherwise closes whatever is open.
function failSafely(opts, res, status, obj) {
	if (res.headersSent) {
		if (!res.writableEnded && !res.destroyed) {
			try {
				res.end();
			} catch (err) {
				log(opts, 'failed to close response:', err && err.message ? err.message : String(err));
			}
		}
		return;
	}
	try {
		sendJson(res, status, obj);
	} catch (err) {
		log(opts, 'failed to send error response:', err && err.message ? err.message : String(err));
		try {
			res.destroy();
		} catch {
			/* nothing else to do */
		}
	}
}

// Mirrors a raw upstream body (used when the upstream answers with a non-2xx status).
function mirrorSafely(opts, res, status, contentType, text) {
	if (res.headersSent) return;
	try {
		res.writeHead(status, { 'content-type': contentType });
		res.end(text);
	} catch (err) {
		log(opts, 'failed to mirror upstream body:', err && err.message ? err.message : String(err));
	}
}

// Keeps an async handler's rejection from becoming an unhandledRejection (which kills Node >=15).
function run(opts, res, promise) {
	Promise.resolve(promise).catch((err) => {
		const message = err && err.message ? err.message : String(err);
		log(opts, 'handler rejected:', message);
		failSafely(opts, res, 502, { error: { message: 'relay: ' + message, type: 'api_error' } });
	});
}

function writeChunk(opts, res, text) {
	if (res.writableEnded || res.destroyed) return;
	try {
		res.write(text);
	} catch (err) {
		log(opts, 'write to client failed:', err && err.message ? err.message : String(err));
	}
}

function sseChunk(opts, res, payload) {
	writeChunk(opts, res, 'data: ' + JSON.stringify(payload) + '\n\n');
}

function chunkEnvelope(upstream, model) {
	return {
		id: upstream.id || 'chatcmpl-relay',
		object: 'chat.completion.chunk',
		created: upstream.created || Math.floor(Date.now() / 1000),
		model: upstream.model || model,
	};
}

// Splits text into small pieces so the client still sees incremental output.
function splitText(text, size) {
	const parts = [];
	for (let i = 0; i < text.length; i += size) parts.push(text.slice(i, i + size));
	return parts.length ? parts : [''];
}

function replayAsStream(opts, res, upstream, model) {
	res.writeHead(200, {
		'content-type': 'text/event-stream',
		'cache-control': 'no-cache, no-transform',
		connection: 'keep-alive',
		'x-accel-buffering': 'no',
	});

	const choice = (upstream.choices && upstream.choices[0]) || {};
	const message = choice.message || {};

	sseChunk(opts, res, { ...chunkEnvelope(upstream, model), choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }] });

	if (typeof message.reasoning_content === 'string' && message.reasoning_content.length > 0) {
		for (const part of splitText(message.reasoning_content, 240)) {
			sseChunk(opts, res, { ...chunkEnvelope(upstream, model), choices: [{ index: 0, delta: { reasoning_content: part }, finish_reason: null }] });
		}
	}

	if (typeof message.content === 'string' && message.content.length > 0) {
		for (const part of splitText(message.content, 240)) {
			sseChunk(opts, res, { ...chunkEnvelope(upstream, model), choices: [{ index: 0, delta: { content: part }, finish_reason: null }] });
		}
	}

	if (Array.isArray(message.tool_calls) && message.tool_calls.length > 0) {
		message.tool_calls.forEach((call, index) => {
			sseChunk(opts, res, {
				...chunkEnvelope(upstream, model),
				choices: [{
					index: 0,
					delta: {
						tool_calls: [{
							index,
							id: call.id || 'call_' + index,
							type: call.type || 'function',
							function: {
								name: (call.function && call.function.name) || '',
								arguments: (call.function && call.function.arguments) || '',
							},
						}],
					},
					finish_reason: null,
				}],
			});
		});
	}

	const finishReason = choice.finish_reason || (Array.isArray(message.tool_calls) && message.tool_calls.length ? 'tool_calls' : 'stop');
	const finalChunk = { ...chunkEnvelope(upstream, model), choices: [{ index: 0, delta: {}, finish_reason: finishReason }] };
	if (upstream.usage) finalChunk.usage = upstream.usage;
	sseChunk(opts, res, finalChunk);

	writeChunk(opts, res, 'data: [DONE]\n\n');
	if (!res.writableEnded && !res.destroyed) {
		res.end();
	}
}
// Upstream bodies arrive as Node streams now (they used to be web streams from fetch). Both shapes
// are accepted so a future transport change does not have to touch every consumer.
function nodeStream(body) {
	if (body && typeof body.pipe === 'function') return body;
	return Readable.fromWeb(body);
}

// Drains a body into a string, tolerating a stream that dies halfway: the status code is what the
// caller needs, and a partial error body is still more useful than nothing.
function readStream(body) {
	const source = nodeStream(body);
	return new Promise((resolve) => {
		const chunks = [];
		const finish = () => resolve(Buffer.concat(chunks).toString('utf8'));
		source.on('data', (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
		source.on('error', finish);
		source.on('end', finish);
	});
}

// Copies an upstream SSE stream to the client as it arrives. The client gets exactly what the
// upstream sent - no re-serialisation, no chunk sizing games - which is only safe when the upstream
// terminates its stream properly. That is what the `stream` setting is for: `off` never takes this
// path, `on` always trusts it, `auto` watches it and reports whether it ended cleanly.
//
// The promise resolves with { terminated } instead of throwing, because by the time the first byte
// is out the client already has a 200 and headers; there is nothing left to fail over to. The
// caller decides what a broken stream means.
function pipeStreamToClient(opts, res, upstream, model) {
	return new Promise((resolve) => {
		const chunks = [];
		let tail = '';
		let terminated = false;
		let bytes = 0;
		// The head is held back until the answer actually starts, so a policy notice can still be
		// turned into an error: once a 200 and its headers are out, nothing can be taken back.
		let head = '';
		let started = false;

		const source = nodeStream(upstream.body);

		const startResponse = () => {
			if (started) return;
			started = true;
			res.writeHead(200, {
				'content-type': 'text/event-stream',
				'cache-control': 'no-cache, no-transform',
				connection: 'keep-alive',
				'x-accel-buffering': 'no',
			});
			if (typeof res.flushHeaders === 'function') res.flushHeaders();
			if (head) {
				try {
					res.write(head);
				} catch (err) {
					log(opts, 'write to client failed:', err && err.message ? err.message : String(err));
				}
			}
		};

		// The client hanging up must also stop reading from the upstream: otherwise the tunnel
		// stays open and the router keeps generating (and billing) tokens nobody will see.
		res.on('close', () => {
			if (!res.writableEnded && !source.destroyed) {
				log(opts, 'client closed the stream early - aborting upstream', { bytes });
				source.destroy();
			}
		});

		source.on('data', (chunk) => {
			const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
			bytes += buf.length;
			chunks.push(buf);
			// Only the tail is decoded: `[DONE]` is always the last event, and decoding the whole
			// answer just to look for it would defeat the point of streaming.
			tail = (tail + buf.toString('utf8')).slice(-PASSTHROUGH_TAIL_CHARS);

			if (!started) {
				head += buf.toString('utf8');
				if (isPolicyBlock(head)) {
					source.destroy();
					resolve({ terminated: false, bytes, started: false, policyBlock: true });
					return;
				}
				if (!headHasAnswer(head) && head.length < POLICY_HEAD_MAX) return;
				startResponse();
				return;
			}

			if (res.writableEnded || res.destroyed) return;
			try {
				res.write(buf);
			} catch (err) {
				log(opts, 'write to client failed:', err && err.message ? err.message : String(err));
			}
		});

		source.on('error', (err) => {
			log(opts, 'stream source error:', err && err.message ? err.message : String(err));
			if (!started) {
				resolve({ terminated: false, bytes, started: false });
				return;
			}
			if (!res.writableEnded && !res.destroyed) {
				try {
					res.end();
				} catch {
					/* the client is gone */
				}
			}
			resolve({ terminated: false, bytes, started: true });
		});

		source.on('end', () => {
			terminated = tail.includes('[DONE]');
			// A stream that ended before a single delta arrived still has to be closed properly -
			// unless its content was the policy notice, which the caller turns into an error.
			if (!started) {
				if (isPolicyBlock(tail)) {
					resolve({ terminated: false, bytes, started: false, policyBlock: true });
					return;
				}
				startResponse();
			}
			const body = Buffer.concat(chunks).toString('utf8');
			const finish = /"finish_reason"\s*:\s*"([^"]+)"/.exec(body);
			log(opts, 'upstream stream done', {
				bytes,
				terminated,
				finish: finish ? finish[1] : null,
				model,
			});
			if (!res.writableEnded && !res.destroyed) {
				try {
					res.end();
				} catch (err) {
					log(opts, 'failed to close stream:', err && err.message ? err.message : String(err));
				}
			}
			resolve({ terminated, bytes, started: true });
		});
	});
}

// Answers a client that asked for a non-streaming completion with a plain JSON body. The upstream
// is always called with "stream": false, so its parsed body is already a complete chat.completion
// (choices[0].message, finish_reason, usage) - it only has to be re-serialised, with `object`
// forced to "chat.completion" in case the upstream labelled it as a chunk.
function replayAsJson(opts, res, upstream, model) {
	if (res.headersSent) return;
	const body = {
		...upstream,
		id: upstream.id || 'chatcmpl-relay',
		object: 'chat.completion',
		created: upstream.created || Math.floor(Date.now() / 1000),
		model: upstream.model || model,
	};
	let buf;
	try {
		buf = Buffer.from(JSON.stringify(body), 'utf8');
	} catch (err) {
		failSafely(opts, res, 502, { error: { message: 'relay: upstream body is not serialisable: ' + (err && err.message ? err.message : String(err)), type: 'api_error' } });
		return;
	}
	try {
		res.writeHead(200, { 'content-type': 'application/json', 'content-length': buf.length });
		res.end(buf);
	} catch (err) {
		log(opts, 'write to client failed:', err && err.message ? err.message : String(err));
	}
}

// Some routers answer a request they do not want with 200 and a policy notice in `content`
// instead of a completion: the notice this project was debugged against says, in Chinese, that the
// site only serves mainstream coding clients and that the current one is not supported. Replaying
// it makes the client believe the model said it, and the text itself is of no use to whoever reads
// the answer. So it is detected and turned into a short error - the notice is never reproduced,
// neither in the response nor in the log.
//
// The markers are written as escapes on purpose: the source stays ASCII, and nobody has to read
// the notice in the code.
const POLICY_BLOCK_MARKERS = ['\u672c\u7ad9\u4ec5\u9650', '\u5f53\u524d\u5ba2\u6237\u7aef\u4e0d\u652f\u6301'];
const POLICY_BLOCK_MESSAGE =
	'the router refused this request: it only accepts clients it recognises as a mainstream coding ' +
	'CLI, so it answered with a notice instead of a completion. Retry from a supported client or ' +
	'check the account plan.';
// How much of a passthrough stream is held back while waiting for the answer to start. The notice
// arrives as the first content delta, so this is only a safety valve for a stream that never does.
const POLICY_HEAD_MAX = 8192;

function isPolicyBlock(text) {
	if (typeof text !== 'string' || text.length === 0) return false;
	return POLICY_BLOCK_MARKERS.some((marker) => text.includes(marker));
}

// Reads the `content` of a complete chat completion, or '' when there is none.
function completionText(json) {
	const choice = json && Array.isArray(json.choices) ? json.choices[0] : null;
	const message = choice ? choice.message : null;
	return message && typeof message.content === 'string' ? message.content : '';
}

// True once the buffered SSE head holds a delta the client would actually see. Role-only,
// usage-only and `[DONE]` events do not count: the answer has not started yet, so the head can
// still be held back without the client noticing a delay.
function headHasAnswer(head) {
	for (const line of head.split('\n')) {
		if (!line.startsWith('data: ')) continue;
		const raw = line.slice(6).trim();
		if (!raw || raw === '[DONE]') continue;
		let parsed;
		try {
			parsed = JSON.parse(raw);
		} catch {
			continue; // a chunk boundary split this line; the next chunk completes it
		}
		const choice = parsed && Array.isArray(parsed.choices) ? parsed.choices[0] : null;
		const delta = choice ? choice.delta : null;
		if (!delta) continue;
		if (typeof delta.content === 'string' && delta.content.length > 0) return true;
		if (typeof delta.reasoning_content === 'string' && delta.reasoning_content.length > 0) return true;
	}
	return false;
}

async function handleChat(opts, req, res) {
	try {
		let payload;
		try {
			payload = JSON.parse(await readBody(req));
		} catch {
			failSafely(opts, res, 400, { error: { message: 'relay: request body is not valid JSON', type: 'invalid_request_error' } });
			return;
		}

		// Remember what the client asked for BEFORE forcing the upstream to non-streaming. Only an
		// explicit "stream": true means SSE: an [OI]-compatible client that omits the field is asking
		// for the API default (non-streaming) and parses the body as one JSON object. Hermes's
		// auxiliary calls take exactly that path - chat.completions.create(**kwargs) with no stream
		// argument - while the Hermes agent turn and Cline both set "stream": true explicitly.
		const clientStreamFlag = payload.stream;
		const clientWantsStream = clientStreamFlag === true;
		payload.stream = false;
		if (opts.model) payload.model = opts.model;
		if (payload.stream_options) delete payload.stream_options;

		// Abort the upstream attempt when the client goes away. Cline gives up on a turn after five
		// minutes; without this the relay kept the request alive - and the router kept billing.
		const controller = new AbortController();
		res.on('close', () => {
			if (!res.writableEnded) controller.abort();
		});
		// The upstream only accepts traffic it recognises as a mainstream coding CLI, so the
		// client's own User-Agent is forwarded instead of sending none (Node sends none by default).
		const clientAgent = req.headers['user-agent'];
		const upstreamHeaders = { 'content-type': 'application/json', authorization: 'Bearer ' + opts.key };
		if (clientAgent) upstreamHeaders['user-agent'] = clientAgent;

		const model = payload.model;
		const bodyText = JSON.stringify(payload);
		const started = Date.now();
		// Passthrough is only offered to a client that explicitly asked for SSE: a non-streaming
		// client (Hermes auxiliary calls, title generation) parses one JSON object and breaks on a
		// stream, no matter what the upstream is capable of.
		const wantPassthrough = clientWantsStream && opts.streamMode !== 'off';
		// Bytes and reasoning params are logged because both matter in practice: the router drops large
		// prompts, and this is the only way to see which reasoning effort Cline really sends.
		log(opts, 'upstream request', {
			model,
			messages: (payload.messages || []).length,
			tools: (payload.tools || []).length,
			bytes: bodyText.length,
			client_stream: clientStreamFlag === undefined ? 'absent' : clientStreamFlag,
			reply: clientWantsStream ? 'sse' : 'json',
			transport: wantPassthrough ? 'passthrough' : 'replay',
			reasoning: payload.reasoning_effort !== undefined ? payload.reasoning_effort : (payload.reasoning !== undefined ? payload.reasoning : null),
		});

		if (wantPassthrough) {
			const streamPayload = JSON.stringify({ ...payload, stream: true });
			let streamed = null;
			let streamError = null;
			try {
				streamed = await requestUpstreamStream(opts, '/chat/completions', {
					method: 'POST',
					headers: upstreamHeaders,
					body: streamPayload,
					signal: controller.signal,
				});
			} catch (err) {
				streamError = err;
			}

			// Passthrough could not even start: every route is known to drop streams. Nothing has
			// been written to the client yet, so falling back to the replay path is free and turns
			// a hard failure into a slower but correct answer - for this request only. Mutating
			// opts.streamMode here used to disable streaming for the rest of the process.
			if (streamError && streamError.streamUnsupported) {
				log(opts, 'passthrough unavailable, falling back to replay:', streamError.message);
			} else if (streamError) {
				// Giving up here is what turned one flaky path into "relay: read ECONNRESET" in the
				// client: a route that resets a passthrough stream often still answers a plain JSON
				// request, and the replay path walks the same routes with its own budget.
				const message = describeFailure(streamError);
				log(opts, 'stream upstream failed, trying the replay path:', message);
				let replayed = null;
				try {
					replayed = await requestUpstream(opts, '/chat/completions', {
						method: 'POST',
						headers: upstreamHeaders,
						body: bodyText,
						signal: controller.signal,
						budgetMs: Math.min(opts.budgetMs || REQUEST_BUDGET_MS, REPLAY_FALLBACK_BUDGET_MS),
					});
				} catch (replayErr) {
					log(opts, 'replay fallback failed too:', describeFailure(replayErr));
				}
				let replayedJson = null;
				if (replayed && replayed.ok) {
					try {
						replayedJson = JSON.parse(replayed.text);
					} catch (err) {
						log(opts, 'replay fallback body is not JSON:', err && err.message ? err.message : String(err));
					}
				}
				// Nothing usable came back: report the original failure, not the fallback's.
				if (!replayed || !replayed.ok || !replayedJson || !Array.isArray(replayedJson.choices) || replayedJson.choices.length === 0) {
					if (replayed && !replayed.ok) {
						log(opts, 'replay fallback status', replayed.status);
						mirrorSafely(opts, res, replayed.status, replayed.contentType, replayed.text);
						return;
					}
					failSafely(opts, res, 502, { error: { message: 'relay: ' + message, type: 'api_error' } });
					return;
				}
				if (isPolicyBlock(completionText(replayedJson))) {
					log(opts, 'upstream refused the request (policy gate: unsupported client), answered with a notice instead of a completion');
					failSafely(opts, res, 502, { error: { message: 'relay: ' + POLICY_BLOCK_MESSAGE, type: 'api_error' } });
					return;
				}
				log(opts, 'upstream ok', { ms: Date.now() - started, finish: replayedJson.choices[0].finish_reason, mode: 'sse', route: replayed.route || null, transport: 'replay-fallback' });
				replayAsStream(opts, res, replayedJson, model);
				return;
			} else {
				if (!streamed.ok) {
					// Headers are known and bad: nothing has been sent to the client yet, so a clean error
					// still works and the failover budget has already been spent on this status.
					const text = await readStream(streamed.body);
					log(opts, 'stream upstream status', streamed.status);
					mirrorSafely(opts, res, streamed.status, streamed.contentType, text);
					return;
				}

				const result = await pipeStreamToClient(opts, res, streamed, model);
				if (result.policyBlock) {
					log(opts, 'upstream refused the request (policy gate: unsupported client), streamed a notice instead of a completion');
					failSafely(opts, res, 502, { error: { message: 'relay: ' + POLICY_BLOCK_MESSAGE, type: 'api_error' } });
					return;
				}
				if (!result.started) {
					// The stream died before the answer began and nothing reached the client, so a
					// clean error is still possible instead of an empty 200.
					log(opts, 'stream ended before the answer started');
					failSafely(opts, res, 502, { error: { message: 'relay: the upstream stream ended before the answer started', type: 'api_error' } });
					return;
				}
				if (opts.streamMode === 'auto' && !result.terminated) {
					// Remember the verdict per route so the next request does not pay for the same
					// discovery: this upstream does not terminate streams, buffer it from now on.
					// The verdict is a hint with an expiry - the route stays in the failover pool.
					markPassthroughBroken(opts, streamed.route);
					log(opts, 'passthrough did not terminate - buffering this route from now on:', streamed.route);
				}
				if (result.terminated) {
					// A terminated stream proves the route can do passthrough, so an older verdict
					// is stale and must not keep costing latency on this route.
					clearPassthroughBroken(opts, streamed.route);
				}
				return;
			}
		}

		let upstream;
		try {
			upstream = await requestUpstream(opts, '/chat/completions', {
				method: 'POST',
				headers: upstreamHeaders,
				body: bodyText,
				signal: controller.signal,
			});
		} catch (err) {
			const message = err && err.message ? err.message : String(err);
			log(opts, 'upstream failed:', message);
			failSafely(opts, res, 502, { error: { message: 'relay: ' + message, type: 'api_error' } });
			return;
		}

		if (!upstream.ok) {
			log(opts, 'upstream status', upstream.status);
			mirrorSafely(opts, res, upstream.status, upstream.contentType, upstream.text);
			return;
		}

		let json;
		try {
			json = JSON.parse(upstream.text);
		} catch {
			failSafely(opts, res, 502, { error: { message: 'relay: upstream returned non-JSON body: ' + upstream.text.slice(0, 400), type: 'api_error' } });
			return;
		}

		// A 200 without choices would otherwise be replayed as an empty answer, which Cline reports
		// as a broken turn; surface it as a proper error instead.
		if (!json || !Array.isArray(json.choices) || json.choices.length === 0) {
			log(opts, 'upstream returned no choices');
			failSafely(opts, res, 502, { error: { message: 'relay: upstream returned no choices: ' + JSON.stringify(json).slice(0, 400), type: 'api_error' } });
			return;
		}

		if (isPolicyBlock(completionText(json))) {
			log(opts, 'upstream refused the request (policy gate: unsupported client), answered with a notice instead of a completion');
			failSafely(opts, res, 502, { error: { message: 'relay: ' + POLICY_BLOCK_MESSAGE, type: 'api_error' } });
			return;
		}

		log(opts, 'upstream ok', { ms: Date.now() - started, finish: json.choices[0].finish_reason, mode: clientWantsStream ? 'sse' : 'json', route: upstream.route || null });
		if (clientWantsStream) {
			replayAsStream(opts, res, json, model);
		} else {
			replayAsJson(opts, res, json, model);
		}
	} catch (err) {
		const message = err && err.stack ? err.stack : String(err);
		log(opts, 'request handling failed:', message);
		failSafely(opts, res, 502, { error: { message: 'relay: ' + message, type: 'api_error' } });
	}
}

async function handleModels(opts, res) {
	try {
		const upstream = await requestUpstream(opts, '/models', {
			headers: { authorization: 'Bearer ' + opts.key },
			timeoutMs: 30000,
		});
		mirrorSafely(opts, res, upstream.status, upstream.contentType, upstream.text);
	} catch (err) {
		const message = err && err.message ? err.message : String(err);
		log(opts, 'models request failed:', message);
		failSafely(opts, res, 502, { error: { message: 'relay: ' + message, type: 'api_error' } });
	}
}

// A cooling route is re-probed on its own timer instead of on the back of a user request. Without
// this a recovered route only came back when its cooldown happened to expire in the middle of a
// turn, and the strike counters grew forever (they reached 189/75 on a ten-day-old process). The
// probe is a cheap GET /models on one route at a time; failures stay silent because a still-blocked
// route is the expected case and it already carries a strike.
function startRouteProbe(opts) {
	if (!opts.probe || opts.routes.length < 2) return null;
	const timer = setInterval(async () => {
		if (!opts.routeCooldown || opts.routeCooldown.size === 0) return;
		const now = Date.now();
		for (const route of opts.routes) {
			const entry = opts.routeCooldown.get(describeRoute(route));
			if (!entry || entry.until > now) continue;
			try {
				const result = await requestOnce(opts, route, '/models', {
					headers: { authorization: 'Bearer ' + opts.key, 'user-agent': 'router-relay/probe' },
					timeoutMs: CONNECT_TIMEOUT_MS * 2,
				});
				if (result.ok) {
					clearCooldown(opts, route);
					log(opts, 'probe recovered:', describeRoute(route), '-', result.status);
				}
			} catch {
				/* still blocked - the cooldown already says so */
			}
		}
	}, ROUTE_PROBE_MS);
	// The probe must never be the reason the process stays alive.
	if (typeof timer.unref === 'function') timer.unref();
	return timer;
}

function main() {
	const opts = parseArgs(process.argv.slice(2));
	if (opts.help) {
		console.log(fs.readFileSync(__filename, 'utf8').split('*/')[0]);
		return;
	}

	fileSink = openLogFile(opts.log);

	const conf = readConf(opts.conf || path.join(__dirname, 'conf.txt'));
	opts.host = opts.host || conf.base_url || conf.base_url_reserved;
	// The reserved host used to be dead config: it was only consulted when base_url was missing
	// entirely, so it never actually served a request. It is now a real failover route.
	opts.hostReserved = opts.hostReserved || conf.base_url_reserved || '';
	opts.key = opts.key || conf.api_key || '';
	opts.proxy = opts.proxy || conf.proxy || '';
	opts.port = resolvePort(opts.port, conf.port);
	opts.streamMode = resolveStreamMode(opts.stream, conf.stream);
	opts.routeOrder = resolveRouteOrder(opts.routeOrder, conf.route_order);
	opts.timeoutMs = resolveSeconds(opts.timeout, conf.timeout, REQUEST_TIMEOUT_MS, 'timeout');
	opts.budgetMs = resolveSeconds(opts.budget, conf.budget, REQUEST_BUDGET_MS, 'budget');
	opts.probe = resolveProbe(opts.probe, conf.probe);
	if (!opts.host || !opts.key) {
		console.error('router-relay: need an upstream host and api key (conf.txt or --host/--key)');
		process.exit(2);
	}
	// A proxy tunnel is only built for https:// upstreams; an http:// base_url with proxy= would
	// otherwise be sent in the clear, which is never what the operator meant.
	if (opts.proxy && !/^https:/i.test(opts.host)) {
		console.error('router-relay: proxy= needs an https:// base_url (got "' + opts.host + '")');
		process.exit(2);
	}
	opts.routes = buildRoutes(opts);

	// An unhandled rejection kills Node (>=15), which is exactly how this relay used to die in the
	// middle of an answer and make Cline report ECONNRESET. Log and stay alive instead: all state
	// of a request is local, so surviving one broken request is always the better trade.
	process.on('unhandledRejection', (reason) => {
		log(opts, 'unhandledRejection (relay stays up):', reason && reason.stack ? reason.stack : String(reason));
	});
	process.on('uncaughtException', (err) => {
		log(opts, 'uncaughtException (relay stays up):', err && err.stack ? err.stack : String(err));
	});

	const server = http.createServer((req, res) => {
		// Nothing may throw out of this callback either.
		req.on('error', (err) => log(opts, 'request stream error:', err && err.message ? err.message : String(err)));
		res.on('error', (err) => log(opts, 'response stream error:', err && err.message ? err.message : String(err)));

		const url = (req.url || '').split('?')[0];
		try {
			if (req.method === 'POST' && url === '/v1/chat/completions') return run(opts, res, handleChat(opts, req, res));
			if (req.method === 'GET' && url === '/v1/models') return run(opts, res, handleModels(opts, res));
			if (req.method === 'GET' && (url === '/health' || url === '/')) return sendJson(res, 200, { ok: true, port: opts.port, stream: opts.streamMode, route_order: opts.routeOrder, timeout_ms: opts.timeoutMs, budget_ms: opts.budgetMs, probe: opts.probe, upstream: opts.host, upstream_reserved: opts.hostReserved || null, proxy: opts.proxy || null, routes: opts.routes.map(describeRoute), cooling: opts.routeCooldown ? Array.from(opts.routeCooldown.entries()).map(([route, e]) => ({ route, strikes: e.strikes, until: new Date(e.until).toISOString() })) : [], passthrough_broken: (opts.passthroughBroken ? opts.routes.map(describeRoute).filter((route) => isPassthroughBroken(opts, route)) : []), pid: process.pid });
			return failSafely(opts, res, 404, { error: { message: 'relay: unknown route ' + req.method + ' ' + url, type: 'invalid_request_error' } });
		} catch (err) {
			log(opts, 'routing failed:', err && err.message ? err.message : String(err));
			failSafely(opts, res, 500, { error: { message: 'relay: internal error', type: 'api_error' } });
		}
	});

	server.on('clientError', (err, socket) => {
		log(opts, 'client error:', err && err.message ? err.message : String(err));
		if (socket.writable) {
			socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
		}
	});
	server.on('error', (err) => {
		log(opts, 'server error:', err && err.stack ? err.stack : String(err));
		if (err && err.code === 'EADDRINUSE') {
			announce('router-relay: port ' + opts.port + ' is already in use - exiting');
			process.exit(3);
		}
	});
	server.keepAliveTimeout = 120000;
	server.headersTimeout = 125000;

	server.listen(opts.port, '127.0.0.1', () => {
		// With port=0 the OS picks the port, so the requested value is meaningless from here on:
		// adopt the real one so /health, the startup banner and the stopper all agree.
		const address = server.address();
		if (address && typeof address === 'object' && address.port) {
			if (opts.port !== address.port) log(opts, 'port 0 requested, OS assigned', address.port);
			opts.port = address.port;
		}
		announce('router-relay listening on http://127.0.0.1:' + opts.port + '/v1  ->  ' + opts.routes.map(describeRoute).join('  ||  ') + ' (pid ' + process.pid + ', stream=' + opts.streamMode + ', order=' + opts.routeOrder + ', timeout=' + Math.round(opts.timeoutMs / 1000) + 's, budget=' + Math.round(opts.budgetMs / 1000) + 's, probe=' + (opts.probe ? 'on' : 'off') + ', failover order: ' + opts.routes.length + ' routes x ' + ROUTE_PASSES + ' passes; streaming clients get SSE, non-streaming clients get JSON)');
		startRouteProbe(opts);
	});
}

main();

