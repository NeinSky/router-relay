#!/usr/bin/env node
/*
 * router-relay.js - local OpenAI-compatible shim for Cline.
 *
 * WHY THIS EXISTS
 *   The upstream router (router.cheap / direct.router-cheap.com) accepts SSE requests but never
 *   finishes them: it sends content chunks, then drops the connection without a `finish_reason`
 *   chunk and without `data: [DONE]`. Cline (which always streams - it calls streamText and has no
 *   non-streaming fallback) therefore reports:
 *
 *     Error: Failed to process successful response: TypeError: The socket connection was closed
 *     unexpectedly. ... (ECONNRESET)
 *
 *   The same router answers perfectly when asked with "stream": false: the full message, a proper
 *   finish_reason and the usage block all arrive in one JSON body.
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
 *   node router-relay.js [--port 8765] [--host URL] [--host-reserved URL] [--key KEY]
 *                        [--model ID] [--conf path] [--proxy URL] [--log FILE] [-v]
 *
 *   Defaults come from conf.txt next to this file (base_url - the main address - then
 *   base_url_reserved; api_key). Use --host to point the relay somewhere else.
 *   The model is taken from the incoming request, so Cline may switch models freely.
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
 *   also flakes on its own. Direct is first because it is faster and needs no proxy. Failover
 *   triggers on transport errors and on 403/408/429/5xx; a 400/401/404 is returned immediately so a
 *   caller-side mistake is not retried across the whole list. A route that fails cools down with
 *   exponential backoff (60s doubling to a 15-minute ceiling), because a blocked connection can take
 *   ~19s to surface and re-paying that per request is worse than the outage it guards against. The
 *   strikes are cleared on success, so a recovered direct path comes back on its own.
 */

'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const DEFAULT_PORT = 8765;
const REQUEST_TIMEOUT_MS = 600000; // 10 min per attempt: reasoning models can think for a long time
// Failover budget: each route in the list is tried once per pass, and a second pass re-walks the
// whole list because the upstream router also flakes on its own, independently of which route we
// picked. Normal traffic succeeds on route 1 and never pays for the rest.
const ROUTE_PASSES = 2;
// A blocked route does not fail fast: a Cloudflare-reset connection can take ~19s to surface, and
// paying that on every single request would be worse than the outage it protects against. So a
// route that fails is put in the corner and the healthy routes are tried first, with the wait
// doubling per consecutive failure up to a ceiling - a persistent block ends up costing one slow
// probe every 15 minutes instead of one per request. A route is never dropped outright: on success
// its strikes are cleared, so a recovered direct path returns on its own.
const ROUTE_COOLDOWN_MS = 60000;
const ROUTE_COOLDOWN_MAX_MS = 900000;

function parseArgs(argv) {
	const opts = { port: DEFAULT_PORT, verbose: false };
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === '--port' || a === '-p') opts.port = Number(argv[++i]);
		else if (a === '--host' || a === '-b') opts.host = argv[++i];
		else if (a === '--host-reserved' || a === '-B') opts.hostReserved = argv[++i];
		else if (a === '--key' || a === '-k') opts.key = argv[++i];
		else if (a === '--model' || a === '-m') opts.model = argv[++i];
		else if (a === '--conf') opts.conf = argv[++i];
		else if (a === '--log') opts.log = argv[++i];
		else if (a === '--proxy') opts.proxy = argv[++i];
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

// Upstream request through a local HTTP proxy (HAPP / xray, e.g. http://127.0.0.1:10809).
// Node 22's fetch ignores HTTP_PROXY and undici is not a public module there, so curl does the
// tunnel: it handles CONNECT, chunked responses and timeouts, and it is present on every Windows
// box we target. The body goes in over stdin, the status code comes back via -w.
function requestViaCurl(opts, proxy, url, options) {
	return new Promise((resolve, reject) => {
		const curl = process.platform === 'win32' ? 'curl.exe' : 'curl';
		const args = [
			'-s', '-S',
			'--proxy', proxy,
			'--max-time', String(Math.round((options.timeoutMs || REQUEST_TIMEOUT_MS) / 1000)),
			'-X', options.method || 'GET',
		];
		for (const [name, value] of Object.entries(options.headers || {})) {
			args.push('-H', name + ': ' + value);
		}
		if (options.body !== undefined) args.push('--data-binary', '@-');
		args.push('-w', '\n__CURL_STATUS__%{http_code}', url);

		const child = spawn(curl, args, { windowsHide: true });
		const outChunks = [];
		const errChunks = [];
		child.stdout.on('data', (chunk) => outChunks.push(chunk));
		child.stderr.on('data', (chunk) => errChunks.push(chunk));
		child.on('error', reject);
		child.on('close', (code) => {
			const combined = Buffer.concat(outChunks).toString('utf8');
			const marker = '__CURL_STATUS__';
			const at = combined.lastIndexOf(marker);
			if (code !== 0 || at < 0) {
				const detail = Buffer.concat(errChunks).toString('utf8').trim();
				reject(new Error('curl via proxy failed (rc=' + code + ')' + (detail ? ': ' + detail.slice(0, 160) : '')));
				return;
			}
			resolve({ status: Number(combined.slice(at + marker.length).trim()), text: combined.slice(0, at) });
		});

		if (options.body !== undefined) child.stdin.end(options.body);
		else child.stdin.end();
	});
}

// Ordered failover routes. Direct comes first: it is the fastest path (no tunnel - roughly 80ms
// of TLS against ~300ms through the proxy) and it has no dependency on the local proxy being up.
// The proxy follows immediately, because when Cloudflare blocks the region every direct host fails
// together - both hostnames are the same zone, so trying the reserved host direct before the proxy
// only adds a doomed attempt. The reserved host is still walked, for the case where a single
// hostname is having trouble rather than the whole zone.
function buildRoutes(opts) {
	const hosts = [];
	if (opts.host) hosts.push(opts.host);
	if (opts.hostReserved && !hosts.includes(opts.hostReserved)) hosts.push(opts.hostReserved);
	const proxies = [''];
	if (opts.proxy) proxies.push(opts.proxy);
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

// One transport attempt on one route. An empty proxy means Node's own fetch, which genuinely goes
// direct (undici ignores HTTP_PROXY/HTTPS_PROXY); otherwise curl performs the tunnel.
async function requestOnce(opts, route, pathname, options) {
	const url = route.host.replace(/\/+$/, '') + pathname;
	if (route.proxy) {
		const result = await requestViaCurl(opts, route.proxy, url, options);
		return {
			ok: result.status >= 200 && result.status < 300,
			status: result.status,
			contentType: 'application/json',
			text: result.text,
		};
	}

	const init = {
		method: options.method || 'GET',
		headers: options.headers,
		signal: AbortSignal.timeout(options.timeoutMs || REQUEST_TIMEOUT_MS),
	};
	if (options.body !== undefined) init.body = options.body;

	const response = await fetch(url, init);
	const text = await response.text();
	return {
		ok: response.ok,
		status: response.status,
		contentType: response.headers.get('content-type') || 'application/json',
		text,
	};
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

async function requestUpstream(opts, pathname, options) {
	// transport + body read + (by the caller) JSON.parse are one retryable unit: the upstream router
	// sometimes drops the connection while the body is still being read, which used to bubble up as
	// an unhandled rejection and kill the process.
	const routes = routeOrder(opts);
	let lastErr = null;
	let lastResponse = null;

	for (let pass = 0; pass < ROUTE_PASSES; pass++) {
		for (const route of routes) {
			let result;
			try {
				result = await requestOnce(opts, route, pathname, options);
			} catch (err) {
				lastErr = err;
				const cool = coolDownRoute(opts, route);
				log(opts, 'route failed (pass ' + (pass + 1) + '/' + ROUTE_PASSES + '), cooling ' + Math.round(cool.wait / 1000) + 's (strike ' + opts.routeCooldown.get(cool.key).strikes + '):', cool.key, '-', err && err.message ? err.message : String(err));
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
	}

	// Every route produced the same edge/upstream error: hand the last one back so the client sees
	// the real status instead of a relay-invented failure.
	if (lastResponse) return lastResponse;
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

		const model = payload.model;
		const bodyText = JSON.stringify(payload);
		const started = Date.now();
		// Bytes and reasoning params are logged because both matter in practice: the router drops large
		// prompts, and this is the only way to see which reasoning effort Cline really sends.
		log(opts, 'upstream request', {
			model,
			messages: (payload.messages || []).length,
			tools: (payload.tools || []).length,
			bytes: bodyText.length,
			client_stream: clientStreamFlag === undefined ? 'absent' : clientStreamFlag,
			reply: clientWantsStream ? 'sse' : 'json',
			reasoning: payload.reasoning_effort !== undefined ? payload.reasoning_effort : (payload.reasoning !== undefined ? payload.reasoning : null),
		});

		let upstream;
		try {
			upstream = await requestUpstream(opts, '/chat/completions', {
				method: 'POST',
				headers: { 'content-type': 'application/json', authorization: 'Bearer ' + opts.key },
				body: bodyText,
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
	if (!opts.host || !opts.key) {
		console.error('router-relay: need an upstream host and api key (conf.txt or --host/--key)');
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
			if (req.method === 'GET' && (url === '/health' || url === '/')) return sendJson(res, 200, { ok: true, upstream: opts.host, upstream_reserved: opts.hostReserved || null, proxy: opts.proxy || null, routes: opts.routes.map(describeRoute), cooling: opts.routeCooldown ? Array.from(opts.routeCooldown.entries()).map(([route, e]) => ({ route, strikes: e.strikes, until: new Date(e.until).toISOString() })) : [], pid: process.pid });
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
		announce('router-relay listening on http://127.0.0.1:' + opts.port + '/v1  ->  ' + opts.routes.map(describeRoute).join('  ||  ') + ' (pid ' + process.pid + ', failover order: ' + opts.routes.length + ' routes x ' + ROUTE_PASSES + ' passes; streaming clients get SSE, non-streaming clients get JSON)');
	});
}

main();

