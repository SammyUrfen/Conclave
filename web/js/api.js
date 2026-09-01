// api.js — the REST client, the WebSocket event client, and server-URL handling.
//
// This is the only module that talks to the network. Everything here is written assuming
// PLAN.md §9.4a is the literal contract and "the server is not running most of the time" is
// the common case, not the exception (task brief, "Robustness").

import { parseEnvelope } from './format.js';

/** The dashboard wire contract this build was written against (PLAN.md §9.4a). */
export const CLIENT_API_VERSION = 1;

/** Thrown for every REST failure — network, non-2xx, or a malformed body. Always has `.code`. */
export class ApiError extends Error {
  constructor(code, message, details = {}, status = 0) {
    super(message);
    this.name = 'ApiError';
    this.code = code;
    this.details = details;
    this.status = status;
  }
}

// ---------------------------------------------------------------------------------------
// Server URL handling — §9.9: an https:// Pages site may open ws://localhost without a
// mixed-content block (localhost/127.0.0.1/[::1] are "potentially trustworthy origins" by
// spec), but ws:// to any OTHER host from an https:// page is blocked. So scheme selection
// depends on both what the user typed and what scheme *this page* was loaded over.
// ---------------------------------------------------------------------------------------

const LOOPBACK_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]', '::1']);

/**
 * Turn whatever the user typed into the server-URL field into concrete REST and WS base
 * URLs. Accepts "host:port", "host" (port-less), or a full "scheme://host[:port]".
 *
 * @param {string} raw
 * @returns {{httpBase: string, wsBase: string, host: string}}
 * @throws {Error} if `raw` is empty or unparseable — callers must show this to the user,
 *   never let it crash the render path (Robustness contract).
 */
export function parseServerURL(raw) {
  const input = (raw || '').trim();
  if (!input) throw new Error('server address is empty');

  let scheme, host;
  if (input.includes('://')) {
    let u;
    try {
      u = new URL(input);
    } catch {
      throw new Error(`"${input}" is not a valid URL`);
    }
    host = u.host;
    // Normalize whichever of http/https/ws/wss the user typed to a canonical http(s) pair;
    // we derive the ws(s) counterpart ourselves rather than trust two independently-typed
    // schemes to agree.
    scheme = u.protocol.replace(':', '').replace(/^ws/, 'http');
  } else {
    host = input;
    scheme = null; // decide below
  }

  if (!host) throw new Error(`"${input}" has no host`);
  const hostname = host.split(':')[0].toLowerCase();
  const isLoopback = LOOPBACK_HOSTS.has(hostname);

  if (!scheme) {
    // §9.9: localhost/127.0.0.1 stay plaintext regardless of how this page was loaded — that
    // is the whole point of the "potentially trustworthy origin" exemption, and it's what
    // makes the localhost:9000 default work from a Pages https:// origin out of the box.
    // Any other host must match the page's own scheme, or a mixed-content block (https page,
    // ws:// target) or a doomed plaintext-to-TLS-only-server request (http page, wss-only
    // target) results — see §9.9 caveat 3, "any remote arbiter must be wss:// with a real
    // certificate."
    const pageIsSecure = window.location.protocol === 'https:';
    scheme = isLoopback ? 'http' : (pageIsSecure ? 'https' : 'http');
  }

  const httpBase = `${scheme}://${host}`;
  const wsBase = `${scheme === 'https' ? 'wss' : 'ws'}://${host}`;
  return { httpBase, wsBase, host };
}

// ---------------------------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------------------------

async function request(httpBase, path, opts = {}) {
  let res;
  try {
    res = await fetch(httpBase + path, {
      ...opts,
      // §9.4a CORS details: never send credentials — there is no auth, and doing so would
      // only widen the attack surface.
      credentials: 'omit',
      headers: opts.body ? { 'Content-Type': 'application/json', ...(opts.headers || {}) } : opts.headers,
    });
  } catch (err) {
    // fetch() throws on network failure, DNS failure, CORS block, refused connection —
    // exactly the "server is not running most of the time" case this UI must not blank on.
    throw new ApiError('network_error', `could not reach ${httpBase}: ${err.message}`, {}, 0);
  }

  const bodyText = await res.text();
  let body = null;
  if (bodyText) {
    try {
      body = parseEnvelope(bodyText);
    } catch {
      // Non-JSON body (e.g. a plain-text 404 from an unregistered route — see the demo-probe
      // use of this, or a proxy's error page). Treat as an opaque failure, not a crash.
      if (!res.ok) {
        throw new ApiError('bad_response', `${res.status} ${res.statusText}`, {}, res.status);
      }
      return { ok: true, body: null, raw: bodyText };
    }
  }

  if (!res.ok) {
    const err = body && body.error ? body.error : {};
    throw new ApiError(err.code || 'internal', err.message || `HTTP ${res.status}`, err.details || {}, res.status);
  }

  if (body && typeof body.api_version === 'number' && body.api_version > CLIENT_API_VERSION) {
    // Surfaced to the caller via a marker rather than thrown — a version-skewed body is
    // still real data the UI may want to show alongside the skew banner.
    body.__skew = true;
  }
  return { ok: true, body, raw: bodyText, status: res.status };
}

/**
 * GET /api/meets. The body carries `demo_enabled` (§9.6) alongside `meets`/`ended` —
 * the server advertises the gated demo surface on this same call rather than a
 * separate capability route, so state.js reads it straight off this response with no
 * extra round trip and no speculative probe of the destructive surface.
 */
export async function listMeets(httpBase) {
  const { body } = await request(httpBase, '/api/meets');
  return body;
}

/** POST /api/meets */
export async function createMeet(httpBase, id) {
  const payload = id ? { id } : {};
  const { body } = await request(httpBase, '/api/meets', { method: 'POST', body: JSON.stringify(payload) });
  return body;
}

/**
 * GET /api/meets/{id}. Implemented for API completeness (§9.3), but the meet-detail view
 * does not call this directly — it opens the WS stream instead, whose first frame is always
 * a `snapshot` carrying this exact body (§9.3: "the same object the WS stream sends as its
 * first snapshot frame"). One fetch path instead of two avoids a render branch for "REST
 * snapshot" vs "WS snapshot" that would only ever differ by a race condition.
 */
export async function getMeet(httpBase, id) {
  const { body } = await request(httpBase, `/api/meets/${encodeURIComponent(id)}`);
  return body;
}

/** POST /api/demo/meets/{id}/evict */
export async function demoEvict(httpBase, meetId, name) {
  const { body } = await request(httpBase, `/api/demo/meets/${encodeURIComponent(meetId)}/evict`, {
    method: 'POST',
    body: JSON.stringify({ name }),
  });
  return body;
}

/** POST /api/demo/meets/{id}/elect */
export async function demoElect(httpBase, meetId, name) {
  const payload = name ? { name } : {};
  const { body } = await request(httpBase, `/api/demo/meets/${encodeURIComponent(meetId)}/elect`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  return body;
}

// ---------------------------------------------------------------------------------------
// WebSocket event stream — GET /api/meets/{id}/events, §9.3/§9.4/§9.4a
// ---------------------------------------------------------------------------------------

// Frozen reconnect schedule (§9.4a): 500ms,1s,2s,4s,8s, then every 15s, ±20% jitter; reset
// to 500ms after a connection survives 30s.
const BACKOFF_STEPS_MS = [500, 1000, 2000, 4000, 8000];
const BACKOFF_STEADY_MS = 15000;
const STABLE_RESET_MS = 30000;
const PING_INTERVAL_MS = 20000;
const STALE_AFTER_MS = 45000; // no frame in this long while OPEN ⇒ "stale", not "live"

function jitter(ms) {
  return Math.round(ms * (0.8 + Math.random() * 0.4));
}

/**
 * One WebSocket connection to a single meet's event stream, with the frozen reconnect
 * policy, seq-gap → resync recovery, and api_version skew detection built in.
 *
 * Close-code handling (§9.4a table):
 *   1000 normal, 1011 internal ⇒ reconnect with backoff
 *   1001 going away (meet deleted), 4404 not found at upgrade ⇒ stop, caller navigates away
 *   1008 policy violation (malformed op only) ⇒ stop, never reconnect
 *   1006 with no prior successful `open` on this socket ⇒ the handshake itself was refused
 *     (§9.4a v2.6) — see below. Stop, never reconnect.
 *   anything else (unrecognized code) ⇒ reconnect with backoff, conservatively
 *
 * **Upgrade failure vs. close, and why 1006 is overloaded (§9.4a v2.6 correction).**
 * `websocket.Accept` refuses a disallowed origin at the HTTP handshake with a **403** —
 * no WebSocket is ever established, so there is no close *frame* and therefore no close
 * *code* the server chose. The browser reports this exactly like any other abnormal
 * closure: `CloseEvent.code === 1006`. That is indistinguishable, by code alone, from a
 * genuine mid-session network drop. The one fact that DOES distinguish them is whether
 * this socket ever fired `open`: a real drop always has an `open` behind it; a refused
 * handshake never does. `everOpened` below is that signal, tracked for the socket's whole
 * lifetime (not reset per reconnect attempt) — once this origin has been accepted once,
 * later 1006s are treated as ordinary drops, never re-litigated as a config error.
 */
export class EventSocket {
  /**
   * @param {string} wsBase
   * @param {string} meetId
   * @param {{onStatus?:(s:string)=>void, onSnapshot?:(d:any)=>void, onEvent?:(e:any)=>void,
   *          onFatal?:(reason:string, detail?:object)=>void, onVersionSkew?:(v:number)=>void}} handlers
   */
  constructor(wsBase, meetId, handlers = {}) {
    this.wsBase = wsBase;
    this.meetId = meetId;
    this.handlers = handlers;
    this.ws = null;
    this.closedByUser = false;
    this.backoffIndex = 0;
    this.reconnectTimer = null;
    this.connectedAt = 0;
    this.expectedSeq = null; // BigInt once the first frame lands
    this.versionSkewed = false;
    this.lastFrameAt = 0;
    this.pingTimer = null;
    this.staleTimer = null;
    this.everOpened = false; // true once 'open' has fired at least once, ever (see class doc)
  }

  connect() {
    this.closedByUser = false;
    this._setStatus('connecting');
    let ws;
    try {
      ws = new WebSocket(`${this.wsBase}/api/meets/${encodeURIComponent(this.meetId)}/events`);
    } catch (err) {
      this._scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.addEventListener('open', () => {
      this.everOpened = true;
      this.connectedAt = Date.now();
      this.lastFrameAt = Date.now();
      this._setStatus('live');
      this._startPing();
      this._startStaleWatch();
    });

    ws.addEventListener('message', (ev) => this._onMessage(ev));

    ws.addEventListener('close', (ev) => this._onClose(ev));

    ws.addEventListener('error', () => {
      // The 'close' event always follows 'error' for a WebSocket; no separate handling needed.
    });
  }

  /** User-initiated disconnect: no auto-reconnect until connect() is called again. */
  close() {
    this.closedByUser = true;
    this._clearTimers();
    if (this.ws) {
      try {
        this.ws.close(1000, 'client disconnect');
      } catch {
        /* already closed */
      }
    }
    this._setStatus('disconnected');
  }

  /** Ask the server for a fresh snapshot — the recovery path for a seq gap. */
  resync() {
    this._send({ op: 'resync' });
  }

  _send(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
    }
  }

  _startPing() {
    this._clearInterval(this.pingTimer);
    this.pingTimer = setInterval(() => this._send({ op: 'ping' }), PING_INTERVAL_MS);
  }

  _startStaleWatch() {
    this._clearInterval(this.staleTimer);
    this.staleTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      const idle = Date.now() - this.lastFrameAt;
      this._setStatus(idle > STALE_AFTER_MS ? 'stale' : 'live');
    }, 5000);
  }

  _clearInterval(t) {
    if (t) clearInterval(t);
  }

  _clearTimers() {
    this._clearInterval(this.pingTimer);
    this._clearInterval(this.staleTimer);
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  _onMessage(ev) {
    this.lastFrameAt = Date.now();
    this._setStatus('live');

    let frame;
    try {
      frame = parseEnvelope(ev.data);
    } catch {
      // A malformed frame must not crash the socket or the page — drop it and keep going.
      return;
    }
    if (!frame || typeof frame !== 'object') return;

    // api_version skew (§9.4a): higher than expected ⇒ non-dismissable banner, stop applying
    // deltas (still allowed to show the last snapshot); lower ⇒ proceed, log to console.
    if (typeof frame.api_version === 'number') {
      if (frame.api_version > CLIENT_API_VERSION && !this.versionSkewed) {
        this.versionSkewed = true;
        this.handlers.onVersionSkew && this.handlers.onVersionSkew(frame.api_version);
      } else if (frame.api_version < CLIENT_API_VERSION) {
        console.warn(`conclave dashboard: server api_version ${frame.api_version} < client ${CLIENT_API_VERSION}`);
      }
    }

    // seq gap detection, per-connection numbering (§9.4). Every frame — snapshot, delta, or
    // pong — carries seq, so this one check covers the initial frame and every resync reply
    // uniformly; no kind-specific special case needed.
    if (typeof frame.seq === 'bigint') {
      if (this.expectedSeq !== null && frame.seq !== this.expectedSeq) {
        console.warn(`conclave dashboard: seq gap on meet ${this.meetId} (expected ${this.expectedSeq}, got ${frame.seq}) — resyncing`);
        this.resync();
      }
      this.expectedSeq = frame.seq + 1n;
    }

    if (this.versionSkewed) return; // stop applying deltas; last snapshot stays on screen

    if (frame.kind === 'snapshot') {
      this.handlers.onSnapshot && this.handlers.onSnapshot(frame);
    } else if (frame.kind === 'pong') {
      // liveness only — already handled by lastFrameAt/status above.
    } else if (frame.kind) {
      this.handlers.onEvent && this.handlers.onEvent(frame);
    }
  }

  _onClose(ev) {
    this._clearTimers();
    if (this.closedByUser) return;

    const code = ev.code;

    // §9.4a v2.6: a refused origin never opens a socket at all, so the browser reports it
    // as the generic 1006 "abnormal closure" — identical, by code, to a real mid-session
    // drop. `everOpened` is the only thing that tells them apart (see class doc comment).
    // Only "never connected" is treated as a config error; "connected then lost" always
    // falls through to the normal reconnect ladder below, same as before this change.
    if (!this.everOpened && code === 1006) {
      this._setStatus('rejected');
      this.handlers.onFatal && this.handlers.onFatal('origin_rejected', { origin: window.location.origin });
      return; // never reconnect — retrying an origin the server will not accept cannot help
    }

    if (code === 1001 || code === 4404) {
      this._setStatus('gone');
      this.handlers.onFatal && this.handlers.onFatal(code === 1001 ? 'meet_deleted' : 'meet_not_found');
      return;
    }
    if (code === 1008) {
      this._setStatus('rejected');
      this.handlers.onFatal && this.handlers.onFatal('policy_violation');
      return; // never reconnect — retrying cannot help
    }
    // 1000, 1011, and anything unrecognized: reconnect with backoff.
    this._scheduleReconnect();
  }

  _scheduleReconnect() {
    this._setStatus('reconnecting');
    const survived = this.connectedAt && Date.now() - this.connectedAt >= STABLE_RESET_MS;
    if (survived) this.backoffIndex = 0;

    const delay = this.backoffIndex < BACKOFF_STEPS_MS.length
      ? BACKOFF_STEPS_MS[this.backoffIndex]
      : BACKOFF_STEADY_MS;
    this.backoffIndex = Math.min(this.backoffIndex + 1, BACKOFF_STEPS_MS.length);

    this.reconnectTimer = setTimeout(() => {
      if (!this.closedByUser) this.connect();
    }, jitter(delay));
  }

  _setStatus(status) {
    this.handlers.onStatus && this.handlers.onStatus(status);
  }
}
