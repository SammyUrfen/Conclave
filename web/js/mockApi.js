// mockApi.js — DEV-ONLY fixture-backed stand-in for api.js.
//
// Not shipped behaviour: this module exists so the render path (meets list → create → live
// subnet → event log → build-state banners) can be proven end to end with no Go server
// running, per the task's verification requirement. It is only ever loaded when the page is
// opened with `?mock=1` — see main.js. `web/fixtures/*.json` are the fixture bodies and are
// themselves marked `_dev_only` so nobody mistakes them for a real server contract.
//
// MockEventSocket subclasses the REAL EventSocket from api.js and only replaces how bytes
// arrive (a scripted timer instead of a socket) — every other line (seq tracking, resync,
// api_version skew, ping/stale) is the actual production code path being exercised.

import { EventSocket, CLIENT_API_VERSION } from './api.js';
import { parseEnvelope } from './format.js';

const FIXTURES_BASE = 'fixtures';
const PLAYBACK_INTERVAL_MS = 1800;

async function loadFixture(name) {
  const res = await fetch(`${FIXTURES_BASE}/${name}`);
  if (!res.ok) throw new Error(`fixture ${name} failed to load: HTTP ${res.status}`);
  return parseEnvelope(await res.text());
}

function stringifyWithBigInt(value) {
  return JSON.stringify(value, (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
}

export async function listMeets() {
  return loadFixture('meets.json');
}

export async function createMeet(_httpBase, id) {
  const template = await loadFixture('meet-standup.json');
  const meetId = id || template.id;
  return {
    api_version: CLIENT_API_VERSION,
    id: meetId,
    created_at_unix_ms: Date.now(),
    join: {
      ws_url: `ws://localhost:9000/ws?room=${meetId}`,
      peer_command: `peer -call -managed -server http://localhost:9000 -room ${meetId} -name YOUR_NAME`,
    },
  };
}

export async function getMeet(_httpBase, _id) {
  return loadFixture('meet-standup.json');
}

// No hasDemoCapability export here any more: `demo_enabled` (§9.6) rides the listMeets
// body like the real server, so fixtures/meets.json's own `"demo_enabled": true` is what
// makes the demo-controls render path exercisable in fixture mode — one read path for
// both api.js and mockApi.js, not a second mock-only mechanism.
export async function demoEvict(_httpBase, _meetId, name) {
  return { ok: true, action: 'evict', target: name };
}

export async function demoElect(_httpBase, _meetId, name) {
  return { ok: true, action: 'elect', target: name || '' };
}

export class MockEventSocket extends EventSocket {
  connect() {
    this.closedByUser = false;
    this._setStatus('connecting');
    // A fake "transport" satisfying the two things EventSocket touches on `this.ws`:
    // readyState (for _send's OPEN check) and send() (routed back into this instance so
    // resync/ping behave exactly as the real client→server protocol expects).
    this.ws = {
      readyState: 1, // WebSocket.OPEN
      send: (data) => this._handleClientSend(data),
      close: () => {},
    };
    this._frames = null;
    this._frameIndex = 0;
    // The connection's own seq counter (§9.4: "monotonic PER CONNECTION, starting at 1").
    // Every frame we hand to _deliver gets stamped from this counter at send time — the
    // fixture JSON's seq values are just placeholders for readability; they are NOT what
    // goes on the wire. This is what lets a ping/pong interleave with scripted events
    // without producing a fake gap: a pong consumes a slot from the SAME counter, exactly
    // like a real server's per-connection numbering would.
    this._seqCounter = 1n;
    this._playTimer = setTimeout(() => this._start(), 150); // simulate connect latency
  }

  async _start() {
    if (this.closedByUser) return;
    this.connectedAt = Date.now();
    this.lastFrameAt = Date.now();
    this._setStatus('live');
    this._startPing();
    this._startStaleWatch();
    try {
      if (!this._frames) {
        this._snapshotTemplate = await loadFixture('meet-standup.json');
        const deltas = await loadFixture('events-standup.json');
        this._frames = deltas;
        this._deliver(this._snapshotFrame());
      }
    } catch (err) {
      console.error('conclave mock: failed to load fixtures', err);
      return;
    }
    this._playNext();
  }

  _snapshotFrame() {
    return {
      api_version: CLIENT_API_VERSION,
      seq: this._seqCounter++,
      at_unix_ms: Date.now(),
      meet_id: this._snapshotTemplate.id,
      epoch: this._snapshotTemplate.epoch,
      rev: this._snapshotTemplate.rev,
      kind: 'snapshot',
      data: this._snapshotTemplate,
    };
  }

  _playNext() {
    if (this.closedByUser || !this._frames || this._frameIndex >= this._frames.length) return;
    // Shallow-copy so the fixture's own `seq` placeholder is never mutated in place, then
    // stamp the real per-connection seq at send time (see the comment in connect()).
    const frame = { ...this._frames[this._frameIndex++], seq: this._seqCounter++ };
    this._deliver(frame);
    this._playTimer = setTimeout(() => this._playNext(), PLAYBACK_INTERVAL_MS);
  }

  _deliver(frameObj) {
    this._onMessage({ data: stringifyWithBigInt(frameObj) });
  }

  _handleClientSend(raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (msg.op === 'resync') {
      this._deliver(this._snapshotFrame());
    } else if (msg.op === 'ping') {
      this._deliver({
        api_version: CLIENT_API_VERSION,
        seq: this._seqCounter++,
        at_unix_ms: Date.now(),
        meet_id: this._snapshotTemplate ? this._snapshotTemplate.id : '',
        epoch: this._snapshotTemplate ? this._snapshotTemplate.epoch : 0,
        rev: this._snapshotTemplate ? this._snapshotTemplate.rev : 0,
        kind: 'pong',
        data: {},
      });
    }
  }

  close() {
    this.closedByUser = true;
    this._clearTimers();
    if (this._playTimer) clearTimeout(this._playTimer);
    this._setStatus('disconnected');
  }
}
