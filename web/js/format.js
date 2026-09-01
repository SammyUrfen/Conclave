// format.js — defensive value formatting + the BigInt-safe envelope parser.
//
// PLAN.md §9.4a is explicit: "epoch/rev/seq = unsigned integers, and the client must treat
// them as opaque and compare only for equality/ordering (they can exceed 2^53 in principle;
// parse as BigInt or compare as strings — do not do arithmetic on them)." `JSON.parse`
// already lost precision on those fields by the time a reviver sees them (the text has been
// tokenized to a JS double), so the only correct fix is to quote the digits *before*
// `JSON.parse` ever runs, then revive the quoted string back to a BigInt. `parseEnvelope`
// below does exactly that for every field in BIGINT_KEYS, wherever it appears in the body
// (top-level `seq`/`epoch`/`rev`, or nested `last_beat_seq` inside `nodes[]`).
//
// This project's whole ethos is "explain the primitive, don't just hand a library" — this
// is the from-scratch version of what a JSON-bigint npm package would otherwise do for us.

const BIGINT_KEYS = new Set(['seq', 'epoch', 'rev', 'last_beat_seq', 'peer_epoch', 'meet_epoch']);

// Matches `"key":123` or `"key": -123` for exactly the frozen bigint field names, and wraps
// the digits in quotes so JSON.parse hands them to the reviver as strings, not doubles.
// `peer_epoch`/`meet_epoch` are `announce_repair`'s (§9.4, v2.3+) — also uint64 on the wire.
const BIGINT_FIELD_RE = /"(seq|epoch|rev|last_beat_seq|peer_epoch|meet_epoch)":\s*(-?\d+)(?!\.)/g;

/**
 * Parse a REST or WebSocket JSON body, converting `seq`/`epoch`/`rev`/`last_beat_seq` to
 * BigInt wherever they occur. Everything else parses as normal JS values.
 *
 * @param {string} text - raw response/message body.
 * @returns {any}
 * @throws {SyntaxError} if `text` is not valid JSON (callers must catch this — a malformed
 *   body from the server must not crash the render path, per the robustness contract).
 */
export function parseEnvelope(text) {
  const quoted = text.replace(BIGINT_FIELD_RE, '"$1":"$2"');
  return JSON.parse(quoted, (key, value) => {
    if (BIGINT_KEYS.has(key) && typeof value === 'string' && /^-?\d+$/.test(value)) {
      try {
        return BigInt(value);
      } catch {
        return value; // fall back to the raw string rather than throw on a corrupt field
      }
    }
    return value;
  });
}

/** True if `v` is a finite JS number (guards every formatter below against bad input). */
function isFiniteNumber(v) {
  return typeof v === 'number' && Number.isFinite(v);
}

/** depth: -1 means "not in the tree" and must render as "unattached", never as a number. */
export function fmtDepth(depth) {
  if (depth === -1 || depth === '-1') return 'unattached';
  if (!isFiniteNumber(depth)) return '—'; // em dash — missing/malformed field
  return String(depth);
}

/** percent fields (0–100, 1 decimal per §9.4a). */
export function fmtPct(v) {
  if (!isFiniteNumber(v)) return '—';
  return `${v.toFixed(1)}%`;
}

/** fitness (0–1, 3 decimals per §9.4a). */
export function fmtFitness(v) {
  if (!isFiniteNumber(v)) return '—';
  return v.toFixed(3);
}

/** *_kbps — kbit/s, integer. */
export function fmtKbps(v) {
  if (!isFiniteNumber(v)) return '—';
  return `${Math.round(v)} kbit/s`;
}

/** *_ms — milliseconds, float. */
export function fmtMs(v) {
  if (!isFiniteNumber(v)) return '—';
  return `${v.toFixed(1)} ms`;
}

/** *_unix_ms — integer ms since epoch, UTC. Renders local wall-clock time for a human. */
export function fmtUnixMs(v) {
  if (!isFiniteNumber(v)) return '—';
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toLocaleTimeString(undefined, { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

/** Opaque BigInt/number id fields (epoch, rev, seq) — never arithmetic, display only. */
export function fmtId(v) {
  if (v == null) return '—';
  return String(v);
}

/** Defensive string accessor: never let a missing/non-string field reach textContent as "undefined". */
export function str(v, fallback = '') {
  return typeof v === 'string' ? v : fallback;
}
