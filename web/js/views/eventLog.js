// eventLog.js — §9.5 "Event log": the envelope stream, newest first, colour-coded by kind.

import { el, setChildren } from '../dom.js';
import { fmtUnixMs, fmtId, str, isArbiterHosting } from '../format.js';

const KIND_CLASS = {
  snapshot: 'kind-neutral',
  member_joined: 'kind-ok',
  member_left: 'kind-muted',
  health_changed: 'kind-warn',
  topology: 'kind-accent',
  reparent: 'kind-accent',
  failover: 'kind-crit',
  election: 'kind-primary',
  announce_repair: 'kind-warn',
  stale_rejected: 'kind-warn',
  settling: 'kind-muted',
  unbuildable: 'kind-crit',
  demo: 'kind-secondary',
  pong: 'kind-muted',
};

export function render(container, events) {
  if (!events || events.length === 0) {
    setChildren(container, el('div', { class: 'empty-state' }, 'No events yet.'));
    return;
  }

  const rows = events.map((frame) => {
    const kind = str(frame.kind, 'unknown');
    const cls = KIND_CLASS[kind] || 'kind-neutral'; // unknown kind ⇒ neutral, never crash
    return el(
      'div',
      { class: `event-row ${cls}` },
      el('span', { class: 'event-time' }, fmtUnixMs(frame.at_unix_ms)),
      el('span', { class: 'event-seq' }, `#${fmtId(frame.seq)}`),
      el('span', { class: 'event-kind' }, kind),
      el('span', { class: 'event-summary' }, summarize(kind, frame.data || {})),
    );
  });

  setChildren(container, el('div', { class: 'event-log-scroll' }, rows));
}

function summarize(kind, data) {
  switch (kind) {
    case 'member_joined': return `${str(data.name, '?')} joined`;
    case 'member_left': return `${str(data.name, '?')} left`;
    case 'health_changed': {
      const name = str(data.name, '?');
      const health = str(data.health, '?');
      // prev_health === health is a VALID, expected shape (internal/coordinator's dwell
      // logic, wired through healthData in internal/dashboard/wire.go): a fired
      // sustained-degradation dwell, or its recovery counterpart clearing, rides this
      // same event with both fields equal, because the node's LIVENESS did not change —
      // only its quality verdict did. Rendering that as "degraded → degraded" reads as a
      // no-op; render it as the verdict it is instead.
      if (data.prev_health !== undefined && data.prev_health === data.health) {
        return health === 'healthy'
          ? `${name}: degradation cleared (${health})`
          : `${name}: sustained ${health} (verdict, not a transition)`;
      }
      // prev_health being "" would mean a genuinely unknown prior value. The coordinator
      // now always sends a real Event.PrevHealth (§15.14), so this path should not fire
      // in normal operation against this server build — kept only as defensive handling
      // against a malformed/older payload, not as an expected case.
      const prev = data.prev_health ? str(data.prev_health) : '(first observation)';
      return `${name}: ${prev} → ${health}`;
    }
    case 'topology': return `root=${str(data.root, '?')} outcome=${str(data.outcome, '?')}${data.reason ? ` (${data.reason})` : ''}`;
    // §9.4 v2.6: `self_promoted` removed from the wire — EventReparent is only ever emitted
    // from the self-promotion path, so the label is unconditional now, implied by the kind.
    case 'reparent': return `${str(data.name, '?')}: ${str(data.from, '?')} → ${str(data.to, '?')} (self-promoted)`;
    case 'failover': return `${str(data.name, '?')} failed — orphans: ${Array.isArray(data.orphans) ? data.orphans.join(', ') || 'none' : '?'}`;
    case 'election': {
      // An empty `coordinator` is ambiguous on its own — vacant, or the arbiter itself
      // hosting — and this event's own `reason` is what resolves it (isArbiterHosting,
      // format.js). Rendering it unconditionally as "(vacant)" was the same bug class as
      // the header's: a demotion TO the arbiter would read as a vacancy that never
      // happened.
      const to = data.coordinator ? str(data.coordinator) : (isArbiterHosting(data.coordinator, data.reason) ? 'the arbiter' : '(vacant)');
      // `prev` has no equivalent disambiguator: it names the OUTGOING coordinator, and
      // this event's `reason` only explains the CURRENT transition, not whatever the
      // previous one was. A blank `prev` renders blank rather than guessing "(vacant)"
      // or "the arbiter" — same principle as `to`, applied honestly where the wire
      // genuinely does not resolve it.
      return `epoch ${fmtId(data.epoch)}: ${str(data.prev, '?')} → ${to} (${str(data.reason, '?')})`;
    }
    case 'announce_repair': return `${str(data.name, '?')}: peer at epoch ${fmtId(data.peer_epoch)} vs meet epoch ${fmtId(data.meet_epoch)} — ${data.resolved ? 'resolved' : 'repairing'}`;
    // total is that peer's cumulative refusal count this session (coordinator.Event.Count)
    // — the informative part of this event, and NOT the meet-wide sum shown elsewhere
    // (see state.js applyDelta for why those two numbers are never mixed).
    case 'stale_rejected': return `${str(data.name, '?')} rejected (epoch ${fmtId(data.epoch)}, rev ${fmtId(data.rev)}, total ${fmtId(data.total)}): ${str(data.reason)}`;
    case 'settling': return `waiting for ${Array.isArray(data.waiting) ? data.waiting.join(', ') || 'telemetry' : 'telemetry'}`;
    case 'unbuildable': return str(data.reason, 'no reason given');
    case 'demo': return `${str(data.action, '?')} → ${str(data.target, '?')} (by ${str(data.by_remote_addr, 'unknown')})`;
    case 'snapshot': return 'full snapshot received';
    case 'pong': return 'keepalive';
    default: return '';
  }
}
