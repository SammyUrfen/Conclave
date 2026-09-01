// eventLog.js — §9.5 "Event log": the envelope stream, newest first, colour-coded by kind.

import { el, setChildren } from '../dom.js';
import { fmtUnixMs, fmtId, str } from '../format.js';

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
      // prev_health is "" for the first transition after a server restart — the dashboard
      // only remembers a node's last-published value in-process, so there is honestly
      // nothing prior to show, not a malformed field.
      const prev = data.prev_health ? str(data.prev_health) : '(first observation)';
      return `${str(data.name, '?')}: ${prev} → ${str(data.health, '?')}`;
    }
    case 'topology': return `root=${str(data.root, '?')} outcome=${str(data.outcome, '?')}${data.reason ? ` (${data.reason})` : ''}`;
    // §9.4 v2.6: `self_promoted` removed from the wire — EventReparent is only ever emitted
    // from the self-promotion path, so the label is unconditional now, implied by the kind.
    case 'reparent': return `${str(data.name, '?')}: ${str(data.from, '?')} → ${str(data.to, '?')} (self-promoted)`;
    case 'failover': return `${str(data.name, '?')} failed — orphans: ${Array.isArray(data.orphans) ? data.orphans.join(', ') || 'none' : '?'}`;
    case 'election': {
      // Coordinator is "" when Reason is "vacated" (§6.8) — the epoch still bumps to
      // fence the old coordinator even though nobody replaces it. Render that plainly
      // rather than as a blank name after the arrow.
      const to = data.coordinator ? str(data.coordinator) : '(vacant)';
      return `epoch ${fmtId(data.epoch)}: ${str(data.prev, '?')} → ${to} (${str(data.reason, '?')})`;
    }
    case 'announce_repair': return `${str(data.name, '?')}: peer at epoch ${fmtId(data.peer_epoch)} vs meet epoch ${fmtId(data.meet_epoch)} — ${data.resolved ? 'resolved' : 'repairing'}`;
    case 'stale_rejected': return `${str(data.name, '?')} rejected (epoch ${fmtId(data.epoch)}, rev ${fmtId(data.rev)}): ${str(data.reason)}`;
    case 'settling': return `waiting for ${Array.isArray(data.waiting) ? data.waiting.join(', ') || 'telemetry' : 'telemetry'}`;
    case 'unbuildable': return str(data.reason, 'no reason given');
    case 'demo': return `${str(data.action, '?')} → ${str(data.target, '?')} (by ${str(data.by_remote_addr, 'unknown')})`;
    case 'snapshot': return 'full snapshot received';
    case 'pong': return 'keepalive';
    default: return '';
  }
}
