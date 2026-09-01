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
    case 'health_changed': return `${str(data.name, '?')}: ${str(data.prev_health, '?')} → ${str(data.health, '?')}`;
    case 'topology': return `root=${str(data.root, '?')} outcome=${str(data.outcome, '?')}${data.reason ? ` (${data.reason})` : ''}`;
    case 'reparent': return `${str(data.name, '?')}: ${str(data.from, '?')} → ${str(data.to, '?')}${data.self_promoted ? ' (self-promoted)' : ''}`;
    case 'failover': return `${str(data.name, '?')} failed — orphans: ${Array.isArray(data.orphans) ? data.orphans.join(', ') || 'none' : '?'}`;
    case 'election': return `epoch ${fmtId(data.epoch)}: ${str(data.prev, '?')} → ${str(data.coordinator, '?')} (${str(data.reason, '?')})`;
    case 'stale_rejected': return `${str(data.name, '?')} rejected (epoch ${fmtId(data.epoch)}, rev ${fmtId(data.rev)}): ${str(data.reason)}`;
    case 'settling': return `waiting for ${Array.isArray(data.waiting) ? data.waiting.join(', ') || 'telemetry' : 'telemetry'}`;
    case 'unbuildable': return str(data.reason, 'no reason given');
    case 'demo': return `${str(data.action, '?')} → ${str(data.target, '?')} (by ${str(data.by_remote_addr, 'unknown')})`;
    case 'snapshot': return 'full snapshot received';
    case 'pong': return 'keepalive';
    default: return '';
  }
}
