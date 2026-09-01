// meetDetail.js — composes the Subnet, Node detail, Event log, Build-state banner, and
// Epoch/rev panels for one meet (§9.5). This is the view that owns the live WebSocket.

import { el, setChildren } from '../dom.js';
import { fmtId, fmtDepth, fmtKbps, fmtMs, fmtPct, fmtFitness, fmtUnixMs, str } from '../format.js';
import * as subnetTree from './subnetTree.js';
import * as eventLog from './eventLog.js';
import * as demoControls from './demoControls.js';

const FATAL_MESSAGE = {
  meet_deleted: 'This meet was deleted on the server.',
  meet_not_found: 'This meet no longer exists.',
  policy_violation: 'The server rejected this connection (origin policy). It will not retry — check -allowed-origins.',
  bad_server_url: 'The configured server address is invalid.',
};

export function render(container, state, actions) {
  const { meetId, meet, meetSocketStatus, meetFatal, buildState, events, flashNode, selectedNodeName } = state;

  const nodes = [
    el('div', { class: 'meet-toolbar' },
      el('button', { class: 'btn btn-sm btn-ghost', onclick: actions.onBack }, '← meets'),
      el('h2', null, `Meet: ${meetId}`),
      el('button', { class: 'btn btn-sm', onclick: actions.onResync }, 'Resync'),
    ),
  ];

  if (meetFatal) {
    nodes.push(el('div', { class: 'error-banner', role: 'alert' },
      FATAL_MESSAGE[meetFatal.reason] || `Connection stopped: ${meetFatal.reason}`,
      meetFatal.reason !== 'policy_violation' ? el('button', { class: 'btn btn-sm', onclick: actions.onBack }, 'Back to meets') : null,
    ));
  }

  if (!meet && !meetFatal) {
    nodes.push(el('div', { class: 'empty-state' }, connectingLabel(meetSocketStatus)));
  }

  if (meet) {
    nodes.push(renderEpochBar(meet, meetSocketStatus));
    nodes.push(renderBuildBanner(buildState));

    const subnetContainer = el('div', { class: 'subnet-container' });
    const detailContainer = el('div', { class: 'node-detail-container' });
    const demoContainer = el('div', { class: 'demo-container' });

    nodes.push(
      el('div', { class: 'meet-columns' },
        el('div', { class: 'subnet-col' }, subnetContainer),
        el('div', { class: 'detail-col' }, detailContainer, demoContainer),
      ),
    );

    const eventContainer = el('div', { class: 'event-log-container' });
    nodes.push(el('div', { class: 'event-col' }, el('h3', null, 'Events'), eventContainer));

    setChildren(container, nodes);

    subnetTree.render(subnetContainer, meet, flashNode, actions.onSelectNode);
    renderNodeDetail(detailContainer, meet, selectedNodeName);
    demoControls.render(demoContainer, state, actions);
    eventLog.render(eventContainer, events);
    return;
  }

  setChildren(container, nodes);
}

function connectingLabel(status) {
  if (status === 'connecting') return 'Connecting to the meet stream…';
  if (status === 'reconnecting') return 'Reconnecting…';
  return 'Waiting for the meet stream…';
}

function renderEpochBar(meet, wsStatus) {
  // §6.8/§9.4: stale_rejected legitimately reads 0 (Phase 6 wiring for this counter is
  // incomplete server-side) — that is honest data, not a fault, so 0 stays in the neutral
  // colour rather than a warning colour.
  const staleRejected = typeof meet.stale_rejected === 'number' ? meet.stale_rejected : 0;
  // Coordinator is "" for BOTH "vacant" and "the arbiter itself is coordinating" — the
  // two are distinguished by arbiter_is_coordinator, never by testing the name for "".
  // (arbiter.Meet.Coordinator's own doc comment says this explicitly; getting it backwards
  // would render an arbiter-coordinated meet as leaderless, which it is not.)
  const coordName = str(meet.coordinator);
  const coordLabel = meet.arbiter_is_coordinator
    ? el('span', { class: 'pill pill-muted', title: 'The arbiter itself is hosting the coordinator role for this meet (no peer coordinator).' }, 'the arbiter')
    : (coordName || '(vacant)'); // §6.8 ReasonVacated: no coordinator, not a missing field
  // §9.4b: Converged/Diverged expose the gap between the coordinator's INTENDED tree and
  // what peers REALIZED from their own heartbeats — convergence lag, a failed apply, or a
  // fenced-out peer, invisible if the UI only ever shows the number it fetched last.
  const diverged = Array.isArray(meet.diverged) ? meet.diverged : [];
  const convergedNode = meet.converged === false || diverged.length > 0
    ? el('span', { class: 'epoch-item text-warn', title: 'Realized parent differs from the published tree for these peers.' }, `diverged: ${diverged.length ? diverged.join(', ') : '(unspecified)'}`)
    : el('span', { class: 'epoch-item fg-muted' }, 'converged');
  return el('div', { class: 'epoch-bar tabular-nums' },
    el('span', { class: 'epoch-item' }, 'epoch ', el('strong', null, fmtId(meet.epoch))),
    el('span', { class: 'epoch-item' }, 'rev ', el('strong', null, fmtId(meet.rev))),
    el('span', { class: 'epoch-item' }, 'coordinator ', el('strong', null, coordLabel)),
    convergedNode,
    el('span', { class: `epoch-item ${staleRejected > 0 ? 'text-warn' : 'fg-muted'}` }, `stale rejected: ${staleRejected}`),
    el('span', { class: `epoch-item ws-inline status-dot ${wsStatus === 'live' ? 'dot-ok' : wsStatus === 'stale' ? 'dot-warn' : 'dot-muted'}` }),
    el('span', { class: 'epoch-item fg-muted' }, wsStatus),
  );
}

// §9.5 "Build state": three distinct renderings that must never be confused.
function renderBuildBanner(bs) {
  if (!bs || bs.kind === 'built') return el('div', { hidden: 'true' });
  if (bs.kind === 'settling') {
    return el('div', { class: 'build-banner banner-settling' },
      `settling — waiting for telemetry from ${bs.waiting && bs.waiting.length ? bs.waiting.join(', ') : 'the newest peers'}`);
  }
  if (bs.kind === 'relaxed') {
    return el('div', { class: 'build-banner banner-relaxed', title: bs.reason || '' },
      'rebuilt from scratch — stability preference dropped', bs.reason ? el('span', { class: 'fg-muted' }, ` (${bs.reason})`) : null);
  }
  if (bs.kind === 'unbuildable') {
    return el('div', { class: 'build-banner banner-unbuildable', role: 'alert' },
      `unbuildable: ${bs.reason || 'no reason given'}`);
  }
  return el('div', { class: 'build-banner' }, `build state: ${bs.kind}`); // unknown value ⇒ neutral render
}

function renderNodeDetail(container, meet, selectedName) {
  const nodesList = Array.isArray(meet.nodes) ? meet.nodes : [];
  const node = selectedName ? nodesList.find((n) => n.name === selectedName) : null;

  if (!node) {
    setChildren(container, el('div', { class: 'node-detail empty-state' }, 'Click a node in the subnet to see its telemetry.'));
    return;
  }

  const roles = Array.isArray(node.roles) ? node.roles : [];
  const rows = [
    ['role(s)', roles.length ? roles.join(', ') : 'leaf'],
    ['health', str(node.health, 'unknown')],
    ['parent', str(node.parent) || '(root)'],
    ['backup', node.backup === '' ? 'no backup (root child)' : (str(node.backup) || '—')],
    ['children', Array.isArray(node.children) && node.children.length ? node.children.join(', ') : 'none'],
    ['depth', fmtDepth(node.depth)],
    ['upload', fmtKbps(node.upload_kbps)],
    ['nat', str(node.nat, 'unknown')],
    ['rtt to server', fmtMs(node.rtt_server_ms)],
    ['loss', fmtPct(node.loss_pct)],
    ['cpu', fmtPct(node.cpu_pct)],
    ['fitness', fmtFitness(node.fitness)],
    ['last heartbeat seq', fmtId(node.last_beat_seq)],
    ['last heartbeat at', fmtUnixMs(node.last_beat_unix_ms)],
  ];

  setChildren(container, el('div', { class: 'node-detail tabular-nums' },
    el('h3', null, str(node.name, '(unnamed)')),
    el('table', { class: 'detail-table' },
      rows.map(([k, v]) => el('tr', null, el('td', { class: 'detail-key' }, k), el('td', { class: 'detail-val' }, v))),
    ),
  ));
}
