// subnetTree.js — the inline-SVG relay-tree renderer (§9.5 "Subnet" panel).
//
// No graph library: this project's ethos is building the primitive, and the tree here is
// small (a handful to a few dozen nodes for a portfolio demo) so a from-scratch layered
// layout is the right tool, not an excuse to import one.
//
// Colour contract (frozen wording from §9.5):
//   "Coordinator badge = --accent-primary, relay badge = --accent-secondary, leaf = --surface.
//    Edges --edge; a degraded node's edge --warn; a failover flash --crit.
//    Two independent badges per node, never one merged role.
//    Backup parents drawn as a dashed edge."
// So the node body itself is always the neutral --surface fill (that IS "leaf" styling); a
// coordinator and/or a relay gets a small additional badge mark in its own colour — a
// coordinator+relay node shows BOTH marks, never one blended colour standing for both roles.

import { el } from '../dom.js';
import { fmtDepth } from '../format.js';

const NODE_R = 22;
const NODE_SPACING = 110;
const LEVEL_HEIGHT = 100;
const PAD = 60;

const SVG_NS = 'http://www.w3.org/2000/svg';
function svgEl(tag, attrs = {}, children = []) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, v);
  for (const c of children) if (c != null) node.appendChild(c);
  return node;
}

function healthColorVar(health) {
  if (health === 'healthy') return 'var(--ok)';
  if (health === 'degraded') return 'var(--warn)';
  if (health === 'gone') return 'var(--crit)';
  return 'var(--fg-muted)'; // unknown enum value, per §9.4a: render neutrally, never crash
}

/** Build the parent→children adjacency, preferring `edges[]` and falling back to `node.children`. */
function buildAdjacency(meet) {
  const nodes = Array.isArray(meet.nodes) ? meet.nodes : [];
  const byName = new Map();
  for (const n of nodes) if (n && typeof n.name === 'string') byName.set(n.name, n);

  const childrenMap = new Map();
  for (const name of byName.keys()) childrenMap.set(name, []);

  const edges = Array.isArray(meet.edges) ? meet.edges : [];
  if (edges.length > 0) {
    for (const e of edges) {
      if (!e || typeof e.parent !== 'string' || typeof e.child !== 'string') continue;
      if (!childrenMap.has(e.parent)) childrenMap.set(e.parent, []);
      childrenMap.get(e.parent).push(e.child);
    }
  } else {
    for (const n of byName.values()) {
      if (Array.isArray(n.children)) childrenMap.set(n.name, n.children.filter((c) => byName.has(c)));
    }
  }
  return { byName, childrenMap };
}

/** Postorder layered layout. Cycle-guarded — server data is untrusted, never crash on a loop. */
function layout(rootName, childrenMap) {
  const positions = new Map();
  let cursor = 0;
  const visited = new Set();

  function visit(name, depth) {
    if (visited.has(name)) return null; // cycle guard
    visited.add(name);
    const kids = (childrenMap.get(name) || []).filter((k) => !visited.has(k));
    if (kids.length === 0) {
      const x = cursor++ * NODE_SPACING;
      positions.set(name, { x, y: depth * LEVEL_HEIGHT });
      return x;
    }
    const kidXs = [];
    for (const k of kids) {
      const x = visit(k, depth + 1);
      if (x != null) kidXs.push(x);
    }
    const x = kidXs.length ? (Math.min(...kidXs) + Math.max(...kidXs)) / 2 : cursor++ * NODE_SPACING;
    positions.set(name, { x, y: depth * LEVEL_HEIGHT });
    return x;
  }

  if (rootName) visit(rootName, 0);
  return positions;
}

// SVG text/title elements need a text child assigned via textContent, not an attribute.
function withText(svgNode, text) {
  svgNode.textContent = text;
  return svgNode;
}

function badgeGroup(cx, cy, color, letter, title) {
  const g = svgEl('g', {});
  g.appendChild(svgEl('circle', { cx, cy, r: 8, fill: color, stroke: 'var(--bg)', 'stroke-width': 1.5 }));
  g.appendChild(withText(svgEl('text', { x: cx, y: cy + 3, 'text-anchor': 'middle', class: 'badge-letter' }), letter));
  g.appendChild(withText(svgEl('title', {}), title));
  return g;
}

/**
 * Render the subnet tree into `container` (a plain HTML element — this function owns an
 * inline <svg> plus an "unattached" strip beneath it).
 *
 * @param {HTMLElement} container
 * @param {object|null} meet - the meet snapshot, or null while still loading.
 * @param {{name:string, until:number}|null} flashNode - transient failover highlight.
 * @param {(name:string)=>void} onSelectNode
 */
export function render(container, meet, flashNode, onSelectNode) {
  container.replaceChildren();

  if (!meet) {
    container.appendChild(el('div', { class: 'empty-state' }, 'Waiting for the first snapshot…'));
    return;
  }

  const { byName, childrenMap } = buildAdjacency(meet);
  const rootName = typeof meet.root === 'string' && byName.has(meet.root) ? meet.root : null;

  // Reachability from root, defensively — a node can claim depth -1 OR simply be absent from
  // the edge/children graph the root reaches; either way it belongs in "unattached", not
  // silently dropped.
  const reachable = new Set();
  if (rootName) {
    const stack = [rootName];
    while (stack.length) {
      const n = stack.pop();
      if (reachable.has(n)) continue;
      reachable.add(n);
      for (const c of childrenMap.get(n) || []) stack.push(c);
    }
  }
  const unattached = [...byName.values()].filter((n) => !reachable.has(n.name));

  if (!rootName || reachable.size === 0) {
    container.appendChild(el('div', { class: 'empty-state' }, 'No rooted subnet to draw yet.'));
    if (unattached.length) container.appendChild(renderUnattached(unattached, onSelectNode));
    return;
  }

  const positions = layout(rootName, childrenMap);
  const xs = [...positions.values()].map((p) => p.x);
  const ys = [...positions.values()].map((p) => p.y);
  const minX = Math.min(...xs) - PAD, maxX = Math.max(...xs) + PAD;
  const minY = Math.min(...ys) - PAD, maxY = Math.max(...ys) + PAD + 20;
  const width = maxX - minX;
  const height = maxY - minY;

  const backups = Array.isArray(meet.backups) ? meet.backups : [];
  const svgChildren = [];

  // Edges first (under nodes). Colour: default --edge; child unhealthy escalates it; a
  // recent failover on the child gets a --crit flash on top of that.
  for (const name of reachable) {
    if (name === rootName) continue;
    const node = byName.get(name);
    if (!node) continue;
    const parentName = node.parent;
    const from = positions.get(parentName);
    const to = positions.get(name);
    if (!from || !to) continue;
    const flashing = flashNode && flashNode.name === name && flashNode.until > Date.now();
    const color = flashing ? 'var(--crit)' : node.health === 'degraded' ? 'var(--warn)' : node.health === 'gone' ? 'var(--crit)' : 'var(--edge)';
    svgChildren.push(svgEl('line', {
      x1: from.x - minX, y1: from.y - minY, x2: to.x - minX, y2: to.y - minY,
      stroke: color, 'stroke-width': flashing ? 3 : 2, class: flashing ? 'edge-flash' : '',
    }));
  }

  // Backup edges — dashed, drawn from the node to its backup parent.
  for (const b of backups) {
    if (!b || typeof b.node !== 'string' || typeof b.parent !== 'string') continue;
    const from = positions.get(b.node);
    const to = positions.get(b.parent);
    if (!from || !to) continue;
    svgChildren.push(svgEl('line', {
      x1: from.x - minX, y1: from.y - minY, x2: to.x - minX, y2: to.y - minY,
      stroke: 'var(--edge)', 'stroke-width': 1.5, 'stroke-dasharray': '5,5', opacity: '0.6',
    }));
  }

  // Nodes on top.
  for (const name of reachable) {
    const node = byName.get(name);
    const pos = positions.get(name);
    if (!node || !pos) continue;
    const cx = pos.x - minX, cy = pos.y - minY;
    const roles = Array.isArray(node.roles) ? node.roles : [];
    const isCoordinator = roles.includes('coordinator');
    const isRelay = roles.includes('relay');

    const g = svgEl('g', { class: 'subnet-node', tabindex: '0', role: 'button' });
    g.appendChild(svgEl('circle', {
      cx, cy, r: NODE_R, fill: 'var(--surface)',
      stroke: healthColorVar(node.health), 'stroke-width': 2.5,
    }));
    g.appendChild(withText(svgEl('text', { x: cx, y: cy + 4, 'text-anchor': 'middle', class: 'node-label' }), safeInitial(node.name)));

    if (isCoordinator) g.appendChild(badgeGroup(cx - NODE_R + 4, cy - NODE_R + 4, 'var(--accent-primary)', 'C', 'coordinator'));
    if (isRelay) g.appendChild(badgeGroup(cx + NODE_R - 4, cy - NODE_R + 4, 'var(--accent-secondary)', 'R', 'relay'));

    g.appendChild(withText(svgEl('text', { x: cx, y: cy + NODE_R + 16, 'text-anchor': 'middle', class: 'node-name' }), node.name || '(unnamed)'));

    g.addEventListener('click', () => onSelectNode(node.name));
    g.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') onSelectNode(node.name); });
    svgChildren.push(g);
  }

  const svg = svgEl('svg', {
    viewBox: `0 0 ${width} ${height}`,
    width: String(width),
    height: String(height),
    class: 'subnet-svg',
  }, svgChildren);

  const scroller = el('div', { class: 'subnet-scroll' }, svg);
  container.appendChild(renderLegend());
  container.appendChild(scroller);
  if (unattached.length) container.appendChild(renderUnattached(unattached, onSelectNode));
}

function safeInitial(name) {
  if (typeof name !== 'string' || name.length === 0) return '?';
  return name[0].toUpperCase();
}

function renderLegend() {
  return el('div', { class: 'subnet-legend' },
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-swatch legend-coordinator' }), 'coordinator'),
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-swatch legend-relay' }), 'relay'),
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-swatch legend-leaf' }), 'leaf'),
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-line legend-edge' }), 'edge'),
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-line legend-backup' }), 'backup (dashed)'),
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-dot dot-warn' }), 'degraded'),
    el('span', { class: 'legend-item' }, el('span', { class: 'legend-dot dot-crit' }), 'gone / failover'),
  );
}

function renderUnattached(nodes, onSelectNode) {
  return el('div', { class: 'unattached-panel' },
    el('h4', null, `Unattached (${nodes.length})`),
    el('div', { class: 'unattached-chips' },
      nodes.map((n) => el('button', {
        class: 'chip',
        onclick: () => onSelectNode(n.name),
        title: `depth: ${fmtDepth(n.depth)}`,
      },
        el('span', { class: `status-dot ${n.health === 'healthy' ? 'dot-ok' : n.health === 'degraded' ? 'dot-warn' : 'dot-crit'}` }),
        n.name || '(unnamed)',
      )),
    ),
  );
}
