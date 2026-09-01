// demoControls.js — §9.6 gated demo surface. Rendered ONLY when state.demoEnabled is true.
// §9.3/§9.4a define no capability flag for whether -demo is on, and the real api.js hard-codes
// `hasDemoCapability()` to false rather than probing with a destructive call — absent means
// off. This panel is therefore dark against a real server until the server positively
// advertises the capability (requested); it only renders today in fixture (?mock=1) mode.

import { el, setChildren } from '../dom.js';
import { str } from '../format.js';

// This panel is rebuilt from scratch on every store change (this app's whole render
// strategy — see state.js's top comment), which would otherwise silently drop an
// in-progress dropdown pick or the last action's status line the instant a live WS frame
// arrives. Demo actions are rare/deliberate clicks, so losing that mid-interaction is worse
// than the small amount of module-level UI state it takes to preserve it across rebuilds.
let pendingEvictName = '';
let pendingElectName = '';
let lastStatusText = '';
let lastMeetId = null;

export function render(container, state, actions) {
  if (state.meetId !== lastMeetId) {
    lastMeetId = state.meetId;
    pendingEvictName = '';
    pendingElectName = '';
    lastStatusText = '';
  }

  if (!state.demoEnabled) {
    setChildren(container, []);
    container.hidden = true;
    return;
  }
  container.hidden = false;

  const nodeNames = state.meet && Array.isArray(state.meet.nodes)
    ? state.meet.nodes.map((n) => str(n.name)).filter(Boolean)
    : [];

  const evictSelect = el('select', {
    class: 'demo-select',
    '.value': nodeNames.includes(pendingEvictName) ? pendingEvictName : '',
    onchange: (e) => { pendingEvictName = e.target.value; },
  },
    el('option', { value: '' }, '(choose a peer)'),
    nodeNames.map((n) => el('option', { value: n }, n)),
  );

  const electSelect = el('select', {
    class: 'demo-select',
    '.value': nodeNames.includes(pendingElectName) ? pendingElectName : '',
    onchange: (e) => { pendingElectName = e.target.value; },
  },
    el('option', { value: '' }, '(best candidate)'),
    nodeNames.map((n) => el('option', { value: n }, n)),
  );

  const status = el('span', { class: 'demo-status fg-muted' }, lastStatusText);

  const run = async (fn, label) => {
    lastStatusText = `${label}…`;
    status.textContent = lastStatusText;
    try {
      await fn();
      lastStatusText = `${label}: ok`;
    } catch (err) {
      lastStatusText = `${label} failed: ${err.message || err}`;
    }
    status.textContent = lastStatusText;
  };

  setChildren(container, [
    el('h4', null, 'Demo controls'),
    el('p', { class: 'fg-muted' }, 'Destructive, unauthenticated, gated behind -demo on the server. Advisory attribution only.'),
    el('div', { class: 'demo-row' },
      evictSelect,
      el('button', {
        class: 'btn btn-sm btn-danger',
        onclick: () => run(() => actions.onEvict(evictSelect.value), 'evict'),
      }, 'Evict'),
    ),
    el('div', { class: 'demo-row' },
      electSelect,
      el('button', {
        class: 'btn btn-sm',
        onclick: () => run(() => actions.onElect(electSelect.value), 'force election'),
      }, 'Force election'),
    ),
    status,
  ]);
}
