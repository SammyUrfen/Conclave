// meetsList.js — §9.5 "Meets" panel: list + create. Creating reveals join.peer_command
// with a copy button (the rendezvous, §9.3).

import { el, setChildren } from '../dom.js';
import { fmtUnixMs, fmtId, str } from '../format.js';

const MEET_ID_HINT = 'lowercase letters, digits, "-", "_" — or leave blank to auto-generate';

export function render(container, state, actions) {
  const { meets, meetsLoading, meetsError, serverStatus, lastCreated } = state;

  const createForm = el(
    'form',
    {
      class: 'create-form',
      onsubmit: (e) => {
        e.preventDefault();
        const input = e.target.elements.meetId;
        actions.onCreateMeet(input.value.trim());
        input.value = '';
      },
    },
    el('input', { name: 'meetId', type: 'text', placeholder: MEET_ID_HINT, class: 'meet-id-input', maxlength: '64' }),
    el('button', { type: 'submit', class: 'btn' }, 'Create meet'),
  );

  const nodes = [
    el('h2', null, 'Meets'),
    createForm,
  ];

  if (lastCreated) {
    nodes.push(renderCreated(lastCreated, actions));
  }

  if (serverStatus !== 'ok') {
    nodes.push(
      el('div', { class: 'empty-state' },
        'Not connected to an arbiter. Set the server address above and click Connect.'),
    );
  } else if (meetsLoading && meets.length === 0) {
    nodes.push(el('div', { class: 'empty-state' }, 'Loading meets…'));
  } else if (meetsError) {
    nodes.push(el('div', { class: 'error-banner' }, `Could not load meets: ${meetsError}`));
  } else if (meets.length === 0) {
    nodes.push(el('div', { class: 'empty-state' }, 'No meets yet. Create one above.'));
  } else {
    nodes.push(renderTable(meets, actions));
  }

  setChildren(container, nodes);
}

function renderCreated(created, actions) {
  const cmd = str(created.join && created.join.peer_command);
  return el(
    'div',
    { class: 'created-panel' },
    el('div', { class: 'created-panel-head' },
      el('strong', null, `Meet "${str(created.id)}" created.`),
      el('button', { class: 'btn btn-sm btn-ghost', onclick: actions.onDismissCreated }, 'dismiss'),
    ),
    el('p', { class: 'fg-muted' }, 'Give a participant this command to join:'),
    el('div', { class: 'copy-row' },
      el('code', { class: 'copy-target' }, cmd || '(no join command returned)'),
      el('button', {
        class: 'btn btn-sm',
        onclick: () => copyToClipboard(cmd),
      }, 'Copy'),
      el('button', { class: 'btn btn-sm btn-ghost', onclick: () => actions.onOpenMeet(str(created.id)) }, 'View subnet →'),
    ),
  );
}

function copyToClipboard(text) {
  if (!text) return;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
  } else {
    fallbackCopy(text);
  }
}

function fallbackCopy(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.select();
  try {
    document.execCommand('copy');
  } catch {
    /* best effort only */
  }
  document.body.removeChild(ta);
}

function renderTable(meets, actions) {
  const rows = meets.map((m) => {
    const id = str(m.id, '(unknown)');
    return el(
      'tr',
      { class: 'meet-row', onclick: () => actions.onOpenMeet(id) },
      el('td', null, el('a', { href: `#/meets/${encodeURIComponent(id)}`, onclick: (e) => e.preventDefault() }, id)),
      el('td', { class: 'num' }, typeof m.members === 'number' ? String(m.members) : '—'),
      el('td', { class: 'num' }, fmtId(m.epoch)),
      el('td', null, str(m.coordinator) || el('span', { class: 'fg-muted' }, 'none')),
      el('td', null, fmtUnixMs(m.created_at_unix_ms)),
    );
  });

  return el(
    'div', { class: 'table-scroll' },
    el(
      'table', { class: 'meets-table' },
      el('thead', null,
        el('tr', null,
          el('th', null, 'id'), el('th', null, 'members'), el('th', null, 'epoch'),
          el('th', null, 'coordinator'), el('th', null, 'created'),
        )),
      el('tbody', null, rows),
    ),
  );
}
