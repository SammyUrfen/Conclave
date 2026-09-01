// serverBar.js — §9.5 "Server bar": server URL (default localhost:9000, persisted),
// connect/disconnect, and a live/stale indicator.

import { el, setChildren } from '../dom.js';

const STATUS_LABEL = {
  unknown: 'not connected',
  connecting: 'connecting…',
  ok: 'connected',
  error: 'connection failed',
};

const STATUS_CLASS = {
  unknown: 'dot-muted',
  connecting: 'dot-warn',
  ok: 'dot-ok',
  error: 'dot-crit',
};

export function render(container, state, actions) {
  const { serverUrlRaw, serverStatus, serverError, demoEnabled, meetId, meetSocketStatus } = state;

  const statusDot = el('span', { class: `status-dot ${STATUS_CLASS[serverStatus] || 'dot-muted'}` });
  const statusText = el('span', { class: 'status-text' }, STATUS_LABEL[serverStatus] || serverStatus);

  const wsStatusNode = meetId
    ? el(
        'span',
        { class: 'ws-status' },
        ' · meet stream: ',
        el('span', { class: `status-dot ${wsStatusClass(meetSocketStatus)}` }),
        el('span', { class: 'status-text' }, meetSocketStatus),
      )
    : null;

  const input = el('input', {
    id: 'server-url',
    type: 'text',
    class: 'server-input',
    '.value': serverUrlRaw,
    placeholder: DEFAULT_HINT,
    spellcheck: 'false',
    autocomplete: 'off',
    onchange: (e) => actions.onServerUrlChange(e.target.value),
  });

  const nodes = [
    el(
      'a',
      { class: 'brand', href: '#/', onclick: (e) => { e.preventDefault(); actions.onNavigateMeets(); } },
      'conclave',
    ),
    el('span', { class: 'brand-sub' }, 'arbiter dashboard'),
    el('div', { class: 'server-field' },
      el('label', { for: 'server-url', class: 'server-label' }, 'server'),
      input,
      el('button', { class: 'btn btn-sm', onclick: actions.onConnect }, serverStatus === 'ok' ? 'Reconnect' : 'Connect'),
      el('button', { class: 'btn btn-sm btn-ghost', onclick: actions.onDisconnect }, 'Disconnect'),
    ),
    el('div', { class: 'server-status' }, statusDot, statusText, wsStatusNode),
  ];

  if (demoEnabled) {
    nodes.push(el('span', { class: 'pill pill-warn', title: 'The gated -demo control surface is registered on this server.' }, 'demo mode'));
  }

  if (serverError) {
    nodes.push(el('div', { class: 'server-error', role: 'alert' }, serverError));
  }

  setChildren(container, nodes);
}

const DEFAULT_HINT = 'localhost:9000 (or 127.0.0.1:9000, or https://host)';

function wsStatusClass(status) {
  if (status === 'live') return 'dot-ok';
  if (status === 'stale' || status === 'connecting' || status === 'reconnecting') return 'dot-warn';
  if (status === 'gone' || status === 'rejected') return 'dot-crit';
  return 'dot-muted';
}
