// state.js — the app's single store, plus the actions that mutate it.
//
// A tiny pub-sub store, not a framework: `getState()`, `setState(patch)`, `subscribe(fn)`.
// Given the scale of this UI (one meet's tree, a bounded event log) a full re-render of the
// affected panel on every state change is simple, correct, and easy to audit — which matters
// more here than shaving DOM writes (Correctness > ... > Performance, per CLAUDE.md).

import * as realApi from './api.js';
import * as mockApi from './mockApi.js';
import { EventSocket } from './api.js';
import { MockEventSocket } from './mockApi.js';
import { isArbiterHosting } from './format.js';

const STORAGE_KEY = 'conclave.serverUrl';
const DEFAULT_SERVER = 'localhost:9000';
const MAX_EVENTS = 300; // bounded event log — an unbounded one is a slow memory leak by design

const USE_MOCK = new URLSearchParams(window.location.search).get('mock') === '1';
const api = USE_MOCK ? mockApi : realApi;
const SocketImpl = USE_MOCK ? MockEventSocket : EventSocket;

function loadStoredServerUrl() {
  try {
    return window.localStorage.getItem(STORAGE_KEY) || DEFAULT_SERVER;
  } catch {
    return DEFAULT_SERVER; // localStorage can throw in a locked-down/private context
  }
}

function saveServerUrl(url) {
  try {
    window.localStorage.setItem(STORAGE_KEY, url);
  } catch {
    /* non-fatal — the field still works for this session */
  }
}

function createStore(initial) {
  let state = initial;
  const listeners = new Set();
  return {
    getState: () => state,
    // `patch` may be a plain object OR a `(prevState) => partialPatch` updater — either way
    // the result is MERGED into state, never used to replace it wholesale. (A prior version
    // of this function did `state = patch(state)` for the function form, which silently
    // discarded every field the updater didn't mention — every call site here computes a
    // small partial patch from `s`, not a full replacement state.)
    setState(patch) {
      const partial = typeof patch === 'function' ? patch(state) : patch;
      state = { ...state, ...partial };
      for (const fn of listeners) fn(state);
    },
    subscribe(fn) {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
  };
}

export const store = createStore({
  serverUrlRaw: loadStoredServerUrl(),
  serverStatus: 'unknown', // unknown | connecting | ok | error
  serverError: null,
  demoEnabled: false,

  route: { name: 'meets' },

  meets: [], // list-view rows
  endedMeets: [], // the tombstone ring (§6.9/§9.3 ended[]), always present, newest-first
  meetsLoading: false,
  meetsError: null,
  lastCreated: null, // { id, join } shown once after a successful create

  meet: null, // current meet snapshot (detail view)
  meetId: null,
  meetSocketStatus: 'idle', // idle | connecting | live | stale | reconnecting | gone | rejected | disconnected
  meetFatal: null, // {reason} once the socket says "stop" (§9.4a close codes)
  buildState: { kind: 'built', reason: '', waiting: [] },
  events: [], // newest first
  flashNode: null, // {name, until} — transient failover highlight
  selectedNodeName: null, // Node detail panel selection (§9.5)

  versionSkew: null, // server api_version once it exceeds ours, else null
});

let socket = null; // the live EventSocket/MockEventSocket for the currently-open meet, if any

// api.js flags any REST body whose api_version exceeds ours with `__skew` (§9.4a). Surface
// that through the same global `versionSkew` field the WS path uses, so the "reload" banner
// is a single source of truth regardless of which transport noticed first.
function noteSkew(body) {
  if (body && body.__skew && typeof body.api_version === 'number') {
    store.setState((s) => (s.versionSkew ? s : { versionSkew: body.api_version }));
  }
}

// ---------------------------------------------------------------------------------------
// Server connection
// ---------------------------------------------------------------------------------------

function currentBases() {
  const { httpBase, wsBase } = realApi.parseServerURL(store.getState().serverUrlRaw);
  return { httpBase, wsBase };
}

export function setServerUrl(raw) {
  saveServerUrl(raw);
  store.setState({ serverUrlRaw: raw });
}

/** Validate the configured server and (re)load the meets list. The "Connect" action. */
export async function connectServer() {
  store.setState({ serverStatus: 'connecting', serverError: null });
  let httpBase;
  try {
    ({ httpBase } = currentBases());
  } catch (err) {
    store.setState({ serverStatus: 'error', serverError: err.message });
    return;
  }
  await refreshMeets(httpBase);
  // demoEnabled is set inside refreshMeets from the list body's own `demo_enabled` field
  // (§9.6) — the server advertises it on the one call the dashboard already makes on
  // connect, so there is no separate capability probe and no extra round trip.
  const s = store.getState();
  if (s.serverStatus !== 'error') {
    store.setState({ serverStatus: 'ok' });
  }
}

/** User-initiated disconnect: close any live meet socket, stop treating the server as reachable. */
export function disconnectServer() {
  closeMeetSocket();
  store.setState({ serverStatus: 'unknown', serverError: null, demoEnabled: false });
}

// ---------------------------------------------------------------------------------------
// Meets list
// ---------------------------------------------------------------------------------------

export async function refreshMeets(httpBaseArg) {
  let httpBase = httpBaseArg;
  if (!httpBase) {
    try {
      ({ httpBase } = currentBases());
    } catch (err) {
      store.setState({ meetsError: err.message });
      return;
    }
  }
  store.setState({ meetsLoading: true, meetsError: null });
  try {
    const body = await api.listMeets(httpBase);
    const meets = Array.isArray(body && body.meets) ? body.meets : [];
    const endedMeets = Array.isArray(body && body.ended) ? body.ended : [];
    // demo_enabled (§9.6) rides this same response; an older server that predates the
    // field simply omits it, and `!!undefined` is the safe "off" default we want.
    const demoEnabled = !!(body && body.demo_enabled);
    store.setState({ meets, endedMeets, demoEnabled, meetsLoading: false });
    noteSkew(body);
  } catch (err) {
    store.setState({
      meetsLoading: false,
      meetsError: err.message || String(err),
      serverStatus: 'error',
      serverError: err.message || String(err),
    });
  }
}

export async function createMeet(id) {
  const { httpBase } = currentBases();
  const body = await api.createMeet(httpBase, id); // throws ApiError — caller (UI) shows it
  noteSkew(body);
  store.setState({ lastCreated: body });
  await refreshMeets(httpBase);
  return body;
}

export function dismissLastCreated() {
  store.setState({ lastCreated: null });
}

// ---------------------------------------------------------------------------------------
// Meet detail — the live subnet view
// ---------------------------------------------------------------------------------------

function pushEvent(frame) {
  store.setState((s) => ({ events: [frame, ...s.events].slice(0, MAX_EVENTS) }));
}

// `coordinator` and `arbiter_is_coordinator` must always agree (see isArbiterHosting's
// doc comment in format.js for why an empty name is ambiguous on its own). Both fields
// are set through this ONE function, never separately, because "two fields that must
// always agree, updated in two places" is exactly how they drifted apart before:
// applyDelta's 'election' case used to set `coordinator` alone, so the header
// (meetDetail.js, gated on `arbiter_is_coordinator`) kept showing "the arbiter" after a
// live handover moved the role to a peer, until the next resync overwrote the whole
// snapshot and masked it as looking merely intermittent.
function applyCoordinator(next, coordinator, reason) {
  next.coordinator = coordinator;
  next.arbiter_is_coordinator = isArbiterHosting(coordinator, reason);
}

/** Apply one delta frame's known, structural effects onto the current snapshot (best-effort). */
function applyDelta(snapshot, frame) {
  if (!snapshot) return snapshot;
  const next = { ...snapshot, nodes: snapshot.nodes ? snapshot.nodes.map((n) => ({ ...n })) : [] };
  // Every frame carries the control-plane version it was observed under — keep epoch/rev live.
  if (frame.epoch !== undefined) next.epoch = frame.epoch;
  if (frame.rev !== undefined) next.rev = frame.rev;

  const data = frame.data || {};
  switch (frame.kind) {
    case 'health_changed': {
      const node = next.nodes.find((n) => n.name === data.name);
      if (node) node.health = data.health;
      break;
    }
    case 'reparent': {
      const node = next.nodes.find((n) => n.name === data.name);
      if (node) node.parent = data.to;
      if (Array.isArray(next.edges)) {
        next.edges = next.edges.filter((e) => !(e.child === data.name && e.parent === data.from));
        if (data.to) next.edges = [...next.edges, { parent: data.to, child: data.name }];
      }
      break;
    }
    case 'topology': {
      if (data.root !== undefined) next.root = data.root;
      if (Array.isArray(data.edges)) next.edges = data.edges;
      if (Array.isArray(data.backups)) next.backups = data.backups;
      break;
    }
    case 'election': {
      if (data.coordinator !== undefined) applyCoordinator(next, data.coordinator, data.reason);
      if (data.epoch !== undefined) next.epoch = data.epoch;
      break;
    }
    // stale_rejected is deliberately NOT applied here. `next.stale_rejected` is the
    // MEET-WIDE sum (coordinator.RoomSnapshot.StaleRejected) — it only ever arrives on a
    // snapshot frame. This event's `data.total` is a DIFFERENT number: one peer's
    // cumulative refusal count this session (coordinator.Event.Count), which the wire
    // doc (internal/dashboard/wire.go) says can legitimately DECREASE when that peer
    // rejoins and its fence resets. Synthesizing the meet-wide sum from a per-peer delta
    // — by incrementing or by any other arithmetic — would mix two different counters
    // into one field. The per-peer total is shown on the event-log line itself
    // (see views/eventLog.js); the meet-wide sum stays whatever the last snapshot said
    // until the next snapshot/resync refreshes it for real.
    default:
      break; // member_joined/left, settling, unbuildable, failover, demo, stale_rejected: log-only
  }
  return next;
}

function deriveBuildState(frame) {
  const data = frame.data || {};
  if (frame.kind === 'settling') {
    return { kind: 'settling', reason: data.reason || '', waiting: Array.isArray(data.waiting) ? data.waiting : [] };
  }
  if (frame.kind === 'unbuildable') {
    return { kind: 'unbuildable', reason: data.reason || '', waiting: [] };
  }
  if (frame.kind === 'topology') {
    const kind = ['built', 'relaxed', 'settling', 'unbuildable'].includes(data.outcome) ? data.outcome : 'built';
    return { kind, reason: data.reason || '', waiting: [] };
  }
  return null; // not a build-state-relevant frame
}

function closeMeetSocket() {
  if (socket) {
    socket.close();
    socket = null;
  }
}

export function openMeetDetail(meetId) {
  closeMeetSocket();
  store.setState({
    meetId,
    meet: null,
    meetSocketStatus: 'idle',
    meetFatal: null,
    buildState: { kind: 'built', reason: '', waiting: [] },
    events: [],
    flashNode: null,
    selectedNodeName: null,
    versionSkew: null,
  });

  let wsBase;
  try {
    ({ wsBase } = currentBases());
  } catch (err) {
    store.setState({ meetSocketStatus: 'rejected', meetFatal: { reason: 'bad_server_url', detail: err.message } });
    return;
  }

  socket = new SocketImpl(wsBase, meetId, {
    onStatus: (status) => store.setState({ meetSocketStatus: status }),
    onSnapshot: (frame) => {
      store.setState((s) => ({
        meet: frame.data,
        buildState: s.buildState, // unchanged until an explicit build-state event arrives
      }));
      pushEvent(frame);
    },
    onEvent: (frame) => {
      store.setState((s) => {
        const patch = { meet: applyDelta(s.meet, frame) };
        const bs = deriveBuildState(frame);
        if (bs) patch.buildState = bs;
        if (frame.kind === 'failover' && frame.data && frame.data.name) {
          patch.flashNode = { name: frame.data.name, until: Date.now() + 1600 };
        }
        return patch;
      });
      pushEvent(frame);
    },
    onFatal: (reason, detail) => {
      store.setState({ meetFatal: { reason, detail } });
    },
    onVersionSkew: (serverVersion) => {
      store.setState({ versionSkew: serverVersion });
    },
  });
  socket.connect();
}

export function closeMeetDetail() {
  closeMeetSocket();
  store.setState({ meetId: null, meet: null, events: [], meetFatal: null });
}

export function resyncMeet() {
  if (socket) socket.resync();
}

export function selectNode(name) {
  store.setState((s) => ({ selectedNodeName: s.selectedNodeName === name ? null : name }));
}

// ---------------------------------------------------------------------------------------
// Demo controls (§9.6) — only ever invoked from UI gated on state.demoEnabled
// ---------------------------------------------------------------------------------------

export async function demoEvict(meetId, name) {
  const { httpBase } = currentBases();
  return api.demoEvict(httpBase, meetId, name);
}

export async function demoElect(meetId, name) {
  const { httpBase } = currentBases();
  return api.demoElect(httpBase, meetId, name);
}

export const isMockMode = USE_MOCK;
