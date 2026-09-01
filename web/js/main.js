// main.js — the entrypoint. Wires the store, the router, and the view modules together.
// No framework: on every state change we re-render whichever panel the current route needs.

import { store, isMockMode, setServerUrl, connectServer, disconnectServer,
  createMeet, dismissLastCreated, openMeetDetail, closeMeetDetail, resyncMeet,
  selectNode, demoEvict, demoElect } from './state.js';
import { onRouteChange, navigateToMeets, navigateToMeet } from './router.js';
import { el } from './dom.js';
import * as serverBar from './views/serverBar.js';
import * as meetsList from './views/meetsList.js';
import * as meetDetail from './views/meetDetail.js';
import { CLIENT_API_VERSION } from './api.js';

const serverBarEl = document.getElementById('server-bar');
const viewRootEl = document.getElementById('view-root');
const versionBannerEl = document.getElementById('version-banner');

if (isMockMode) {
  document.title = '(mock) ' + document.title;
  const notice = el('div', { class: 'mock-banner' },
    'Fixture mode (?mock=1) — rendering web/fixtures/*.json, not a live server. Not shipped behaviour.');
  document.body.insertBefore(notice, document.body.firstChild);
}

const serverBarActions = {
  onServerUrlChange: (raw) => setServerUrl(raw),
  onConnect: () => connectServer(),
  onDisconnect: () => disconnectServer(),
  onNavigateMeets: () => navigateToMeets(),
};

const meetsListActions = {
  onOpenMeet: (id) => navigateToMeet(id),
  onCreateMeet: async (id) => {
    try {
      // Deliberately does NOT navigate into the meet: the whole point of this panel is the
      // join.peer_command copy button (§9.3's "rendezvous"), and navigating away before the
      // user can copy it would defeat that. They open the subnet view themselves when ready.
      await createMeet(id);
    } catch (err) {
      store.setState({ meetsError: err.message || String(err) });
    }
  },
  onDismissCreated: () => dismissLastCreated(),
};

function meetDetailActions() {
  const s = store.getState();
  return {
    onBack: () => { closeMeetDetail(); navigateToMeets(); },
    onResync: () => resyncMeet(),
    onSelectNode: (name) => selectNode(name),
    onEvict: (name) => {
      if (!name) return Promise.reject(new Error('choose a peer first'));
      return demoEvict(s.meetId, name);
    },
    onElect: (name) => demoElect(s.meetId, name || undefined),
  };
}

function renderVersionBanner(state) {
  if (state.versionSkew) {
    versionBannerEl.hidden = false;
    versionBannerEl.textContent =
      `This dashboard (api_version ${CLIENT_API_VERSION}) is older than the server ` +
      `(api_version ${state.versionSkew}). New data has stopped applying. Reload to update.`;
  } else {
    versionBannerEl.hidden = true;
    versionBannerEl.textContent = '';
  }
}

let currentRoute = { name: 'meets' };

function renderAll() {
  const state = store.getState();
  serverBar.render(serverBarEl, state, serverBarActions);
  renderVersionBanner(state);

  if (currentRoute.name === 'meet') {
    meetDetail.render(viewRootEl, state, meetDetailActions());
  } else {
    meetsList.render(viewRootEl, state, meetsListActions);
  }
}

onRouteChange((route) => {
  const prev = currentRoute;
  currentRoute = route;

  if (route.name === 'meet') {
    if (prev.name !== 'meet' || prev.id !== route.id) {
      openMeetDetail(route.id);
    }
  } else if (prev.name === 'meet') {
    closeMeetDetail();
  }
  renderAll();
});

store.subscribe(renderAll);

// Deliberately NOT auto-connecting on load. The arbiter is down more often than it's up
// (task brief), and attempting a fetch to an address nothing is listening on logs a
// browser-level network/CORS diagnostic to the console regardless of whether our JS catches
// the rejection — that's the browser's own devtools instrumentation, not something script
// can suppress. So the very first paint is the honest "not connected" state with zero
// network activity, and the user's own Connect click is what makes the first request.
