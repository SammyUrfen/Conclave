// router.js — a minimal hash router. Two routes only: the meets list and one meet's detail.
//
// Hash-based (not the History API) on purpose: this is a static file served with no server
// rewrite rules (GitHub Pages, or `file://`), so any URL the server doesn't have a real file
// for would 404 on refresh/deep-link under path-based routing. `#/...` never leaves the
// client.

/**
 * @param {string} hash - e.g. "#/meets/standup" or "" / "#/"
 * @returns {{name: 'meets'} | {name: 'meet', id: string}}
 */
export function parseRoute(hash) {
  const path = (hash || '').replace(/^#/, '');
  const meetMatch = path.match(/^\/meets\/([^/]+)\/?$/);
  if (meetMatch) return { name: 'meet', id: decodeURIComponent(meetMatch[1]) };
  return { name: 'meets' };
}

export function navigateToMeets() {
  window.location.hash = '#/';
}

export function navigateToMeet(id) {
  window.location.hash = `#/meets/${encodeURIComponent(id)}`;
}

/** Calls `handler(route)` once immediately and again on every hash change. */
export function onRouteChange(handler) {
  const fire = () => handler(parseRoute(window.location.hash));
  window.addEventListener('hashchange', fire);
  fire();
}
