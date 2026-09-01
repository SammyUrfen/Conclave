// dom.js — tiny DOM-construction helpers.
//
// WHY this exists at all: §9's robustness contract requires every render path to be
// XSS-safe because meet ids and peer names are user-supplied (PLAN.md §9 "Robustness").
// `el()` never accepts an HTML string for content — text always goes through
// `Node.textContent` / `Text` nodes, never `innerHTML`. That makes "paste a name into a
// template string" structurally impossible rather than merely disciplined-against.

/**
 * Create a DOM element.
 *
 * @param {string} tag - element tag name, e.g. "div".
 * @param {object} [props] - attributes/properties. `class`/`className`, `dataset` (object),
 *   `on<Event>` handlers (function), and any other key is set via `setAttribute` unless it
 *   starts with `.` (then it is assigned as a JS property, e.g. `.value` on an <input>).
 * @param {...(Node|string|number|null|undefined)} children - text becomes a Text node
 *   (never parsed as markup); null/undefined are skipped so callers can inline conditionals.
 * @returns {HTMLElement}
 */
export function el(tag, props, ...children) {
  const node = document.createElement(tag);
  if (props) {
    for (const [key, value] of Object.entries(props)) {
      if (value == null) continue;
      if (key === 'class' || key === 'className') {
        node.className = value;
      } else if (key === 'dataset') {
        for (const [dk, dv] of Object.entries(value)) node.dataset[dk] = dv;
      } else if (key.startsWith('on') && typeof value === 'function') {
        node.addEventListener(key.slice(2).toLowerCase(), value);
      } else if (key.startsWith('.')) {
        node[key.slice(1)] = value;
      } else if (key === 'html') {
        // Deliberately unsupported. Any caller reaching for raw HTML on user-derived data
        // is exactly the bug this module exists to prevent — fail loud, not silently.
        throw new Error('el(): "html" prop is not supported; build nodes/text instead');
      } else {
        node.setAttribute(key, String(value));
      }
    }
  }
  appendChildren(node, children);
  return node;
}

function appendChildren(node, children) {
  for (const child of children) {
    if (child == null) continue;
    if (Array.isArray(child)) {
      appendChildren(node, child);
    } else if (child instanceof Node) {
      node.appendChild(child);
    } else {
      node.appendChild(document.createTextNode(String(child)));
    }
  }
}

/** Replace all children of `node` with `children` (safe, textContent-based). */
export function setChildren(node, children) {
  node.replaceChildren();
  appendChildren(node, Array.isArray(children) ? children : [children]);
}

/** A <span> of plain text with an optional class — the common case of `el`. */
export function text(str, className) {
  return el('span', className ? { class: className } : null, str);
}
