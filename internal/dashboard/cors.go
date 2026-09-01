package dashboard

import "net/http"

// The CORS response values, frozen by §9.8.
const (
	// corsAllowMethods is the complete verb set this API uses. OPTIONS is listed
	// because the preflight itself is answered here.
	corsAllowMethods = "GET, POST, OPTIONS"
	// corsAllowHeaders is Content-Type and nothing else: the client sends no auth
	// header, and every additional name here is a header a page may then send.
	corsAllowHeaders = "Content-Type"
	// corsMaxAge is 600 seconds — the ceiling Chrome actually honours for preflight
	// caching. Asking for more is silently clamped, so this is "the largest value
	// that means what it says": it removes the preflight from the steady-state click
	// path without pretending to a cache lifetime no browser grants.
	corsMaxAge = "600"
)

// withCORS attaches the §9.8 headers and answers preflights.
//
// Three behaviours are load-bearing and each one is a decision:
//
//   - NO Origin header ⇒ no CORS headers at all. CORS is a browser mechanism; a curl
//     or a health checker is asking nothing and gets nothing.
//   - A non-matching Origin ⇒ the NORMAL response, minus Access-Control-Allow-Origin,
//     so the browser is the thing that blocks it. Deliberately not a 403: a
//     distinguishable error would let a probing page enumerate the allow-list.
//   - The matching origin is ECHOED, never "*". "*" is technically sufficient while
//     nothing here uses credentials, and it becomes a real vulnerability the moment
//     anyone adds one. It costs a line to do correctly now.
//
// Vary: Origin is set whenever an Origin was present, matching or not, because the
// response genuinely does vary by it and a cache that does not know that will serve
// one origin's answer to another.
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Vary", "Origin")
			if echo, ok := s.origins.Match(origin); ok {
				h := w.Header()
				h.Set("Access-Control-Allow-Origin", echo)
				h.Set("Access-Control-Allow-Methods", corsAllowMethods)
				h.Set("Access-Control-Allow-Headers", corsAllowHeaders)
				h.Set("Access-Control-Max-Age", corsMaxAge)
				// Access-Control-Allow-Credentials is NEVER sent (§9.4a). There is no
				// auth, so credentials would only widen the attack surface.
			}
		}
		if r.Method == http.MethodOptions {
			// Preflight: 204, the headers above, no body. Every path under /api/
			// accepts the same verb set, so there is nothing to vary per route.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
