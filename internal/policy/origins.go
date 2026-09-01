package policy

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

// Origins is a parsed allow-list of browser origins permitted to reach this
// process: the WebSocket upgrade (via Patterns, fed to
// websocket.AcceptOptions.OriginPatterns) and the dashboard's REST CORS surface
// (via Match). Patterns and Match MUST accept exactly the same set — see Match.
type Origins []string

// ParseOrigins parses the comma-separated -allowed-origins value, failing loud on
// an entry that is not a scheme://host origin or that misuses the wildcard.
//
// Failing loud here, at startup, rather than admitting a bad pattern and having
// it silently never match (or worse, match more than intended) at request time is
// the same discipline logging.ParseLevel and validateCoordinatorFlags already use
// for other flag-driven config: an operator typo in a rarely-exercised value
// should break the process immediately, with a message naming the bad entry, not
// surface later as "the dashboard mysteriously can't reach the arbiter".
func ParseOrigins(csv string) (Origins, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		// Empty means "no cross-origin browser surface configured" — same-origin
		// only. That is a legitimate, if narrow, deployment (a Go peer sends no
		// Origin header at all, so it is unaffected either way).
		return nil, nil
	}
	fields := strings.Split(csv, ",")
	out := make(Origins, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			return nil, fmt.Errorf("policy: -allowed-origins %q has an empty entry", csv)
		}
		if err := validatePattern(f); err != nil {
			return nil, fmt.Errorf("policy: -allowed-origins entry %q: %w", f, err)
		}
		out = append(out, f)
	}
	return out, nil
}

// validatePattern enforces the one wildcard rule: '*' may appear at most once,
// and only as a trailing PORT wildcard on an otherwise-exact scheme://host —
// "http://localhost:*", never "http://*.example.com" or a bare "*". Anything else
// is rejected, because those looser shapes are exactly what makes an allow-list
// silently permissive (docs/PLAN.md §2.4, §9.8).
func validatePattern(p string) error {
	scheme, host, ok := strings.Cut(p, "://")
	if !ok || scheme == "" || host == "" {
		return fmt.Errorf("not a scheme://host origin")
	}
	if strings.Contains(scheme, "*") {
		return fmt.Errorf("'*' is not allowed in the scheme")
	}
	switch strings.Count(host, "*") {
	case 0:
		return nil
	case 1:
		exactHost, ok := strings.CutSuffix(host, ":*")
		if !ok || exactHost == "" {
			return fmt.Errorf(`'*' is only allowed as a trailing port wildcard ("host:*")`)
		}
		return nil
	default:
		return fmt.Errorf("at most one '*' is allowed")
	}
}

// Patterns returns the list in the form coder/websocket's
// AcceptOptions.OriginPatterns expects. Every entry already IS a valid
// path.Match pattern against "scheme://host", because ParseOrigins accepted only
// that shape — this is a defensive copy, not a transformation.
func (o Origins) Patterns() []string {
	out := make([]string, len(o))
	copy(out, o)
	return out
}

// Match reports whether origin is allowed and, if so, returns the value to echo
// in Access-Control-Allow-Origin: the REQUEST's origin, verbatim, never "*". "*"
// is technically harmless while nothing here uses credentials, but it is a
// footgun that becomes a real vulnerability the instant anyone adds one, and
// echoing the matched origin costs nothing now (docs/PLAN.md §9.8).
//
// Match and Patterns MUST accept exactly the same set — that is the entire
// reason this package exists (docs/PLAN.md §2.4) — so Match reimplements the
// identical primitive coder/websocket's Accept uses internally (path.Match,
// case-folded, against "scheme://host"), rather than inventing a matching shape
// of its own that could silently diverge. internal/signaling carries the test
// that asserts the two agree against the real library (§12.2): this package
// stays a leaf and does not import coder/websocket itself.
func (o Origins) Match(origin string) (string, bool) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	target := strings.ToLower(u.Scheme + "://" + u.Host)
	for _, pattern := range o {
		if matched, _ := path.Match(strings.ToLower(pattern), target); matched {
			return origin, true
		}
	}
	return "", false
}
