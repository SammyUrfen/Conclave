package policy

import (
	"fmt"
	"net/url"
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

// validatePattern enforces the one wildcard rule: '*' may appear at most once, and
// only as a trailing PORT wildcard on an otherwise-exact scheme://host —
// "http://localhost:*", never "http://*.example.com" or a bare "*". Anything else is
// rejected, because those looser shapes are exactly what makes an allow-list
// silently permissive (docs/PLAN.md §2.4, §9.8).
//
// It also rejects the OTHER glob metacharacters ('?', '[', ']', '\\'). They were
// unpoliced while '*' was, which meant "http://localhost:900?" passed validation and
// then quietly matched a whole range of ports the operator never wrote, and a '['
// could form a malformed pattern whose error one caller swallowed as deny and
// another propagated as deny-everything. Rejecting them here is what lets Match be
// TOTAL: no accepted pattern has an error case, so a deny is always a real deny.
func validatePattern(p string) error {
	scheme, host, ok := strings.Cut(p, "://")
	if !ok || scheme == "" || host == "" {
		return fmt.Errorf("not a scheme://host origin")
	}
	if strings.Contains(scheme, "*") {
		return fmt.Errorf("'*' is not allowed in the scheme")
	}
	if i := strings.IndexAny(p, "?[]\\"); i >= 0 {
		return fmt.Errorf("%q is a pattern metacharacter and is not allowed; "+
			"the only wildcard permitted is a trailing \":*\" port wildcard", p[i:i+1])
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
// AcceptOptions.OriginPatterns expects — a defensive copy, not a transformation.
//
// DO NOT USE IT TO GATE AN UPGRADE. websocket.Accept consults OriginPatterns only
// AFTER returning allow on strings.EqualFold(r.Host, u.Host), so handing it this
// list admits a strictly larger set than Match does: any origin whose host equals the
// request's Host, scheme ignored. Host is a client-supplied header, so that shortcut
// is authorisation granted by the attacker to themselves, and it is precisely what a
// DNS-rebinding attack manufactures. Every entry point in this process must call
// AllowUpgrade and pass InsecureSkipVerify, so that this package is the only matcher.
// Kept because it remains the correct shape for a caller that genuinely wants the
// library's semantics — of which there should be none.
func (o Origins) Patterns() []string {
	out := make([]string, len(o))
	copy(out, o)
	return out
}

// Match reports whether origin is allowed and, if so, returns the value to echo in
// Access-Control-Allow-Origin: the REQUEST's origin, verbatim, never "*". "*" is
// technically harmless while nothing here uses credentials, but it is a footgun that
// becomes a real vulnerability the instant anyone adds one, and echoing the matched
// origin costs nothing now (docs/PLAN.md §9.8).
//
// Match is the ONE matcher: the REST CORS surface and every WebSocket upgrade (via
// AllowUpgrade) must accept exactly the set it accepts, which is the entire reason
// this package exists (docs/PLAN.md §2.4).
//
// It compares directly rather than through path.Match. That is a deliberate change
// from the original, which mirrored the library's primitive in the hope of matching
// its behaviour — a hope the same-Host shortcut made false anyway, since no choice of
// pattern primitive can reproduce a rule that fires before patterns are consulted.
// Once the library is off the decision path, mirroring buys nothing and costs the
// metacharacter surface and an error case; ParseOrigins has already reduced every
// accepted pattern to exactly two shapes, so matching them by hand is both total and
// obvious.
//
// A trailing ":*" is a PORT wildcard only: it matches any port, never a different
// host, and never a missing port ("http://localhost:*" does not match
// "http://localhost", which is a different origin to a browser).
func (o Origins) Match(origin string) (string, bool) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	target := strings.ToLower(u.Scheme + "://" + u.Host)
	for _, pattern := range o {
		if matchOne(strings.ToLower(pattern), target) {
			return origin, true
		}
	}
	return "", false
}

// matchOne compares one already-validated, already-lowercased pattern against one
// already-lowercased "scheme://host" target.
func matchOne(pattern, target string) bool {
	prefix, isWildcard := strings.CutSuffix(pattern, ":*")
	if !isWildcard {
		return pattern == target
	}
	// The wildcard covers the port and nothing else: the host must match exactly and
	// a port must actually be present. Rejecting an empty port keeps ":*" from
	// admitting "scheme://host:" as a sneaky spelling of the portless origin.
	port, ok := strings.CutPrefix(target, prefix+":")
	return ok && port != "" && !strings.ContainsAny(port, ":/")
}

// AllowUpgrade reports whether a WebSocket upgrade carrying this Origin header value
// may proceed. It is the single decision every upgrade endpoint in this process must
// make — /ws and the dashboard's event stream — so that a security control does not
// exist in two places and diverge.
//
// Two rules:
//
//   - An ABSENT Origin is allowed. Only browsers send the header; every Go peer sends
//     none, and refusing them would break the entire data plane. This is not a hole:
//     a request with no Origin is not a cross-origin browser request, which is the
//     only thing this gate exists to stop.
//   - A PRESENT Origin is allowed only if the operator listed it. Notably NOT if it
//     merely agrees with the request's Host — that is the DNS-rebinding bypass, and
//     the reason callers must pass InsecureSkipVerify to websocket.Accept and gate
//     here instead of handing the library Patterns().
func (o Origins) AllowUpgrade(origin string) bool {
	if origin == "" {
		return true
	}
	_, ok := o.Match(origin)
	return ok
}
