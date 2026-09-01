package dashboard

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// deriveBase turns -public-url and -addr into the two base URLs the join block is
// built from (§9.4a).
//
// One value in, two out, computed once: join.ws_url and join.peer_command are built
// from the same derivation, so they are correct together or obviously wrong together,
// never subtly disagreeing.
//
// When publicURL is empty the base comes from addr: a host-ful addr ("example.com:9000")
// is used as-is, and the host-less default (":9000") becomes localhost — which is what
// makes the out-of-the-box story work, because W3C Secure Contexts classifies localhost
// as potentially trustworthy and an https:// GitHub Pages page may therefore open
// ws://localhost:9000 without a mixed-content block (§9.9).
//
// The scheme is plain http/ws unless publicURL says otherwise. The server cannot know
// whether something terminates TLS in front of it, so a TLS deployment MUST set
// -public-url; guessing https here would produce a join command that fails for everyone
// on a plain deployment.
func deriveBase(publicURL, addr string) (httpBase, wsBase string, err error) {
	if p := strings.TrimSpace(publicURL); p != "" {
		u, perr := url.Parse(p)
		if perr != nil {
			return "", "", fmt.Errorf("-public-url %q is not a URL: %w", p, perr)
		}
		scheme := strings.ToLower(u.Scheme)
		switch scheme {
		case "http", "ws":
			scheme = "http"
		case "https", "wss":
			scheme = "https"
		default:
			return "", "", fmt.Errorf("-public-url %q must be http(s):// or ws(s)://", p)
		}
		if u.Host == "" {
			return "", "", fmt.Errorf("-public-url %q has no host", p)
		}
		base := scheme + "://" + u.Host + strings.TrimSuffix(u.Path, "/")
		return base, wsSchemeOf(base), nil
	}

	host := strings.TrimSpace(addr)
	if host == "" {
		return "", "", fmt.Errorf("either -public-url or -addr must be set")
	}
	if h, port, serr := net.SplitHostPort(host); serr == nil {
		if h == "" {
			// ":9000", the default. localhost rather than 127.0.0.1 because the
			// literal name is what the spec names as trustworthy, and the UI offers
			// 127.0.0.1 as the hedge for a machine whose DNS disagrees (§9.9).
			h = "localhost"
		}
		host = net.JoinHostPort(h, port)
	}
	base := "http://" + host
	return base, wsSchemeOf(base), nil
}

// wsSchemeOf maps an http(s) base to its ws(s) counterpart. Derived rather than
// separately configured, so the two can never disagree about whether TLS is in play.
func wsSchemeOf(httpBase string) string {
	if strings.HasPrefix(httpBase, "https://") {
		return "wss://" + strings.TrimPrefix(httpBase, "https://")
	}
	return "ws://" + strings.TrimPrefix(httpBase, "http://")
}

// joinNamePlaceholder is the -name value handed to a human to replace.
//
// It MUST satisfy policy.ValidPeerName, which is lowercase-only: the hub validates
// -name at its boundary, so the previous "YOUR_NAME" produced a command that was
// rejected the instant it was pasted unchanged — a copy button handing out a failing
// command. It also has to still READ as a placeholder, because one that looked like a
// real name would be pasted without noticing.
const joinNamePlaceholder = "your-name"

// joinFor builds the rendezvous for one meet.
//
// The meet id is query-escaped even though policy.MeetIDPattern already excludes every
// character that would need it. The escaping is not redundant defence-in-depth theatre:
// it is what keeps the guarantee local, so a future widening of the pattern cannot turn
// this line into an injection point silently.
//
// PeerCommand, by contrast, interpolates into a SHELL COMMAND with no quoting, and its
// safety rests entirely on policy.MeetIDPattern and policy.ValidPeerName excluding every
// shell metacharacter. Do not loosen either pattern without escaping here first.
func (s *Server) joinFor(meetID string) joinInfo {
	return joinInfo{
		WSURL: s.wsBase + "/ws?room=" + url.QueryEscape(meetID),
		PeerCommand: "peer -call -managed -server " + s.httpBase +
			" -room " + meetID + " -name " + joinNamePlaceholder,
	}
}
