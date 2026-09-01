package policy

import "testing"

// TestAllowUpgrade pins the one decision every WebSocket entry point in this
// process must make, so that /ws and the dashboard's event stream cannot drift
// apart. Two rules, and the second is the security-relevant one: an ABSENT Origin
// is allowed because non-browser clients (every Go peer) send none, and a PRESENT
// Origin is allowed only if the operator listed it — never because it happens to
// agree with the request's Host, which the caller controls.
func TestAllowUpgrade(t *testing.T) {
	origins, err := ParseOrigins("https://sammyurfen.github.io,http://localhost:*")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}

	tests := []struct {
		label  string
		origin string
		want   bool
	}{
		{label: "absent origin is a non-browser client", origin: "", want: true},
		{label: "listed origin", origin: "https://sammyurfen.github.io", want: true},
		{label: "port wildcard", origin: "http://localhost:9000", want: true},
		{label: "unlisted origin", origin: "https://evil.example"},
		{label: "listed host under the wrong scheme", origin: "http://sammyurfen.github.io"},
		{label: "suffix confusion", origin: "https://sammyurfen.github.io.evil.example"},
		{label: "a rebound name is still just an unlisted origin", origin: "http://evil.example"},
		{label: "loopback is not self-authorising", origin: "http://127.0.0.1:44321"},
		{label: "unparseable", origin: "://"},
		{label: "no scheme", origin: "sammyurfen.github.io"},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			if got := origins.AllowUpgrade(tt.origin); got != tt.want {
				t.Errorf("AllowUpgrade(%q) = %v, want %v", tt.origin, got, tt.want)
			}
		})
	}

	t.Run("an empty allow-list still admits non-browser clients", func(t *testing.T) {
		var none Origins
		if !none.AllowUpgrade("") {
			t.Error("a peer sending no Origin must be admitted even with no list configured")
		}
		if none.AllowUpgrade("http://localhost:9000") {
			t.Error("with no list configured, no browser origin may be admitted")
		}
	})
}

// TestParseOriginsRejectsMatchMetacharacters closes a quieter hole in the same
// function. '*' was policed, but the other glob metacharacters were not — so
// "http://localhost:900?" passed validation and then silently matched a range of
// ports the operator never wrote, and a '[' could produce a malformed pattern whose
// error the matcher swallowed as deny while the library propagated it as deny-ALL.
// The one function whose job is to fail loud must not have a silent mode.
func TestParseOriginsRejectsMatchMetacharacters(t *testing.T) {
	bad := []string{
		"http://localhost:900?",
		"http://l?calhost:9000",
		"http://localhost:[89]000",
		`http://local\host:9000`,
		"http://*.example.com",
		"*",
		"http://*",
		"https://example.com:*:*",
		"example.com",
		"://example.com",
		"http://",
	}
	for _, p := range bad {
		t.Run(p, func(t *testing.T) {
			if _, err := ParseOrigins(p); err == nil {
				t.Errorf("ParseOrigins(%q) was accepted; it must fail loud", p)
			}
		})
	}

	good := []string{
		"http://localhost:*",
		"https://sammyurfen.github.io",
		"http://127.0.0.1:*",
		"http://localhost:*,https://sammyurfen.github.io",
	}
	for _, p := range good {
		t.Run(p, func(t *testing.T) {
			if _, err := ParseOrigins(p); err != nil {
				t.Errorf("ParseOrigins(%q) was rejected: %v", p, err)
			}
		})
	}
}

// TestMatchIsTotalOverAcceptedPatterns proves the matcher has no error path left to
// swallow: every pattern ParseOrigins accepts yields a definite yes or no for every
// origin, so "deny because the pattern was malformed" — indistinguishable from a
// real deny, and the divergence the review flagged — cannot occur.
func TestMatchIsTotalOverAcceptedPatterns(t *testing.T) {
	origins, err := ParseOrigins("http://localhost:*,https://sammyurfen.github.io")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}
	for _, origin := range []string{
		"http://localhost:1", "http://localhost:65535", "http://localhost",
		"https://sammyurfen.github.io", "https://other.example", "http://localhost:*",
	} {
		echo, ok := origins.Match(origin)
		if ok && echo != origin {
			t.Errorf("Match(%q) echoed %q; it must echo the request's origin verbatim", origin, echo)
		}
		if !ok && echo != "" {
			t.Errorf("Match(%q) denied but returned %q", origin, echo)
		}
	}
}
