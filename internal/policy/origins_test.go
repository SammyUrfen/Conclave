package policy

import "testing"

// TestParseOrigins covers the wildcard rule (§9.8/§2.4): '*' is permitted only as
// a single trailing PORT wildcard on an otherwise-exact scheme://host. Every other
// shape — a bare '*', a wildcard host, a wildcard mid-string, a missing scheme —
// must fail loud, because a silently-accepted bad pattern is the failure mode this
// package exists to close off.
func TestParseOrigins(t *testing.T) {
	t.Run("a valid comma-separated list parses in order, trimmed", func(t *testing.T) {
		got, err := ParseOrigins(" https://sammyurfen.github.io ,http://localhost:*,http://127.0.0.1:*")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := Origins{"https://sammyurfen.github.io", "http://localhost:*", "http://127.0.0.1:*"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("entry %d = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("empty csv is an empty list, not an error", func(t *testing.T) {
		got, err := ParseOrigins("")
		if err != nil {
			t.Fatalf("ParseOrigins(\"\") returned an error: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ParseOrigins(\"\") = %v, want empty", got)
		}
	})

	rejects := []struct {
		name string
		csv  string
	}{
		{name: "mid-string wildcard", csv: "http://ev*l.example.com"},
		{name: "wildcard host", csv: "http://*.example.com"},
		{name: "bare star, no scheme", csv: "http://*"},
		{name: "star alone", csv: "*"},
		{name: "wildcard scheme", csv: "*://a.example.com"},
		{name: "two wildcards in one entry", csv: "http://a:*,http://b*:*"},
		{name: "wildcard port with no host", csv: "http://:*"},
		{name: "missing scheme", csv: "localhost:5173"},
		{name: "empty entry between valid ones", csv: "http://a.example.com,,http://b.example.com"},
		{name: "trailing comma", csv: "http://a.example.com,"},
	}
	for _, tt := range rejects {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := ParseOrigins(tt.csv); err == nil {
				t.Errorf("ParseOrigins(%q) = %v, nil; want a rejection", tt.csv, got)
			}
		})
	}
}

// TestOriginsPatterns proves Patterns() hands back exactly the strings ParseOrigins
// accepted, in order — the shape websocket.AcceptOptions.OriginPatterns expects.
func TestOriginsPatterns(t *testing.T) {
	o, err := ParseOrigins("https://a.example.com,http://localhost:*")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}
	got := o.Patterns()
	want := []string{"https://a.example.com", "http://localhost:*"}
	if len(got) != len(want) {
		t.Fatalf("Patterns() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Patterns()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestOriginsMatch covers Match in isolation: exact origins, the port wildcard,
// scheme mismatch, an unlisted origin, and the "never echo '*'" rule (§9.8). The
// property that Match must accept EXACTLY what Patterns() makes websocket.Accept
// accept is asserted separately, against the real library, in
// internal/signaling (see docs/PLAN.md §12.2 — that test may import both
// packages; this one must not import coder/websocket to stay a leaf).
func TestOriginsMatch(t *testing.T) {
	o, err := ParseOrigins("https://sammyurfen.github.io,http://localhost:*")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}
	tests := []struct {
		name   string
		origin string
		want   bool
	}{
		{name: "exact match", origin: "https://sammyurfen.github.io", want: true},
		{name: "port wildcard", origin: "http://localhost:5173", want: true},
		{name: "case-insensitive", origin: "HTTPS://SAMMYURFEN.GITHUB.IO", want: true},
		{name: "unlisted host", origin: "https://evil.example", want: false},
		{name: "right host, wrong scheme", origin: "http://sammyurfen.github.io", want: false},
		{name: "empty origin", origin: "", want: false},
		{name: "not a URL with a host", origin: "not a url", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			echoed, ok := o.Match(tt.origin)
			if ok != tt.want {
				t.Fatalf("Match(%q) ok = %v, want %v", tt.origin, ok, tt.want)
			}
			if ok && echoed != tt.origin {
				t.Errorf("Match(%q) echoed %q, want the exact request origin (never '*')", tt.origin, echoed)
			}
			if !ok && echoed != "" {
				t.Errorf("Match(%q) echoed %q on rejection, want empty", tt.origin, echoed)
			}
		})
	}
}
