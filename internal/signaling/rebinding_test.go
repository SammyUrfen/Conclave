package signaling

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/policy"
)

// TestUpgradeRefusesRebindingOrigin is the regression test for the DNS-rebinding
// bypass. It is written as a RAW handshake rather than through websocket.Dial
// because the whole attack turns on the Host header, which Dial derives from the
// URL and will not let a caller forge.
//
// The attack: the victim loads evil.example, whose DNS has been rebound to the
// arbiter's address (127.0.0.1, or a LAN IP). The victim's browser then sends
// Host: evil.example and Origin: http://evil.example to OUR server. Host and Origin
// agree, so any "is this same-origin?" shortcut waves it through — and the attacker
// now has a WebSocket into a process they could never dial directly. That reachability
// is the entire reason "it only listens on localhost" is considered safe, so a gate
// that can be walked past by agreeing with itself is not a gate.
//
// The rule this pins: the ONLY thing that may admit a browser origin is the
// operator's allow-list. Host is attacker-controlled and carries no authority.
func TestUpgradeRefusesRebindingOrigin(t *testing.T) {
	origins, err := policy.ParseOrigins("https://sammyurfen.github.io")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}
	srv, wsURL, _ := newWireServer(t, HubConfig{AllowedOrigins: origins}, nil)
	defer srv.Close()

	realHost := srv.Listener.Addr().String()
	httpURL := "http" + strings.TrimPrefix(strings.TrimSuffix(wsURL, "?room=demo"), "ws")

	tests := []struct {
		name       string
		host       string // the forged Host header
		origin     string
		wantStatus int
	}{
		{
			// The bypass. Host and Origin agree, and neither is on the allow-list.
			name:       "rebound host agreeing with its origin",
			host:       "evil.example",
			origin:     "http://evil.example",
			wantStatus: http.StatusForbidden,
		},
		{
			// The same hole reached without any DNS trickery: a page served from
			// the arbiter's own address is still not on the allow-list, and the
			// allow-list is the only authority.
			name:       "the server's own address is not self-authorising",
			host:       realHost,
			origin:     "http://" + realHost,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "an unlisted origin under the real host",
			host:       realHost,
			origin:     "https://evil.example",
			wantStatus: http.StatusForbidden,
		},
		{
			// A listed origin is admitted no matter what Host says, which is the
			// other half of "Host carries no authority".
			name:       "a listed origin under a forged host",
			host:       "evil.example",
			origin:     "https://sammyurfen.github.io",
			wantStatus: http.StatusSwitchingProtocols,
		},
		{
			// Go peers send no Origin at all. Breaking this breaks every peer.
			name:       "no origin header is still a peer",
			host:       realHost,
			origin:     "",
			wantStatus: http.StatusSwitchingProtocols,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpURL+"?room=demo", nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			// req.Host forges the Host HEADER while the client still dials the real
			// listener — exactly what a rebound DNS name produces.
			req.Host = tt.host
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", newWebSocketKey(t))
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("handshake: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("Host=%q Origin=%q: status %d, want %d",
					tt.host, tt.origin, resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func newWebSocketKey(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b[:])
}
