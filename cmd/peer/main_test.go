package main

import "testing"

func TestHealthURLFor(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "full url", in: "http://localhost:9000", want: "http://localhost:9000/healthz"},
		{name: "https preserved", in: "https://conclave.example", want: "https://conclave.example/healthz"},
		{name: "trailing slash trimmed", in: "http://localhost:9000/", want: "http://localhost:9000/healthz"},
		{name: "bare host:port gets http", in: "localhost:9000", want: "http://localhost:9000/healthz"},
		{name: "whitespace trimmed", in: "  http://localhost:9000  ", want: "http://localhost:9000/healthz"},
		{name: "empty is error", in: "", wantErr: true},
		{name: "scheme only is error", in: "http://", wantErr: true},
		// Regression: these have a non-empty u.Host (":", ":9000") but no
		// hostname. They mirror the server's "-addr :9000" and previously slipped
		// through to build a hostless "http://:/healthz". Must be rejected.
		{name: "bare colon is error", in: ":", wantErr: true},
		{name: "port only is error", in: ":9000", wantErr: true},
		{name: "scheme and port no host is error", in: "http://:9000", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := healthURLFor(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("healthURLFor(%q): expected error, got %q", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("healthURLFor(%q): unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("healthURLFor(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
