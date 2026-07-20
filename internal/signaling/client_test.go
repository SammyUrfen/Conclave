package signaling

import "testing"

func TestWSURLFor(t *testing.T) {
	tests := []struct {
		name     string
		server   string
		room     string
		peerName string
		want     string
		wantErr  bool
	}{
		{name: "http becomes ws", server: "http://localhost:9000", room: "demo", want: "ws://localhost:9000/ws?room=demo"},
		{name: "https becomes wss", server: "https://conclave.example", room: "r", want: "wss://conclave.example/ws?room=r"},
		{name: "ws preserved", server: "ws://localhost:9000", room: "x", want: "ws://localhost:9000/ws?room=x"},
		{name: "bare host:port gets ws", server: "localhost:9000", room: "demo", want: "ws://localhost:9000/ws?room=demo"},
		{name: "empty room omits query", server: "http://localhost:9000", room: "", want: "ws://localhost:9000/ws"},
		{name: "name adds query", server: "http://localhost:9000", room: "demo", peerName: "relay", want: "ws://localhost:9000/ws?name=relay&room=demo"},
		{name: "name only, no room", server: "localhost:9000", room: "", peerName: "leaf-b", want: "ws://localhost:9000/ws?name=leaf-b"},
		{name: "path is replaced", server: "http://localhost:9000/ignored", room: "demo", want: "ws://localhost:9000/ws?room=demo"},
		{name: "empty is error", server: "", wantErr: true},
		{name: "hostless is error", server: ":9000", wantErr: true},
		{name: "bad scheme is error", server: "ftp://localhost:9000", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := wsURLFor(tt.server, tt.room, tt.peerName)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("wsURLFor(%q, %q): expected error, got %q", tt.server, tt.room, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("wsURLFor(%q, %q): unexpected error: %v", tt.server, tt.room, err)
			}
			if got != tt.want {
				t.Errorf("wsURLFor(%q, %q) = %q, want %q", tt.server, tt.room, got, tt.want)
			}
		})
	}
}
