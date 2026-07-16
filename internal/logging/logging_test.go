package logging

import (
	"log/slog"
	"testing"
)

// TestParseLevel is the project's first test, written table-driven on purpose:
// each row is a (name, input, expected) case, and t.Run turns each row into its
// own named subtest so a failure tells you *which* case broke. Adding a new
// case is one line, not one copy-pasted function — this is the idiom you'll
// reach for constantly once BuildTree and the coordinator logic show up.
func TestParseLevel(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    slog.Level
		wantErr bool
	}{
		{name: "debug", in: "debug", want: slog.LevelDebug},
		{name: "info", in: "info", want: slog.LevelInfo},
		{name: "warn", in: "warn", want: slog.LevelWarn},
		{name: "warning alias", in: "warning", want: slog.LevelWarn},
		{name: "error", in: "error", want: slog.LevelError},
		{name: "mixed case", in: "InFo", want: slog.LevelInfo},
		{name: "surrounding whitespace", in: "  debug  ", want: slog.LevelDebug},
		{name: "empty is an error", in: "", wantErr: true},
		{name: "garbage is an error", in: "loud", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLevel(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseLevel(%q): expected an error, got nil (level=%v)", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLevel(%q): unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
