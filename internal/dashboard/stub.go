// Package dashboard is the browser-facing HTTP + WebSocket surface (docs/PLAN.md §9).
//
// THIS FILE IS THE RED-PHASE SKELETON: declarations only, so the test suite in this
// package compiles and fails on real assertions rather than on "undefined". It is
// replaced wholesale by the implementation commit.
package dashboard

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/policy"
)

// APIVersion is the wire contract version carried by every body and frame.
const APIVersion = 1

// eventBuffer is how many frames may queue for one event-stream connection.
const eventBuffer = 32

// ErrMemberNotFound is the sentinel a DemoControl returns when the target is absent.
var ErrMemberNotFound = errors.New("dashboard: member not found")

// MeetSource is what the dashboard needs from the arbiter.
type MeetSource interface {
	ListMeets(ctx context.Context) ([]arbiter.Meet, error)
	ListEndedMeets(ctx context.Context) ([]arbiter.EndedMeet, error)
	CreateMeet(ctx context.Context, id string) (arbiter.Meet, error)
	GetMeet(ctx context.Context, id string) (arbiter.Meet, error)
}

// SubnetSource is what the dashboard needs from the coordinator.
type SubnetSource interface {
	Snapshot(ctx context.Context, roomID string) (coordinator.RoomSnapshot, error)
}

// DemoControl is the gated destructive surface (§9.6).
type DemoControl interface {
	Evict(ctx context.Context, roomID, name string) error
	ForceElection(ctx context.Context, roomID, name string) error
}

// Config parameterises the dashboard.
type Config struct {
	Meets     MeetSource
	Subnet    SubnetSource
	Demo      DemoControl
	Origins   policy.Origins
	PublicURL string
	Addr      string
	Clock     clock.Clock
}

// Server is the dashboard.
type Server struct{}

// New constructs a Server. RED PHASE: it succeeds and yields a NULL OBJECT, so every
// test in this package fails on the assertion it actually makes rather than all of
// them failing identically at construction — a red that proves nothing.
func New(log *slog.Logger, cfg Config) (*Server, error) { return &Server{}, nil }

// Handler returns the http.Handler to mount at the root.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"api_version":1}`))
	})
}

// Publish implements coordinator.Publisher.
func (s *Server) Publish(coordinator.Event) {}

// PublishElection implements part of arbiter.Publisher.
func (s *Server) PublishElection(arbiter.Announcement) {}

// PublishRepair implements the rest of arbiter.Publisher.
func (s *Server) PublishRepair(roomID, name string, peerEpoch, meetEpoch uint64, resolved bool) {}

// Close shuts every live event stream down.
func (s *Server) Close() {}
