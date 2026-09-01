// This file contains NO runtime test, deliberately. It is a compile-time seam check:
// each declaration below fails to BUILD if a producer stops satisfying a consumer-defined
// interface, which is the failure cmd/server would otherwise hit at wiring time. It lives
// in a _test.go file so it links into no binary and adds no production import edge.
package dashboard_test

import (
	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/dashboard"
)

var (
	_ dashboard.MeetSource   = (*arbiter.Arbiter)(nil)
	_ dashboard.SubnetSource = (*coordinator.Coordinator)(nil)
	_ coordinator.Publisher  = (*dashboard.Server)(nil)
	_ arbiter.Publisher      = (*dashboard.Server)(nil)
)
