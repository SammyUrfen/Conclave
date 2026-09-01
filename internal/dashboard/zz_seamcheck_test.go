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
