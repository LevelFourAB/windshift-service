package events

import (
	"github.com/levelfourab/sprout-go"
	"go.uber.org/fx"
)

// Module for FX that enables events.
var Module = fx.Module(
	"events",
	fx.Provide(sprout.Logger("events"), fx.Private),
	fx.Provide(sprout.ServiceTracer(), fx.Private),
	fx.Provide(NewManager),
)
