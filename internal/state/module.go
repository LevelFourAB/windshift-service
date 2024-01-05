package state

import (
	"github.com/levelfourab/sprout-go"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"state",
	fx.Provide(sprout.Logger("state"), fx.Private),
	fx.Provide(sprout.ServiceTracer(), fx.Private),
	fx.Provide(NewManager),
)
