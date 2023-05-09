package events

import (
	"github.com/levelfourab/sprout-go"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"events",
	fx.Provide(sprout.Logger("events"), fx.Private),
	fx.Provide(NewManager),
)
