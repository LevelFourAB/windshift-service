package api

import (
	"github.com/levelfourab/sprout-go"
	"go.uber.org/fx"
)

type Config struct {
	Port int `env:"PORT" envDefault:"8080"`
}

var Module = fx.Module(
	"api",
	fx.Provide(sprout.Config("GRPC", &Config{})),
	fx.Provide(sprout.Logger("grpc")),
	fx.Provide(newServer),
)
