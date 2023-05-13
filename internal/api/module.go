package api

import (
	"github.com/levelfourab/sprout-go"
	"github.com/planetscale/vtprotobuf/codec/grpc"
	"go.uber.org/fx"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto" // For vtprotobuf
)

func init() {
	encoding.RegisterCodec(grpc.Codec{})
}

type Config struct {
	Port int `env:"PORT" envDefault:"8080"`
}

var Module = fx.Module(
	"api",
	fx.Provide(sprout.Config("GRPC", &Config{})),
	fx.Provide(sprout.Logger("grpc")),
	fx.Provide(newServer),
)
