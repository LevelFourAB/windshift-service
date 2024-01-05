package v1alpha1

import (
	statev1alpha1 "windshift/service/internal/proto/windshift/state/v1alpha1"

	"github.com/levelfourab/sprout-go"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var Module = fx.Module(
	"grpc.v1alpha1",
	fx.Provide(sprout.Logger("grpc.state.v1alpha1"), fx.Private),
	fx.Provide(newStateServiceServer),
	fx.Invoke(register),
)

func register(server *grpc.Server, events *StateServiceServer) {
	statev1alpha1.RegisterStateServiceServer(server, events)
}
