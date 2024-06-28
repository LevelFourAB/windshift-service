package v1alpha1

import (
	"context"

	"github.com/levelfourab/windshift-server/internal/events"
	eventsv1alpha1 "github.com/levelfourab/windshift-server/internal/proto/windshift/events/v1alpha1"

	"github.com/levelfourab/sprout-go"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var Module = fx.Module(
	"grpc.v1alpha1",
	fx.Provide(sprout.Logger("grpc.events.v1alpha1"), fx.Private),
	fx.Provide(newEventsServiceServer),
	fx.Invoke(register),
)

type EventsServiceServer struct {
	eventsv1alpha1.UnimplementedEventsServiceServer

	logger        *zap.Logger
	w3cPropagator propagation.TextMapPropagator

	events     *events.Manager
	globalStop chan struct{}
}

func newEventsServiceServer(
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
	events *events.Manager,
) *EventsServiceServer {
	server := &EventsServiceServer{
		logger:        logger,
		w3cPropagator: propagation.TraceContext{},

		events:     events,
		globalStop: make(chan struct{}),
	}

	lifecycle.Append(fx.Hook{
		OnStop: func(context.Context) error {
			close(server.globalStop)
			return nil
		},
	})
	return server
}

func register(server *grpc.Server, events *EventsServiceServer) {
	eventsv1alpha1.RegisterEventsServiceServer(server, events)
}
