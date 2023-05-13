package v1alpha1

import (
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"github.com/levelfourab/sprout-go"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var Module = fx.Module(
	"grpc.v1alpha1",
	fx.Provide(sprout.Logger("grpc.v1alpha1"), fx.Private),
	fx.Provide(newEventsServiceServer),
	fx.Invoke(register),
)

type EventsServiceServer struct {
	eventsv1alpha1.UnimplementedEventsServiceServer

	logger        *zap.Logger
	w3cPropagator propagation.TextMapPropagator

	events *events.Manager
}

func newEventsServiceServer(
	logger *zap.Logger,
	events *events.Manager,
) *EventsServiceServer {
	return &EventsServiceServer{
		logger:        logger,
		w3cPropagator: propagation.TraceContext{},

		events: events,
	}
}

func register(server *grpc.Server, events *EventsServiceServer) {
	eventsv1alpha1.RegisterEventsServiceServer(server, events)
}
