package v1alpha1

import (
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var Module = fx.Module(
	"grpc.v1alpha1",
	fx.Provide(newEventsServiceServer),
	fx.Invoke(register),
)

type EventsServiceServer struct {
	eventsv1alpha1.UnimplementedEventsServiceServer

	events *events.Manager
}

func newEventsServiceServer(events *events.Manager) *EventsServiceServer {
	return &EventsServiceServer{
		events: events,
	}
}

func register(server *grpc.Server, events *EventsServiceServer) {
	eventsv1alpha1.RegisterEventsServiceServer(server, events)
}
