package events

import (
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Manager is used to manage everything related to events, streams, consumers,
// event publishing, and event consuming.
type Manager struct {
	// logger is the logger used by the event manager.
	logger *zap.Logger
	// tracer is the tracer used to trace events.
	tracer trace.Tracer
	// w3cPropagator is the W3C propagator used to propagate tracing information
	// into published and consumed events.
	w3cPropagator propagation.TextMapPropagator

	// js is the NATS JetStream instance used to talk to NATS.
	js jetstream.JetStream
}

// NewManager creates a new event manager.
func NewManager(
	logger *zap.Logger,
	tracer trace.Tracer,
	js jetstream.JetStream,
) (*Manager, error) {
	m := &Manager{
		logger:        logger,
		tracer:        tracer,
		w3cPropagator: propagation.TraceContext{},
		js:            js,
	}

	return m, nil
}
