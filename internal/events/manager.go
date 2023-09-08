package events

import (
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type Manager struct {
	logger        *zap.Logger
	tracer        trace.Tracer
	w3cPropagator propagation.TextMapPropagator

	js jetstream.JetStream
}

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
