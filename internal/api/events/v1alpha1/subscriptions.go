package v1alpha1

import (
	"context"
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
)

func (e *EventsServiceServer) EnsureConsumer(ctx context.Context, req *eventsv1alpha1.EnsureConsumerRequest) (*eventsv1alpha1.EnsureConsumerResponse, error) {
	config := &events.ConsumerConfig{
		Stream:   req.Stream,
		Subjects: req.Subjects,
	}

	if req.Name != nil {
		config.Name = *req.Name
	}

	if req.Timeout != nil {
		config.Timeout = req.Timeout.AsDuration()
	}

	if req.Pointer != nil {
		config.Pointer = toStreamPointer(req.Pointer)
	}

	consumer, err := e.events.EnsureConsumer(ctx, config)
	if err != nil {
		return nil, err
	}

	return &eventsv1alpha1.EnsureConsumerResponse{
		Id: consumer.ID,
	}, nil
}
