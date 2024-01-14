package v1alpha1

import (
	"context"
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (e *EventsServiceServer) EnsureConsumer(ctx context.Context, req *eventsv1alpha1.EnsureConsumerRequest) (*eventsv1alpha1.EnsureConsumerResponse, error) {
	config := &events.ConsumerConfig{
		Stream:   req.Stream,
		Subjects: req.Subjects,
	}

	if req.Name != nil {
		config.Name = *req.Name
	}

	if req.ProcessingTimeout != nil {
		config.Timeout = req.ProcessingTimeout.AsDuration()
	}

	if req.From != nil {
		config.From = toStreamPointer(req.From)
	}

	consumer, err := e.events.EnsureConsumer(ctx, config)
	if errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Canceled, "context canceled")
	} else if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.DeadlineExceeded, "timed out")
	} else if events.IsValidationError(err) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, err
	}

	return &eventsv1alpha1.EnsureConsumerResponse{
		Id: consumer.ID,
	}, nil
}
