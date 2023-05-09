package v1alpha1

import (
	"context"
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
)

func (e *EventsServiceServer) EnsureStream(ctx context.Context, req *eventsv1alpha1.EnsureStreamRequest) (*eventsv1alpha1.EnsureStreamResponse, error) {
	config := &events.StreamConfig{
		Name:     req.Name,
		Subjects: req.Subjects,
	}

	if req.RetentionPolicy != nil {
		policy := req.RetentionPolicy
		if policy.MaxAge != nil {
			config.MaxAge = policy.MaxAge.AsDuration()
		}

		if policy.MaxBytes != nil {
			config.MaxBytes = uint(*policy.MaxBytes)
		}

		if policy.MaxMessages != nil {
			config.MaxMsgs = uint(*policy.MaxMessages)
		}
	}

	_, err := e.events.EnsureStream(ctx, config)
	if err != nil {
		return nil, err
	}

	return &eventsv1alpha1.EnsureStreamResponse{}, nil
}
