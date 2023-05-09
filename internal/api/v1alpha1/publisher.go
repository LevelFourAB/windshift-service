package v1alpha1

import (
	"context"
	"time"

	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
)

func (e *EventsServiceServer) PublishEvent(ctx context.Context, req *eventsv1alpha1.PublishEventRequest) (*eventsv1alpha1.PublishEventResponse, error) {
	timestamp := time.Now()
	if req.Timestamp != nil {
		timestamp = req.Timestamp.AsTime()
	}

	config := &events.PublishConfig{
		Subject: req.Subject,
		Data:    req.Data,
	}

	if req.Timestamp != nil {
		config.PublishedTime = &timestamp
	}

	if req.ExpectedLastId != nil {
		config.ExpectedSubjectSeq = req.ExpectedLastId
	}

	if req.IdempotencyKey != nil {
		config.IdempotencyKey = *req.IdempotencyKey
	}

	ack, err := e.events.Publish(ctx, config)
	if err != nil {
		return nil, err
	}
	return &eventsv1alpha1.PublishEventResponse{
		Id: ack.ID,
	}, nil
}
