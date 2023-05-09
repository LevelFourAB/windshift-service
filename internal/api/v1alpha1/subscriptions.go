package v1alpha1

import (
	"context"
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
)

func (e *EventsServiceServer) EnsureSubscription(ctx context.Context, req *eventsv1alpha1.EnsureSubscriptionRequest) (*eventsv1alpha1.EnsureSubscriptionResponse, error) {
	config := &events.SubscriptionConfig{
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
		switch pointer := req.Pointer.Pointer.(type) {
		case *eventsv1alpha1.EnsureSubscriptionRequest_StreamPointer_Time:
			config.DeliverFromTime = pointer.Time.AsTime()
		case *eventsv1alpha1.EnsureSubscriptionRequest_StreamPointer_Id:
			config.DeliverFromID = pointer.Id
		case *eventsv1alpha1.EnsureSubscriptionRequest_StreamPointer_Start:
			config.DeliverFromFirst = true
		case *eventsv1alpha1.EnsureSubscriptionRequest_StreamPointer_End:
			config.DeliverFromFirst = false
		}
	}

	sub, err := e.events.EnsureSubscription(ctx, config)
	if err != nil {
		return nil, err
	}

	return &eventsv1alpha1.EnsureSubscriptionResponse{
		Id: sub.ID,
	}, nil
}
