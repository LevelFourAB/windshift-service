package v1alpha1

import (
	"context"
	"errors"
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (e *EventsServiceServer) EnsureStream(ctx context.Context, req *eventsv1alpha1.EnsureStreamRequest) (*eventsv1alpha1.EnsureStreamResponse, error) {
	config := &events.StreamConfig{
		Name: req.Name,
	}

	if req.RetentionPolicy != nil {
		policy := req.RetentionPolicy
		if policy.MaxAge != nil {
			config.MaxAge = policy.MaxAge.AsDuration()
		}

		if policy.MaxBytes != nil {
			config.MaxBytes = uint(*policy.MaxBytes)
		}

		if policy.MaxEvents != nil {
			config.MaxMsgs = uint(*policy.MaxEvents)
		}

		if policy.MaxEventsPerSubject != nil {
			config.MaxMsgsPerSubject = uint(*policy.MaxEventsPerSubject)
		}

		if policy.DiscardPolicy != nil {
			switch *policy.DiscardPolicy {
			case eventsv1alpha1.EnsureStreamRequest_DISCARD_POLICY_NEW:
				config.DiscardPolicy = events.DiscardPolicyNew
			case eventsv1alpha1.EnsureStreamRequest_DISCARD_POLICY_OLD,
				eventsv1alpha1.EnsureStreamRequest_DISCARD_POLICY_UNSPECIFIED:
				config.DiscardPolicy = events.DiscardPolicyOld
			}
		}

		if policy.DiscardNewPerSubject != nil {
			config.DiscardNewPerSubject = *policy.DiscardNewPerSubject
		}
	}

	if req.Source == nil {
		return nil, errors.New("a source must be specified for the stream")
	}

	switch source := req.Source.(type) {
	case *eventsv1alpha1.EnsureStreamRequest_Subjects_:
		config.Subjects = source.Subjects.Subjects
	case *eventsv1alpha1.EnsureStreamRequest_Mirror:
		config.Mirror = toStreamSource(source.Mirror)
	case *eventsv1alpha1.EnsureStreamRequest_Aggregate:
		config.Sources = make([]*events.StreamSource, len(source.Aggregate.Sources))
		for i, s := range source.Aggregate.Sources {
			config.Sources[i] = toStreamSource(s)
		}
	}

	if req.Storage != nil {
		if req.Storage.Replicas != nil {
			replicas := uint(*req.Storage.Replicas)
			config.Replicas = &replicas
		}

		if req.Storage.Type != nil {
			switch *req.Storage.Type {
			case eventsv1alpha1.EnsureStreamRequest_STORAGE_TYPE_MEMORY:
				config.StorageType = events.StorageTypeMemory
			case eventsv1alpha1.EnsureStreamRequest_STORAGE_TYPE_FILE,
				eventsv1alpha1.EnsureStreamRequest_STORAGE_TYPE_UNSPECIFIED:
				config.StorageType = events.StorageTypeFile
			}
		}
	}

	if req.DeduplicationWindow != nil {
		deduplicationWindow := req.DeduplicationWindow.AsDuration()
		config.DeduplicationWindow = &deduplicationWindow
	}

	if req.MaxEventSize != nil {
		maxEventSize := uint(*req.MaxEventSize)
		config.MaxEventSize = &maxEventSize
	}

	_, err := e.events.EnsureStream(ctx, config)
	if errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Canceled, "context canceled")
	} else if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.DeadlineExceeded, "timed out")
	} else if events.IsValidationError(err) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, err
	}

	return &eventsv1alpha1.EnsureStreamResponse{}, nil
}

func toStreamSource(s *eventsv1alpha1.EnsureStreamRequest_StreamSource) *events.StreamSource {
	return &events.StreamSource{
		Name:           s.Name,
		From:           toStreamPointer(s.From),
		FilterSubjects: s.FilterSubjects,
	}
}

func toStreamPointer(p *eventsv1alpha1.StreamPointer) *events.StreamPointer {
	if p.Pointer != nil {
		switch pointer := p.Pointer.(type) {
		case *eventsv1alpha1.StreamPointer_Time:
			return &events.StreamPointer{
				Time: pointer.Time.AsTime(),
			}
		case *eventsv1alpha1.StreamPointer_Offset:
			return &events.StreamPointer{
				ID: pointer.Offset,
			}
		case *eventsv1alpha1.StreamPointer_Start:
			return &events.StreamPointer{
				First: true,
			}
		case *eventsv1alpha1.StreamPointer_End:
			return &events.StreamPointer{
				First: false,
			}
		}
	}

	return &events.StreamPointer{
		First: false,
	}
}
