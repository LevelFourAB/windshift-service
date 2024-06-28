package v1alpha1

import (
	"time"
	"windshift/service/internal/events"

	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *EventsServiceServer) Events(server eventsv1alpha1.EventsService_EventsServer) error {
	ctx := server.Context()

	subscribe, err := server.Recv()
	if err != nil {
		return errors.Wrap(err, "could not receive initial subscription")
	}

	var events *events.Events
	if sub := subscribe.GetSubscribe(); sub != nil {
		config := e.createEventConsumeConfig(sub)

		var err2 error
		events, err2 = e.events.Events(ctx, config)
		if err2 != nil {
			return errors.Wrap(err2, "could not subscribe")
		}
	} else {
		return status.Error(codes.InvalidArgument, "first message must be a subscribe")
	}
	defer events.Close()

	// Send initial response
	err = server.Send(&eventsv1alpha1.EventsResponse{
		Response: &eventsv1alpha1.EventsResponse_Subscribed_{
			Subscribed: &eventsv1alpha1.EventsResponse_Subscribed{
				ProcessingTimeout: durationpb.New(events.Timeout),
			},
		},
	})
	if err != nil {
		return err
	}

	// Start a goroutine to read incoming messages and send them to a channel
	messages := make(chan *eventsv1alpha1.EventsRequest)
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			request, err2 := server.Recv()
			if err2 != nil {
				if status.Code(err2) == codes.Canceled {
					// The context is done, can no longer receive any messages
					return
				}

				e.logger.Warn("Could not receive message", zap.Error(err2))
			} else {
				messages <- request
			}
		}
	}()

	eventMap := newEventTracker()

	timeout := events.Timeout
	lastGC := time.Now()

	for {
		// Check if it's time to GC the event map
		if time.Since(lastGC) > timeout {
			eventMap.RemoveOlderThan(timeout)
			lastGC = time.Now()
		}

		select {
		case <-time.After(timeout):
			// If the timeout is reached continue the loop to do a periodic GC
			continue
		case <-ctx.Done():
			return nil
		case <-e.globalStop:
			return nil
		case event := <-events.Incoming():
			eventMap.Add(event)

			// Create the common headers
			headers := &eventsv1alpha1.Headers{
				Timestamp:      timestamppb.New(event.Headers.PublishedAt),
				IdempotencyKey: event.Headers.IdempotencyKey,
			}

			// Inject the span from the event context
			e.w3cPropagator.Inject(event.Context, eventTracingHeaders{
				headers: headers,
			})

			// Send the actual event
			err = server.Send(&eventsv1alpha1.EventsResponse{
				Response: &eventsv1alpha1.EventsResponse_Event{
					Event: &eventsv1alpha1.Event{
						Id:      event.StreamSeq,
						Data:    event.Data,
						Subject: event.Subject,
						Headers: headers,
					},
				},
			})
			if err != nil {
				return errors.Wrap(err, "could not send event")
			}

			event.DiscardData()
		case request := <-messages:
			switch r := request.Request.(type) {
			case *eventsv1alpha1.EventsRequest_Subscribe_:
				return errors.New("cannot subscribe again")
			case *eventsv1alpha1.EventsRequest_Ack_:
				err = e.handleAck(server, eventMap, r)
				if err != nil {
					return err
				}
			case *eventsv1alpha1.EventsRequest_Reject_:
				err = e.handleReject(server, eventMap, r)
				if err != nil {
					return err
				}
			case *eventsv1alpha1.EventsRequest_Ping_:
				err = e.handlePing(server, eventMap, r)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (e *EventsServiceServer) handleAck(
	server eventsv1alpha1.EventsService_EventsServer,
	eventMap eventTracker,
	r *eventsv1alpha1.EventsRequest_Ack_,
) error {
	ids := r.Ack.Ids

	processedIDs := make([]uint64, 0, len(ids))
	invalidIDs := make([]uint64, 0, len(ids))
	temporaryErrors := make([]uint64, 0, len(ids))
	for _, id := range ids {
		event := eventMap.Get(id)
		if event != nil {
			err := event.Ack()
			if err != nil {
				if errors.Is(err, nats.ErrInvalidJSAck) || errors.Is(err, nats.ErrMsgAlreadyAckd) {
					invalidIDs = append(invalidIDs, id)
					eventMap.Remove(id)
				} else {
					e.logger.Warn("Could not reject event", zap.Error(err))
					temporaryErrors = append(temporaryErrors, id)
				}
			} else {
				processedIDs = append(processedIDs, id)
				eventMap.Remove(id)
			}
		} else {
			invalidIDs = append(invalidIDs, id)
		}
	}

	err := server.Send(&eventsv1alpha1.EventsResponse{
		Response: &eventsv1alpha1.EventsResponse_AckConfirmation_{
			AckConfirmation: &eventsv1alpha1.EventsResponse_AckConfirmation{
				Ids:                processedIDs,
				InvalidIds:         invalidIDs,
				TemporaryFailedIds: temporaryErrors,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "could not send acknowledge confirmation")
	}

	return nil
}

func (e *EventsServiceServer) handleReject(
	server eventsv1alpha1.EventsService_EventsServer,
	eventMap eventTracker,
	r *eventsv1alpha1.EventsRequest_Reject_,
) error {
	ids := r.Reject.Ids
	permanently := r.Reject.Permanently
	delay := r.Reject.Delay

	processedIDs := make([]uint64, 0, len(ids))
	invalidIDs := make([]uint64, 0, len(ids))
	temporaryErrors := make([]uint64, 0, len(ids))
	for _, id := range ids {
		event := eventMap.Get(id)
		if event != nil {
			var err error
			if permanently != nil && *permanently {
				err = event.RejectPermanently()
			} else if delay != nil {
				err = event.RejectWithDelay(delay.AsDuration())
			} else {
				err = event.Reject()
			}

			if err != nil {
				e.logger.Warn("Could not reject event", zap.Error(err))
				if errors.Is(err, nats.ErrInvalidJSAck) || errors.Is(err, nats.ErrMsgAlreadyAckd) {
					invalidIDs = append(invalidIDs, id)
					eventMap.Remove(id)
				} else {
					temporaryErrors = append(temporaryErrors, id)
				}
			} else {
				processedIDs = append(processedIDs, id)
				eventMap.Remove(id)
			}
		} else {
			invalidIDs = append(invalidIDs, id)
		}
	}

	err := server.Send(&eventsv1alpha1.EventsResponse{
		Response: &eventsv1alpha1.EventsResponse_RejectConfirmation_{
			RejectConfirmation: &eventsv1alpha1.EventsResponse_RejectConfirmation{
				Ids:                processedIDs,
				InvalidIds:         invalidIDs,
				TemporaryFailedIds: temporaryErrors,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "could not send reject confirmation")
	}

	return nil
}

func (e *EventsServiceServer) handlePing(
	server eventsv1alpha1.EventsService_EventsServer,
	eventMap eventTracker,
	r *eventsv1alpha1.EventsRequest_Ping_,
) error {
	ids := r.Ping.Ids

	processedIDs := make([]uint64, 0, len(ids))
	invalidIDs := make([]uint64, 0, len(ids))
	temporaryErrors := make([]uint64, 0, len(ids))
	for _, id := range ids {
		event := eventMap.Get(id)
		if event != nil {
			err := event.Ping()
			if err != nil {
				e.logger.Warn("Could not ping event", zap.Error(err))
				if errors.Is(err, nats.ErrInvalidMsg) {
					invalidIDs = append(invalidIDs, id)
				} else {
					temporaryErrors = append(temporaryErrors, id)
				}
			} else {
				processedIDs = append(processedIDs, id)
				eventMap.MarkPinged(id)
			}
		} else {
			invalidIDs = append(invalidIDs, id)
		}
	}

	err := server.Send(&eventsv1alpha1.EventsResponse{
		Response: &eventsv1alpha1.EventsResponse_PingConfirmation_{
			PingConfirmation: &eventsv1alpha1.EventsResponse_PingConfirmation{
				Ids:                processedIDs,
				InvalidIds:         invalidIDs,
				TemporaryFailedIds: temporaryErrors,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "could not send ping confirmation")
	}

	return nil
}

func (*EventsServiceServer) createEventConsumeConfig(sub *eventsv1alpha1.EventsRequest_Subscribe) *events.EventConsumeConfig {
	maxPendingEvents := uint(0)
	if sub.MaxProcessingEvents != nil {
		maxPendingEvents = uint(*sub.MaxProcessingEvents)
	}

	return &events.EventConsumeConfig{
		Stream:           sub.Stream,
		Name:             sub.Consumer,
		MaxPendingEvents: maxPendingEvents,
	}
}
