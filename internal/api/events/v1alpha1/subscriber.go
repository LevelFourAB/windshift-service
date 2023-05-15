package v1alpha1

import (
	"time"
	"windshift/service/internal/events"

	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *EventsServiceServer) Consume(server eventsv1alpha1.EventsService_ConsumeServer) error {
	ctx := server.Context()

	subscribe, err := server.Recv()
	if err != nil {
		return errors.Wrap(err, "could not receive initial subscription")
	}

	var queue *events.Queue
	if sub := subscribe.GetSubscribe(); sub != nil {
		config := e.createQueueConfig(sub)

		var err2 error
		queue, err2 = e.events.Subscribe(ctx, config)
		if err2 != nil {
			return errors.Wrap(err2, "could not subscribe")
		}
	} else {
		return errors.New("first message must be a subscribe")
	}
	defer queue.Close()

	// Send initial response
	err = server.Send(&eventsv1alpha1.ConsumeResponse{
		Response: &eventsv1alpha1.ConsumeResponse_Subscribed_{
			Subscribed: &eventsv1alpha1.ConsumeResponse_Subscribed{
				ProcessingTimeout: durationpb.New(queue.Timeout),
			},
		},
	})
	if err != nil {
		return err
	}

	// Start a goroutine to read incoming messages and send them to a channel
	messages := make(chan *eventsv1alpha1.ConsumeRequest)
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			request, err2 := server.Recv()
			if err2 != nil {
				e.logger.Warn("Could not receive message", zap.Error(err2))
				return
			}

			messages <- request
		}
	}()

	eventMap := newEventTracker()

	timeout := queue.Timeout
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
		case event := <-queue.Events():
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
			err = server.Send(&eventsv1alpha1.ConsumeResponse{
				Response: &eventsv1alpha1.ConsumeResponse_Event{
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
			case *eventsv1alpha1.ConsumeRequest_Subscribe_:
				return errors.New("cannot subscribe again")
			case *eventsv1alpha1.ConsumeRequest_Ack_:
				err = e.handleAck(server, eventMap, r)
				if err != nil {
					return err
				}
			case *eventsv1alpha1.ConsumeRequest_Reject_:
				err = e.handleReject(server, eventMap, r)
				if err != nil {
					return err
				}
			case *eventsv1alpha1.ConsumeRequest_Ping_:
				err = e.handlePing(server, eventMap, r)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (e *EventsServiceServer) handleAck(
	server eventsv1alpha1.EventsService_ConsumeServer,
	eventMap eventTracker,
	r *eventsv1alpha1.ConsumeRequest_Ack_,
) error {
	ids := r.Ack.Ids

	processedIds := make([]uint64, 0, len(ids))
	invalidIds := make([]uint64, 0, len(ids))
	temporaryErrors := make([]uint64, 0, len(ids))
	for _, id := range ids {
		event := eventMap.Get(id)
		if event != nil {
			err := event.Ack()
			if err != nil {
				if errors.Is(err, nats.ErrInvalidJSAck) || errors.Is(err, nats.ErrMsgAlreadyAckd) {
					invalidIds = append(invalidIds, id)
					eventMap.Remove(id)
				} else {
					e.logger.Warn("Could not reject event", zap.Error(err))
					temporaryErrors = append(temporaryErrors, id)
				}
			} else {
				processedIds = append(processedIds, id)
				eventMap.Remove(id)
			}
		} else {
			invalidIds = append(invalidIds, id)
		}
	}

	err := server.Send(&eventsv1alpha1.ConsumeResponse{
		Response: &eventsv1alpha1.ConsumeResponse_AckConfirmation_{
			AckConfirmation: &eventsv1alpha1.ConsumeResponse_AckConfirmation{
				Ids:                processedIds,
				InvalidIds:         invalidIds,
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
	server eventsv1alpha1.EventsService_ConsumeServer,
	eventMap eventTracker,
	r *eventsv1alpha1.ConsumeRequest_Reject_,
) error {
	ids := r.Reject.Ids
	permanently := r.Reject.Permanently
	delay := r.Reject.Delay

	processedIds := make([]uint64, 0, len(ids))
	invalidIds := make([]uint64, 0, len(ids))
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
					invalidIds = append(invalidIds, id)
					eventMap.Remove(id)
				} else {
					temporaryErrors = append(temporaryErrors, id)
				}
			} else {
				processedIds = append(processedIds, id)
				eventMap.Remove(id)
			}
		} else {
			invalidIds = append(invalidIds, id)
		}
	}

	err := server.Send(&eventsv1alpha1.ConsumeResponse{
		Response: &eventsv1alpha1.ConsumeResponse_RejectConfirmation_{
			RejectConfirmation: &eventsv1alpha1.ConsumeResponse_RejectConfirmation{
				Ids:                processedIds,
				InvalidIds:         invalidIds,
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
	server eventsv1alpha1.EventsService_ConsumeServer,
	eventMap eventTracker,
	r *eventsv1alpha1.ConsumeRequest_Ping_,
) error {
	ids := r.Ping.Ids

	processedIds := make([]uint64, 0, len(ids))
	invalidIds := make([]uint64, 0, len(ids))
	temporaryErrors := make([]uint64, 0, len(ids))
	for _, id := range ids {
		event := eventMap.Get(id)
		if event != nil {
			err := event.Ping()
			if err != nil {
				e.logger.Warn("Could not ping event", zap.Error(err))
				if errors.Is(err, nats.ErrInvalidMsg) {
					invalidIds = append(invalidIds, id)
				} else {
					temporaryErrors = append(temporaryErrors, id)
				}
			} else {
				processedIds = append(processedIds, id)
				eventMap.MarkPinged(id)
			}
		} else {
			invalidIds = append(invalidIds, id)
		}
	}

	err := server.Send(&eventsv1alpha1.ConsumeResponse{
		Response: &eventsv1alpha1.ConsumeResponse_PingConfirmation_{
			PingConfirmation: &eventsv1alpha1.ConsumeResponse_PingConfirmation{
				Ids:                processedIds,
				InvalidIds:         invalidIds,
				TemporaryFailedIds: temporaryErrors,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "could not send ping confirmation")
	}

	return nil
}

func (*EventsServiceServer) createQueueConfig(sub *eventsv1alpha1.ConsumeRequest_Subscribe) *events.QueueConfig {
	return &events.QueueConfig{
		Stream: sub.Stream,
		Name:   sub.Consumer,
	}
}
