package v1alpha1

import (
	"windshift/service/internal/events"

	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *EventsServiceServer) Events(server eventsv1alpha1.EventsService_EventsServer) error {
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
	err = server.Send(&eventsv1alpha1.EventsResponse{
		Response: &eventsv1alpha1.EventsResponse_Subscribed_{
			Subscribed: &eventsv1alpha1.EventsResponse_Subscribed{},
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
				// TODO: Logging?
				return
			}

			messages <- request
		}
	}()

	eventMap := make(map[uint64]*events.Event)

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-queue.Events():
			eventMap[event.StreamSeq] = event
			// TODO: Keep track of the expiry of events

			err = server.Send(&eventsv1alpha1.EventsResponse{
				Response: &eventsv1alpha1.EventsResponse_Event{
					Event: &eventsv1alpha1.Event{
						Id:          event.StreamSeq,
						Data:        event.Data,
						PublishTime: timestamppb.New(event.PublishedAt),
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
			case *eventsv1alpha1.EventsRequest_Accept_:
				ids := r.Accept.Ids
				for _, id := range ids {
					event, ok := eventMap[id]
					if ok {
						err = event.Accept()
						if err != nil {
							return errors.Wrap(err, "could not accept event")
						}

						delete(eventMap, id)
					}
				}

				err = server.Send(&eventsv1alpha1.EventsResponse{
					Response: &eventsv1alpha1.EventsResponse_AcceptConfirmation_{
						AcceptConfirmation: &eventsv1alpha1.EventsResponse_AcceptConfirmation{
							Ids: ids,
						},
					},
				})
				if err != nil {
					return errors.Wrap(err, "could not send accept confirmation")
				}
			case *eventsv1alpha1.EventsRequest_Reject_:
				ids := r.Reject.Ids
				retry := r.Reject.Retry
				for _, id := range ids {
					event, ok := eventMap[id]
					if ok {
						err = event.Reject(retry)
						if err != nil {
							return errors.Wrap(err, "could not reject event")
						}

						delete(eventMap, id)
					}
				}

				err = server.Send(&eventsv1alpha1.EventsResponse{
					Response: &eventsv1alpha1.EventsResponse_RejectConfirmation_{
						RejectConfirmation: &eventsv1alpha1.EventsResponse_RejectConfirmation{
							Ids: ids,
						},
					},
				})
				if err != nil {
					return errors.Wrap(err, "could not send reject confirmation")
				}
			case *eventsv1alpha1.EventsRequest_Ping_:
				ids := r.Ping.Ids
				for _, id := range ids {
					event, ok := eventMap[id]
					if ok {
						err = event.Ping()
						if err != nil {
							return errors.Wrap(err, "could not ping event")
						}
					}
				}

				err = server.Send(&eventsv1alpha1.EventsResponse{
					Response: &eventsv1alpha1.EventsResponse_PingConfirmation_{
						PingConfirmation: &eventsv1alpha1.EventsResponse_PingConfirmation{
							Ids: ids,
						},
					},
				})
				if err != nil {
					return errors.Wrap(err, "could not send ping confirmation")
				}
			}
		}
	}
}

func (*EventsServiceServer) createQueueConfig(sub *eventsv1alpha1.EventsRequest_Subscribe) *events.QueueConfig {
	config := &events.QueueConfig{
		Stream: sub.Stream,
		Name:   sub.SubscriberId,
	}

	if sub.Concurrency != nil {
		config.Concurrency = int(*sub.Concurrency)
	}

	return config
}
