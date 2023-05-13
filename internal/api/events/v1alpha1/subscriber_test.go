package v1alpha1_test

import (
	"context"
	"time"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Events", func() {
	var service eventsv1alpha1.EventsServiceClient

	BeforeEach(func(ctx context.Context) {
		service, _ = GetClient()

		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "events",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"events.>"},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Ephemeral Subscriptions", func() {
		It("can subscribe", func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())

			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}
		})

		It("can subscribe and receive events", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Send an event
			_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the event
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}
		})
	})

	Describe("Accepting and rejecting events", func() {
		It("can accept received event", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Send an event
			_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the event
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}

			eventID := in.GetEvent().GetId()

			// Accept the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Accept_{
					Accept: &eventsv1alpha1.ConsumeRequest_Accept{
						Ids: []uint64{eventID},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Check that we get the accepted response
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_AcceptConfirmation_); ok {
				Expect(r.AcceptConfirmation.Ids).To(Equal([]uint64{eventID}))
			} else {

				Fail("expected AcceptConfirmation message")
			}
		})

		It("accepting unknown event fails", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Accept a non-existent event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Accept_{
					Accept: &eventsv1alpha1.ConsumeRequest_Accept{
						Ids: []uint64{1},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Check that we get the accepted response
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_AcceptConfirmation_); ok {
				Expect(r.AcceptConfirmation.InvalidIds).To(Equal([]uint64{1}))
			} else {

				Fail("expected AcceptConfirmation message")
			}
		})

		It("accepting received event twice fails", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Send an event
			_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the event
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}

			eventID := in.GetEvent().GetId()

			// Accept the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Accept_{
					Accept: &eventsv1alpha1.ConsumeRequest_Accept{
						Ids: []uint64{eventID},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Accept_{
					Accept: &eventsv1alpha1.ConsumeRequest_Accept{
						Ids: []uint64{eventID},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Check that we get the accepted response
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_AcceptConfirmation_); ok {
				Expect(r.AcceptConfirmation.InvalidIds).To(Equal([]uint64{eventID}))
			} else {
				Fail("expected AcceptConfirmation message")
			}
		})

		It("rejecting events redelivers them", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Send an event
			_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the event
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}

			eventID := in.GetEvent().GetId()

			// Reject the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Reject_{
					Reject: &eventsv1alpha1.ConsumeRequest_Reject{
						Ids: []uint64{eventID},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_RejectConfirmation_); ok {
				Expect(r.RejectConfirmation.Ids).To(Equal([]uint64{eventID}))
			} else {
				Fail("expected RejectConfirmation message")
			}

			// Receive the event again
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}
		})

		It("rejecting unknown event fails", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Reject the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Reject_{
					Reject: &eventsv1alpha1.ConsumeRequest_Reject{
						Ids: []uint64{1},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_RejectConfirmation_); ok {
				Expect(r.RejectConfirmation.InvalidIds).To(Equal([]uint64{1}))
			} else {
				Fail("expected RejectConfirmation message")
			}
		})

		It("rejecting event twice errors", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Send an event
			_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the event
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}

			eventID := in.GetEvent().GetId()

			// Reject the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Reject_{
					Reject: &eventsv1alpha1.ConsumeRequest_Reject{
						Ids:   []uint64{eventID},
						Delay: durationpb.New(1 * time.Second),
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Reject_{
					Reject: &eventsv1alpha1.ConsumeRequest_Reject{
						Ids:   []uint64{eventID},
						Delay: durationpb.New(1 * time.Second),
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			Expect(in.Response).To(BeAssignableToTypeOf(&eventsv1alpha1.ConsumeResponse_RejectConfirmation_{}))
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_RejectConfirmation_); ok {
				Expect(r.RejectConfirmation.InvalidIds).To(Equal([]uint64{eventID}))
			} else {
				Fail("expected RejectConfirmation message")
			}
		})

		It("pinging event works", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Send an event
			_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the event
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Event); !ok {
				Fail("expected Event message")
			}

			eventID := in.GetEvent().GetId()

			// Ping the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Ping_{
					Ping: &eventsv1alpha1.ConsumeRequest_Ping{
						Ids: []uint64{eventID},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Check that we get the ping response
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_PingConfirmation_); ok {
				Expect(r.PingConfirmation.Ids).To(Equal([]uint64{eventID}))
			} else {
				Fail("expected PingConfirmation message")
			}
		})

		It("pinging unknown event fails", NodeTimeout(5*time.Second), func(ctx context.Context) {
			s, err := service.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
				Stream: "events",
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			client, err := service.Consume(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_Subscribed_); !ok {
				Fail("expected Subscribed message")
			}

			// Ping the event
			err = client.Send(&eventsv1alpha1.ConsumeRequest{
				Request: &eventsv1alpha1.ConsumeRequest_Ping_{
					Ping: &eventsv1alpha1.ConsumeRequest_Ping{
						Ids: []uint64{1},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Check that we get the ping response
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if r, ok := in.Response.(*eventsv1alpha1.ConsumeResponse_PingConfirmation_); ok {
				Expect(r.PingConfirmation.InvalidIds).To(Equal([]uint64{1}))
			} else {
				Fail("expected PingConfirmation message")
			}
		})
	})
})
