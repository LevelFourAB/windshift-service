package v1alpha1_test

import (
	"context"
	"time"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

			client, err := service.Events(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.EventsRequest{
				Request: &eventsv1alpha1.EventsRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.EventsRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())

			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_Subscribed_); !ok {
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

			client, err := service.Events(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.EventsRequest{
				Request: &eventsv1alpha1.EventsRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.EventsRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_Subscribed_); !ok {
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
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_Event); !ok {
				Fail("expected Event message")
			}

			// Accept the event
			err = client.Send(&eventsv1alpha1.EventsRequest{
				Request: &eventsv1alpha1.EventsRequest_Accept_{
					Accept: &eventsv1alpha1.EventsRequest_Accept{
						Ids: []uint64{in.GetEvent().GetId()},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Check that we get the accepted response
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_AcceptConfirmation_); !ok {
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

			client, err := service.Events(ctx)
			Expect(err).ToNot(HaveOccurred())
			defer client.CloseSend() //nolint:errcheck

			err = client.Send(&eventsv1alpha1.EventsRequest{
				Request: &eventsv1alpha1.EventsRequest_Subscribe_{
					Subscribe: &eventsv1alpha1.EventsRequest_Subscribe{
						Stream:   "events",
						Consumer: s.Id,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			// Receive the subscribe confirmation
			in, err := client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_Subscribed_); !ok {
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
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_Event); !ok {
				Fail("expected Event message")
			}

			// Accept the event
			err = client.Send(&eventsv1alpha1.EventsRequest{
				Request: &eventsv1alpha1.EventsRequest_Reject_{
					Reject: &eventsv1alpha1.EventsRequest_Reject{
						Ids:   []uint64{in.GetEvent().GetId()},
						Retry: true,
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())

			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_RejectConfirmation_); !ok {
				Fail("expected RejectConfirmation message")
			}

			// Receive the event again
			in, err = client.Recv()
			Expect(err).ToNot(HaveOccurred())
			if _, ok := in.Response.(*eventsv1alpha1.EventsResponse_Event); !ok {
				Fail("expected Event message")
			}
		})
	})
})
