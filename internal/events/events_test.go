package events_test

import (
	"context"
	"time"

	"windshift/service/internal/events"
	testv1 "windshift/service/internal/proto/windshift/test/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Event Consumption", func() {
	var manager *events.Manager

	BeforeEach(func() {
		manager, _ = createManagerAndJetStream()

		_, err := manager.EnsureStream(context.Background(), &events.StreamConfig{
			Name: "events",
			Subjects: []string{
				"events.>",
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Ephemeral consumption", func() {
		It("can create", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()
		})

		It("without stream name fails to create", func(ctx context.Context) {
			_, err := manager.Events(ctx, &events.EventConsumeConfig{
				Name: "test",
			})
			Expect(err).To(HaveOccurred())
		})

		It("without name fails to create", func(ctx context.Context) {
			_, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
			})
			Expect(err).To(HaveOccurred())
		})

		It("can receive events", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg := &testv1.StringValue{
				Value: "test",
			}
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    Data(msg),
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.Subject).To(Equal("events.test"))
				Expect(event.Data).ToNot(BeNil())
				data, err := event.Data.UnmarshalNew()
				Expect(err).ToNot(HaveOccurred())
				if msg2, ok := data.(*testv1.StringValue); ok {
					Expect(msg2.Value).To(BeEquivalentTo(msg.Value))
				} else {
					Fail("wrong type")
				}
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("multiple subscribers receive same events", func(ctx context.Context) {
			sub1, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			sub2, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub1.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec1.Close()

			ec2, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub2.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec2.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec1.Incoming():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec2.Incoming():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("will not receive events published before subscription", func(ctx context.Context) {
			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			select {
			case <-ec.Incoming():
				Fail("received event")
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("can receive events published before subscription", func(ctx context.Context) {
			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			sub, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
				From: &events.StreamPointer{
					First: true,
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})
	})

	Describe("Durable consumption", func() {
		It("can create", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()
		})

		It("can receive events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())

				err = event.Ack()
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.Data.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("can receive events with multiple subscribers with same name", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec1.Close()

			ec2, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec2.Close()

			for i := 0; i < 10; i++ {
				msg, err2 := anypb.New(&emptypb.Empty{})
				Expect(err2).ToNot(HaveOccurred())
				_, err2 = manager.Publish(ctx, &events.PublishConfig{
					Subject: "events.test",
					Data:    msg,
				})
				Expect(err2).ToNot(HaveOccurred())
			}

			eventsReceived := 0
			ec1EventsReceived := 0
			ec2EventsReceived := 0
		_outer:
			for {
				select {
				case e := <-ec1.Incoming():
					eventsReceived++
					ec1EventsReceived++
					err = e.Ack()
					Expect(err).ToNot(HaveOccurred())
				case e := <-ec2.Incoming():
					eventsReceived++
					ec2EventsReceived++
					err = e.Ack()
					Expect(err).ToNot(HaveOccurred())
				case <-time.After(500 * time.Millisecond):
					break _outer
				}
			}

			// Check that the right number of events were received
			Expect(eventsReceived).To(BeNumerically("==", 10))

			// Make sure that each instance has received at least one event
			Expect(ec1EventsReceived).To(BeNumerically(">", 0))
			Expect(ec2EventsReceived).To(BeNumerically(">", 0))
		})

		It("multiple subscribers with different names receive same events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test1",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test2",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test1",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec1.Close()

			ec2, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test2",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec2.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec1.Incoming():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec2.Incoming():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("closing stops receiving events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			ec.Close()

			// Check if the event can be received again
			select {
			case _, ok := <-ec.Incoming():
				if ok {
					Fail("event received after close")
				}
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("acknowledging event stops delivery", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())

				err = event.Ack()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-ec.Incoming():
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
				// Make sure event isn't delivered for a certain period
			}
		})

		It("can acknowledge event after discarding data", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())

				event.DiscardData()
				err = event.Ack()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-ec.Incoming():
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
				// Make sure event isn't delivered for a certain period
			}
		})

		It("rejecting event redelivers it", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 1))

				err = event.Reject()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 2))

				err = event.Ack()
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.Data.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("can reject event after discarding data", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 1))

				event.DiscardData()
				err = event.Reject()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 2))

				err = event.Ack()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("rejecting event redelivers it to another instance", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec1, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())

			ec2, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())

			defer ec1.Close()
			defer ec2.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-ec1.Incoming()
			Expect(event).ToNot(BeNil())
			Expect(event.DeliveryAttempt).To(BeNumerically("==", 1))
			err = ec1.Close()
			Expect(err).ToNot(HaveOccurred())

			err = event.Reject()
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case event = <-ec2.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 2))
			case <-time.After(500 * time.Millisecond):
				Fail("timeout waiting for event")
			}
		})

		It("not processing event redelivers it", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 1))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			time.Sleep(200 * time.Millisecond)

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())
				Expect(event.DeliveryAttempt).To(BeNumerically("==", 2))

				err = event.Ack()
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.Data.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(1000 * time.Millisecond):
				Fail("redelivered event not received")
			}
		})

		It("can reject with a delay", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-ec.Incoming()
			Expect(event).ToNot(BeNil())
			Expect(event.DeliveryAttempt).To(BeNumerically("==", 1))

			start := time.Now()
			err = event.RejectWithDelay(100 * time.Millisecond)
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case event := <-ec.Incoming():
				if time.Since(start) < 100*time.Millisecond {
					Fail("event received too early")
				}

				Expect(event.DeliveryAttempt).To(BeNumerically("==", 2))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("permanently rejecting event does not redeliver it", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-ec.Incoming()
			Expect(event).ToNot(BeNil())

			err = event.RejectPermanently()
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case <-ec.Incoming():
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("event gets permanently rejected after max deliveries is reached", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				MaxDeliveryAttempts: 1,
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-ec.Incoming()
			Expect(event).ToNot(BeNil())

			err = event.Reject()
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case <-ec.Incoming():
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
			}
		})
	})

	Describe("OpenTelemetry", func() {
		var tracer trace.Tracer

		BeforeEach(func() {
			// Set up a trace.Tracer that will record all spans
			tracingProvider := sdktrace.NewTracerProvider()
			tracer = tracingProvider.Tracer("test")
		})

		It("receiving an event creates a span", func(ctx context.Context) {
			ctx, span := tracer.Start(ctx, "test")
			defer span.End()

			sub, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ec, err := manager.Events(ctx, &events.EventConsumeConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer ec.Close()

			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-ec.Incoming():
				Expect(event).ToNot(BeNil())

				eventSpan := trace.SpanFromContext(event.Context)
				Expect(eventSpan.SpanContext().IsValid()).To(BeTrue())
				Expect(eventSpan.SpanContext().TraceID().String()).To(Equal(span.SpanContext().TraceID().String()))
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})
	})
})
