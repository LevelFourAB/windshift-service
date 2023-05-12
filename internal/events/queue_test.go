package events_test

import (
	"context"
	"time"

	"windshift/service/internal/events"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Queue", func() {
	var manager *events.Manager

	BeforeEach(func() {
		natsConn := GetNATS()

		natsEvents, err := events.NewManager(
			zaptest.NewLogger(GinkgoT()),
			otel.Tracer("tests"),
			natsConn,
		)
		Expect(err).ToNot(HaveOccurred())

		_, err = natsEvents.EnsureStream(context.Background(), &events.StreamConfig{
			Name: "events",
			Subjects: []string{
				"events.>",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		manager = natsEvents
	})

	Describe("Ephemeral queues", func() {
		It("can create a queue", func(ctx context.Context) {
			sub, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()
		})

		It("queue without stream name fails to create", func(ctx context.Context) {
			_, err := manager.Subscribe(ctx, &events.QueueConfig{
				Name: "test",
			})
			Expect(err).To(HaveOccurred())
		})

		It("queue without name fails to create", func(ctx context.Context) {
			_, err := manager.Subscribe(ctx, &events.QueueConfig{
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())
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

			queue1, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub1.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue1.Close()

			queue2, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub2.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue2.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue1.Events():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-queue2.Events():
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			select {
			case <-queue.Events():
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
				Pointer: &events.StreamPointer{
					First: true,
				},
			})
			Expect(err).ToNot(HaveOccurred())

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})
	})

	Describe("Durable queues", func() {
		It("can create a queue", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				err = event.Accept()
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

			queue1, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue1.Close()

			queue2, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue2.Close()

			for i := 0; i < 10; i++ {
				msg, err := anypb.New(&emptypb.Empty{})
				Expect(err).ToNot(HaveOccurred())
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "events.test",
					Data:    msg,
				})
				Expect(err).ToNot(HaveOccurred())
			}

			eventsReceived := 0
			queue1EventsReceived := 0
			queue2EventsReceived := 0
		_outer:
			for {
				select {
				case <-queue1.Events():
					eventsReceived++
					queue1EventsReceived++
				case <-queue2.Events():
					eventsReceived++
					queue2EventsReceived++
				case <-time.After(500 * time.Millisecond):
					break _outer
				}
			}

			// Check that the right number of events were received
			Expect(eventsReceived).To(BeNumerically("==", 10))

			// Make sure that each queue has received at least one event
			Expect(queue1EventsReceived).To(BeNumerically(">", 0))
			Expect(queue2EventsReceived).To(BeNumerically(">", 0))
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

			queue1, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test1",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue1.Close()

			queue2, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test2",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue2.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue1.Events():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-queue2.Events():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("closing a queue stops receiving events", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
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

			queue.Close()

			// Check if the event can be received again
			select {
			case _, ok := <-queue.Events():
				if ok {
					Fail("event received after close")
				}
			case <-time.After(200 * time.Millisecond):
			}
		})

		It("accepting event stops delivery", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				err = event.Accept()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-queue.Events():
				Fail("event received again")
			case <-time.After(200 * time.Millisecond):
				// Make sure event isn't delivered for a certain period
			}
		})

		It("can accept event after discarding data", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
				Timeout: 100 * time.Millisecond,
			})
			Expect(err).ToNot(HaveOccurred())

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				event.DiscardData()
				err = event.Accept()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case <-queue.Events():
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				err = event.Reject(true)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				err = event.Accept()
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				event.DiscardData()
				err = event.Reject(true)
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				err = event.Accept()
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}
		})

		It("rejecting event redelivers it to another queue", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "events",
				Name:   "test",
				Subjects: []string{
					"events.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			queue1, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())

			queue2, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())

			defer queue1.Close()
			defer queue2.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-queue1.Events()
			Expect(event).ToNot(BeNil())
			err = queue1.Close()
			Expect(err).ToNot(HaveOccurred())

			err = event.Reject(true)
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case event = <-queue2.Events():
				Expect(event).ToNot(BeNil())
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())
			case <-time.After(200 * time.Millisecond):
				Fail("no event received")
			}

			time.Sleep(200 * time.Millisecond)

			select {
			case event := <-queue.Events():
				Expect(event).ToNot(BeNil())

				err = event.Accept()
				Expect(err).ToNot(HaveOccurred())

				// Check that we have the correct message
				empty := &emptypb.Empty{}
				err = event.Data.UnmarshalTo(empty)
				Expect(err).ToNot(HaveOccurred())
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-queue.Events()
			Expect(event).ToNot(BeNil())

			err = event.Reject(false)
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case <-queue.Events():
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   "test",
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			msg, err := anypb.New(&emptypb.Empty{})
			Expect(err).ToNot(HaveOccurred())
			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    msg,
			})
			Expect(err).ToNot(HaveOccurred())

			event := <-queue.Events()
			Expect(event).ToNot(BeNil())

			err = event.Reject(false)
			Expect(err).ToNot(HaveOccurred())

			// Receive the event again
			select {
			case <-queue.Events():
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

			queue, err := manager.Subscribe(ctx, &events.QueueConfig{
				Stream: "events",
				Name:   sub.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			defer queue.Close()

			_, err = manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			select {
			case event := <-queue.Events():
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
