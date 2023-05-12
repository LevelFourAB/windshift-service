package events_test

import (
	"context"
	"windshift/service/internal/events"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap/zaptest"
)

var _ = Describe("Consumers", func() {
	var manager *events.Manager
	var js nats.JetStreamContext

	BeforeEach(func() {
		var err error
		natsConn := GetNATS()
		js, err = natsConn.JetStream()
		Expect(err).ToNot(HaveOccurred())

		manager, err = events.NewManager(
			zaptest.NewLogger(GinkgoT()),
			otel.Tracer("tests"),
			natsConn,
		)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Configuration issues", func() {
		It("consumer with no stream fails", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Subjects: []string{"test"},
			})
			Expect(err).To(HaveOccurred())
		})

		It("consumer with zero subjects fails", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
			})
			Expect(err).To(HaveOccurred())
		})

		It("consumer with multiple subjects fails", func(ctx context.Context) {
			_, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Ephemeral", func() {
		It("can create a subscription", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, nats.ErrConsumerNotFound)).To(BeTrue())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())
		})

		It("can update subject of subscription", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())

			s, err = manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test.2",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ci, err := js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(ci.Config.FilterSubject).To(Equal("test.2"))
		})
	})

	Describe("Durable", func() {
		It("can create a subscription", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, nats.ErrConsumerNotFound)).To(BeTrue())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())
		})

		It("can update subject of subscription", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())

			s, err = manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test.2",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ci, err := js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(ci.Config.FilterSubject).To(Equal("test.2"))
		})
	})
})
