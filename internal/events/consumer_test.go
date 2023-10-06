package events_test

import (
	"context"
	"windshift/service/internal/events"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap/zaptest"
)

var _ = Describe("Consumers", func() {
	var manager *events.Manager
	var js jetstream.JetStream

	BeforeEach(func() {
		var err error
		natsConn := GetNATS()
		js, err = jetstream.New(natsConn)
		Expect(err).ToNot(HaveOccurred())

		manager, err = events.NewManager(
			zaptest.NewLogger(GinkgoT()),
			otel.Tracer("tests"),
			js,
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
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
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

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())
		})

		It("can create consumer with multiple subjects", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test",
					"test.2",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(c.CachedInfo().Config.FilterSubjects).To(ConsistOf("test", "test.2"))
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

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", s.ID)
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

			_, err = js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())

			s, err = manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test.2",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(Equal("test.2"))
		})

		It("can create consumer with multiple subjects", func(ctx context.Context) {
			_, err := manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.Consumer(ctx, "test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue())

			s, err := manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test",
					"test.2",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(c.CachedInfo().Config.FilterSubjects).To(ConsistOf("test", "test.2"))
		})

		It("can update from one subject to multiple", func(ctx context.Context) {
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

			_, err = js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())

			s, err = manager.EnsureConsumer(ctx, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test.2",
					"test.3",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			c, err := js.Consumer(ctx, "test", s.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(c.CachedInfo().Config.FilterSubject).To(BeEmpty())
			Expect(c.CachedInfo().Config.FilterSubjects).To(ConsistOf("test.2", "test.3"))
		})
	})
})
