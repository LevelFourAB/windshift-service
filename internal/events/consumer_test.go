package events_test

import (
	"context"
	"windshift/service/internal/events"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscriptions", func() {
	var js nats.JetStreamContext

	BeforeEach(func() {
		js = GetJetStream()
	})

	Describe("Ephemeral Subscriptions", func() {
		It("can create a subscription", func(ctx context.Context) {
			_, err := events.EnsureStream(ctx, js, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, nats.ErrConsumerNotFound)).To(BeTrue())

			s, err := events.EnsureConsumer(ctx, js, &events.ConsumerConfig{
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
			_, err := events.EnsureStream(ctx, js, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			s, err := events.EnsureConsumer(ctx, js, &events.ConsumerConfig{
				Stream: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())

			s, err = events.EnsureConsumer(ctx, js, &events.ConsumerConfig{
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

	Describe("Durable Subscriptions", func() {
		It("can create a subscription", func(ctx context.Context) {
			_, err := events.EnsureStream(ctx, js, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", "test")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, nats.ErrConsumerNotFound)).To(BeTrue())

			s, err := events.EnsureConsumer(ctx, js, &events.ConsumerConfig{
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
			_, err := events.EnsureStream(ctx, js, &events.StreamConfig{
				Name: "test",
				Subjects: []string{
					"test",
					"test.>",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			s, err := events.EnsureConsumer(ctx, js, &events.ConsumerConfig{
				Stream: "test",
				Name:   "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = js.ConsumerInfo("test", s.ID)
			Expect(err).ToNot(HaveOccurred())

			s, err = events.EnsureConsumer(ctx, js, &events.ConsumerConfig{
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
