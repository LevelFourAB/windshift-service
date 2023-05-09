package events_test

import (
	"context"
	"time"
	"windshift/service/internal/events"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streams", func() {
	var js nats.JetStreamContext

	BeforeEach(func() {
		js = GetJetStream()
	})

	It("can create an stream", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
	})

	It("can update an existing stream", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with single subject", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
			Subjects: []string{
				"test",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.Subjects).To(ContainElement("test"))
	})

	It("can update stream with single subject", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
			Subjects: []string{
				"test",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
			Subjects: []string{
				"test2",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.Subjects).To(ContainElement("test2"))
		Expect(info.Config.Subjects).ToNot(ContainElement("test"))
	})

	It("can create stream with multiple subjects", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
			Subjects: []string{
				"test",
				"test.*",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.Subjects).To(ContainElement("test"))
		Expect(info.Config.Subjects).To(ContainElement("test.*"))
	})

	It("can create stream with max age", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:   "test",
			MaxAge: 1 * time.Hour,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
	})

	It("can update stream without max and set max age", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:   "test",
			MaxAge: 1 * time.Hour,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
	})

	It("can update stream and change max age", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:   "test",
			MaxAge: 1 * time.Hour,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:   "test",
			MaxAge: 2 * time.Hour,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxAge).To(Equal(2 * time.Hour))
	})

	It("can create stream with max messages", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:    "test",
			MaxMsgs: 100,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
	})

	It("can update stream without max and set max messages", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:    "test",
			MaxMsgs: 100,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
	})

	It("can update stream and change max messages", func(ctx context.Context) { //nolint:dupl
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:    "test",
			MaxMsgs: 100,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:    "test",
			MaxMsgs: 200,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxMsgs).To(Equal(int64(200)))
	})

	It("can create stream with max bytes", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:     "test",
			MaxBytes: 100,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxBytes).To(Equal(int64(100)))
	})

	It("can update stream without max and set max bytes", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:     "test",
			MaxBytes: 100,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxBytes).To(Equal(int64(100)))
	})

	It("can update stream and change max bytes", func(ctx context.Context) { //nolint:dupl
		_, err := js.StreamInfo("test")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, nats.ErrStreamNotFound)).To(BeTrue())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:     "test",
			MaxBytes: 100,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = events.EnsureStream(ctx, js, &events.StreamConfig{
			Name:     "test",
			MaxBytes: 200,
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
		Expect(info.Config.MaxBytes).To(Equal(int64(200)))
	})
})
