package events_test

import (
	"context"
	"time"

	"windshift/service/internal/events"
	testv1 "windshift/service/internal/proto/windshift/test/v1"

	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Publish", func() {
	var manager *events.Manager
	var js jetstream.JetStream

	BeforeEach(func() {
		manager, js = createManagerAndJetStream()

		_, err := manager.EnsureStream(context.Background(), &events.StreamConfig{
			Name: "events",
			Subjects: []string{
				"events.>",
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("publishing to unbound subject fails", func(ctx context.Context) {
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject: "test",
			Data:    Data(&emptypb.Empty{}),
		})
		Expect(err).To(HaveOccurred())
	})

	It("can publish to a stream", func(ctx context.Context) {
		e, err := manager.Publish(ctx, &events.PublishConfig{
			Subject: "events.test",
			Data: Data(&testv1.StringValue{
				Value: "abc",
			}),
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))

		// Check the data
		msg, err := stream.GetMsg(ctx, e.ID)
		Expect(err).ToNot(HaveOccurred())

		// Check that there is a published time header
		h := msg.Header.Get("WS-Published-Time")
		_, err = time.Parse(time.RFC3339Nano, h)
		Expect(err).ToNot(HaveOccurred())

		// Check that the data type is present
		h = msg.Header.Get("WS-Data-Type")
		Expect(h).To(Equal("windshift.test.v1.StringValue"))

		// Check that the data is present
		Expect(msg.Data).ToNot(BeNil())
		testDataBytes, err := proto.Marshal(&testv1.StringValue{
			Value: "abc",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(msg.Data).To(Equal(testDataBytes))
	})

	It("can publish multiple events to a stream", func(ctx context.Context) {
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject: "events.test",
			Data:    Data(&emptypb.Empty{}),
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, &events.PublishConfig{
			Subject: "events.test",
			Data:    Data(&emptypb.Empty{}),
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(2)))
	})

	It("setting published time works", func(ctx context.Context) {
		publishedTime := time.Now().Add(-time.Hour)
		e, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:       "events.test",
			Data:          Data(&emptypb.Empty{}),
			PublishedTime: &publishedTime,
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))

		// Check the data
		msg, err := stream.GetMsg(ctx, e.ID)
		Expect(err).ToNot(HaveOccurred())

		h := msg.Header.Get("WS-Published-Time")
		Expect(h).To(Equal(publishedTime.Format(time.RFC3339Nano)))
	})

	It("setting idempotency key stops second publish", func(ctx context.Context) {
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:        "events.test",
			Data:           Data(&emptypb.Empty{}),
			IdempotencyKey: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, &events.PublishConfig{
			Subject:        "events.test",
			Data:           Data(&emptypb.Empty{}),
			IdempotencyKey: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))
	})

	It("setting idempotency key does not stop second publish with different key", func(ctx context.Context) {
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:        "events.test",
			Data:           Data(&emptypb.Empty{}),
			IdempotencyKey: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, &events.PublishConfig{
			Subject:        "events.test",
			Data:           Data(&emptypb.Empty{}),
			IdempotencyKey: "test2",
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(2)))
	})

	It("setting idempotency key stops second publish with different subject", func(ctx context.Context) {
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:        "events.test",
			Data:           Data(&emptypb.Empty{}),
			IdempotencyKey: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, &events.PublishConfig{
			Subject:        "events.test2",
			Data:           Data(&emptypb.Empty{}),
			IdempotencyKey: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))
	})

	It("setting expected sequence works for first message", func(ctx context.Context) {
		seq := uint64(0)
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:            "events.test",
			Data:               Data(&emptypb.Empty{}),
			ExpectedSubjectSeq: &seq,
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))
	})

	It("setting wrong expected sequence errors for first message", func(ctx context.Context) {
		seq := uint64(1)
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:            "events.test",
			Data:               Data(&emptypb.Empty{}),
			ExpectedSubjectSeq: &seq,
		})
		Expect(err).To(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(0)))
	})

	It("setting expected sequence works for second message", func(ctx context.Context) {
		seq := uint64(0)
		e, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:            "events.test",
			Data:               Data(&emptypb.Empty{}),
			ExpectedSubjectSeq: &seq,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, &events.PublishConfig{
			Subject:            "events.test",
			Data:               Data(&emptypb.Empty{}),
			ExpectedSubjectSeq: &e.ID,
		})
		Expect(err).ToNot(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(2)))
	})

	It("setting wrong expected sequence errors for second message", func(ctx context.Context) {
		seq := uint64(0)
		_, err := manager.Publish(ctx, &events.PublishConfig{
			Subject:            "events.test",
			Data:               Data(&emptypb.Empty{}),
			ExpectedSubjectSeq: &seq,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Publish(ctx, &events.PublishConfig{
			Subject:            "events.test",
			Data:               Data(&emptypb.Empty{}),
			ExpectedSubjectSeq: &seq,
		})
		Expect(err).To(HaveOccurred())

		// Verify that we have the correct number of messages in the stream
		stream, err := js.Stream(ctx, "events")
		Expect(err).ToNot(HaveOccurred())
		Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))
	})

	Describe("OpenTelemetry", func() {
		var tracer trace.Tracer

		BeforeEach(func() {
			// Set up a trace.Tracer that will record all spans
			tracingProvider := sdktrace.NewTracerProvider()
			tracer = tracingProvider.Tracer("test")
		})

		It("SpanContext is published as headers", func(ctx context.Context) {
			ctx, span := tracer.Start(ctx, "test")
			defer span.End()

			id := span.SpanContext().TraceID().String()

			_, err := manager.Publish(ctx, &events.PublishConfig{
				Subject: "events.test",
				Data:    Data(&emptypb.Empty{}),
			})
			Expect(err).ToNot(HaveOccurred())

			// Verify that we have the correct number of messages in the stream
			stream, err := js.Stream(ctx, "events")
			Expect(err).ToNot(HaveOccurred())
			Expect(stream.CachedInfo().State.Msgs).To(Equal(uint64(1)))

			// Check the data
			msg, err := stream.GetMsg(ctx, 1)
			Expect(err).ToNot(HaveOccurred())

			h := msg.Header.Get("WS-Trace-Parent")
			Expect(h).To(ContainSubstring(id))
		})
	})
})
