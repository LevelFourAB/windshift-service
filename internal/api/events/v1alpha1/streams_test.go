package v1alpha1_test

import (
	"context"
	"time"

	eventsv1alpha1 "github.com/levelfourab/windshift-server/internal/proto/windshift/events/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/durationpb"
)

var _ = Describe("Streams", func() {
	var service eventsv1alpha1.EventsServiceClient

	BeforeEach(func() {
		service, _ = GetClient()
	})

	It("can create a stream", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can update subjects of a stream", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test", "test.>"},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create a stream with multiple subjects", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max age", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxAge: durationpb.New(1 * time.Hour),
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max events", func(ctx context.Context) {
		maxEvents := uint64(100)
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxEvents: &maxEvents,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max bytes", func(ctx context.Context) {
		maxBytes := uint64(1024)
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxBytes: &maxBytes,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max age and max events", func(ctx context.Context) {
		maxEvents := uint64(100)
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxAge:    durationpb.New(1 * time.Hour),
				MaxEvents: &maxEvents,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max age and max bytes", func(ctx context.Context) {
		maxBytes := uint64(1024)
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxAge:   durationpb.New(1 * time.Hour),
				MaxBytes: &maxBytes,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max events and max bytes", func(ctx context.Context) {
		maxEvents := uint64(100)
		maxBytes := uint64(1024)
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxEvents: &maxEvents,
				MaxBytes:  &maxBytes,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can create stream with max age, max events and max bytes", func(ctx context.Context) {
		maxEvents := uint64(100)
		maxBytes := uint64(1024)
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{"test"},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxAge:    durationpb.New(1 * time.Hour),
				MaxEvents: &maxEvents,
				MaxBytes:  &maxBytes,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can update stream without max age and set max age", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{
						"test",
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
				Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
					Subjects: []string{
						"test",
					},
				},
			},
			RetentionPolicy: &eventsv1alpha1.EnsureStreamRequest_RetentionPolicy{
				MaxAge: durationpb.New(1 * time.Hour),
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})
})
