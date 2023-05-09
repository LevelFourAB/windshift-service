package v1alpha1_test

import (
	"context"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streams", func() {
	var service eventsv1alpha1.EventsServiceClient

	BeforeEach(func() {
		service = GetClient()
	})

	It("can create a stream", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Subjects: []string{
				"test",
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can update subjects of a stream", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Subjects: []string{
				"test",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Subjects: []string{
				"test",
				"test.>",
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})
})
