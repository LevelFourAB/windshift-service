package v1alpha1_test

import (
	"context"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscriptions", func() {
	var service eventsv1alpha1.EventsServiceClient

	BeforeEach(func(ctx context.Context) {
		service = GetClient()

		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Subjects: []string{
				"test",
				"events.>",
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Ephemeral", func() {
		It("can create a subscription", func(ctx context.Context) {
			_, err := service.EnsureSubscription(ctx, &eventsv1alpha1.EnsureSubscriptionRequest{
				Stream: "test",
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("Durable", func() {
		It("can update subject of subscription", func(ctx context.Context) {
			subID := "test-sub"
			_, err := service.EnsureSubscription(ctx, &eventsv1alpha1.EnsureSubscriptionRequest{
				Stream: "test",
				Name:   &subID,
				Subjects: []string{
					"test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = service.EnsureSubscription(ctx, &eventsv1alpha1.EnsureSubscriptionRequest{
				Stream: "test",
				Name:   &subID,
				Subjects: []string{
					"events.test",
				},
			})
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
