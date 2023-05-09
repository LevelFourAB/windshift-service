package v1alpha1_test

import (
	"context"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Publisher", func() {
	var service eventsv1alpha1.EventsServiceClient

	BeforeEach(func() {
		service = GetClient()
	})

	It("can publish to a stream", func(ctx context.Context) {
		_, err := service.EnsureStream(ctx, &eventsv1alpha1.EnsureStreamRequest{
			Name: "test",
			Subjects: []string{
				"test",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		data, err := anypb.New(&emptypb.Empty{})
		Expect(err).ToNot(HaveOccurred())

		_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
			Subject: "test",
			Data:    data,
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("publishing to unbound subject fails", func(ctx context.Context) {
		data, err := anypb.New(&emptypb.Empty{})
		Expect(err).ToNot(HaveOccurred())

		_, err = service.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
			Subject: "test",
			Data:    data,
		})
		Expect(err).To(HaveOccurred())
	})
})
