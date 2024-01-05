package state_test

import (
	"context"

	"windshift/service/internal/state"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("State Store", func() {
	var manager *state.Manager
	//var js nats.JetStreamContext

	BeforeEach(func(ctx context.Context) {
		manager, _ = createManagerAndJetStream()

		err := manager.EnsureStore(ctx, &state.StoreConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("can set and get a value", func(ctx context.Context) {
		_, err := manager.Set(ctx, "test", "key", Data(wrapperspb.String("value")))
		Expect(err).ToNot(HaveOccurred())

		value, err := manager.Get(ctx, "test", "key")
		Expect(err).ToNot(HaveOccurred())
		Expect(value).ToNot(BeNil())

		var unmarshalled wrapperspb.StringValue
		err = value.Value.UnmarshalTo(&unmarshalled)
		Expect(err).ToNot(HaveOccurred())
		Expect(unmarshalled.Value).To(Equal("value"))
	})

	It("getting unset value returns error", func(ctx context.Context) {
		value, err := manager.Get(ctx, "test", "key")
		Expect(err).To(MatchError(state.ErrKeyNotFound))
		Expect(value).To(BeNil())
	})

	It("can create value if it does not exist", func(ctx context.Context) {
		_, err := manager.Create(ctx, "test", "key", Data(wrapperspb.String("value")))
		Expect(err).ToNot(HaveOccurred())

		value, err := manager.Get(ctx, "test", "key")
		Expect(err).ToNot(HaveOccurred())
		Expect(value).ToNot(BeNil())

		var unmarshalled wrapperspb.StringValue
		err = value.Value.UnmarshalTo(&unmarshalled)
		Expect(err).ToNot(HaveOccurred())
		Expect(unmarshalled.Value).To(Equal("value"))
	})

	It("can not create value if it already exists", func(ctx context.Context) {
		_, err := manager.Create(ctx, "test", "key", Data(wrapperspb.String("value")))
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Create(ctx, "test", "key", Data(wrapperspb.String("value")))
		Expect(err).To(MatchError(state.ErrKeyAlreadyExists))
	})

	It("can update a value with a revision", func(ctx context.Context) {
		revision, err := manager.Set(ctx, "test", "key", Data(wrapperspb.String("value")))
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Update(ctx, "test", "key", Data(wrapperspb.String("value2")), revision)
		Expect(err).ToNot(HaveOccurred())

		value, err := manager.Get(ctx, "test", "key")
		Expect(err).ToNot(HaveOccurred())
		Expect(value).ToNot(BeNil())

		var unmarshalled wrapperspb.StringValue
		err = value.Value.UnmarshalTo(&unmarshalled)
		Expect(err).ToNot(HaveOccurred())
		Expect(unmarshalled.Value).To(Equal("value2"))
	})

	It("can not update value with wrong revision", func(ctx context.Context) {
		revision, err := manager.Set(ctx, "test", "key", Data(wrapperspb.String("value")))
		Expect(err).ToNot(HaveOccurred())

		_, err = manager.Update(ctx, "test", "key", Data(wrapperspb.String("value2")), revision+1)
		Expect(err).To(MatchError(state.ErrRevisionMismatch))
	})

	It("can update non-existent value with revision 0", func(ctx context.Context) {
		_, err := manager.Update(ctx, "test", "key", Data(wrapperspb.String("value")), 0)
		Expect(err).ToNot(HaveOccurred())

		value, err := manager.Get(ctx, "test", "key")
		Expect(err).ToNot(HaveOccurred())
		Expect(value).ToNot(BeNil())

		var unmarshalled wrapperspb.StringValue
		err = value.Value.UnmarshalTo(&unmarshalled)
		Expect(err).ToNot(HaveOccurred())
		Expect(unmarshalled.Value).To(Equal("value"))
	})

	It("can not update non-existent value with revision > 0", func(ctx context.Context) {
		_, err := manager.Update(ctx, "test", "key", Data(wrapperspb.String("value")), 1)
		Expect(err).To(MatchError(state.ErrRevisionMismatch))
	})
})
