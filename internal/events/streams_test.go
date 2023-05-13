package events_test

import (
	"context"
	"sync"
	"time"
	"windshift/service/internal/events"

	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ = Describe("Streams", func() {
	var manager *events.Manager
	var js nats.JetStreamContext

	BeforeEach(func() {
		manager, js = createManagerAndJetStream()
	})

	It("can create a stream", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(MatchError(nats.ErrStreamNotFound))

		_, err = manager.EnsureStream(ctx, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))
	})

	It("parallel requests to create stream succeed", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(MatchError(nats.ErrStreamNotFound))

		tries := 2
		wg := &sync.WaitGroup{}
		wg.Add(tries)
		errorList := make([]error, 0, tries)

		for i := 0; i < tries; i++ {
			go func() {
				defer wg.Done()
				_, err := manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				if err != nil {
					errorList = append(errorList, err)
				}
			}()
		}

		wg.Wait()
		Expect(errorList).To(BeEmpty())
	})

	It("can update an existing stream", func(ctx context.Context) {
		_, err := js.StreamInfo("test")
		Expect(err).To(MatchError(nats.ErrStreamNotFound))

		_, err = manager.EnsureStream(ctx, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())

		info, err := js.StreamInfo("test")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Config.Name).To(Equal("test"))

		_, err = manager.EnsureStream(ctx, &events.StreamConfig{
			Name: "test",
		})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Sources", func() {
		Context("Subjects", func() {
			It("can create stream with single subject", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
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
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
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
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
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

			It("can remove a subject from a stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
						"test.*",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Subjects).To(ContainElement("test"))
				Expect(info.Config.Subjects).ToNot(ContainElement("test.*"))
			})

			It("can add a subject to a stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
					},
				})
				Expect(err).ToNot(HaveOccurred())
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
						"test.*",
					},
				})
				Expect(err).ToNot(HaveOccurred())
				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Subjects).To(ContainElement("test"))
				Expect(info.Config.Subjects).To(ContainElement("test.*"))
			})

			It("can create stream with multiple subjects and wildcards", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
						"test.1.*",
						"test.2.*",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Subjects).To(ContainElement("test"))
				Expect(info.Config.Subjects).To(ContainElement("test.1.*"))
				Expect(info.Config.Subjects).To(ContainElement("test.2.*"))
			})
		})

		Context("mirroring streams", func() {
			It("can create a mirror of a stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Mirror).ToNot(BeNil())
				Expect(info.Config.Mirror.Name).To(Equal("test"))
			})

			It("a mirror of a stream does not copy old data", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the mirror stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				// Verify that the mirror stream has no messages
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(0)))
			})

			It("a mirror of a stream copies new data", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the mirror stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
					},
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				// Verify that the mirror stream has the message
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})

			It("can create a mirror of a stream with a start time", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				time := time.Now().Add(-time.Minute)
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							Time: time,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Mirror).ToNot(BeNil())
				Expect(info.Config.Mirror.Name).To(Equal("test"))
				Expect(*info.Config.Mirror.OptStartTime).To(BeTemporally("~", time))
			})

			It("a mirror of a stream with a start time copies old data", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the mirror stream
				timestamp := time.Now().Add(-time.Minute)
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							Time: timestamp,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				// Verify that the mirror stream has the message
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})

			It("a mirror of a stream with a start time does not copy too old data", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				// Create the mirror stream
				timestamp := time.Now()
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							Time: timestamp,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				// Verify that the mirror stream does not have the message
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(0)))
			})

			It("can create a mirror of a stream with a start id", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							ID: 1,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Mirror).ToNot(BeNil())
				Expect(info.Config.Mirror.Name).To(Equal("test"))
				Expect(info.Config.Mirror.OptStartSeq).To(Equal(uint64(1)))
			})

			It("can create a mirror of a stream with a start id and it will copy data from id", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish some messages to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())
				e, err := manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the mirror stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							ID: e.ID,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(100 * time.Millisecond)

				// Verify that the mirror stream has the message
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})

			It("can create a mirror of a stream with a start id and it will receive new events", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							ID: 1,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(100 * time.Millisecond)

				// Verify that the mirror stream has the message
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})

			It("can create a mirror of a stream starting at the first event", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							First: true,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Mirror).ToNot(BeNil())
				Expect(info.Config.Mirror.Name).To(Equal("test"))
				Expect(info.Config.Mirror.OptStartSeq).To(Equal(uint64(0)))
			})

			It("can create a mirror of a stream starting at the first event and it will receive old data", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish some messages to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the mirror stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test-mirror",
					Mirror: &events.StreamSource{
						Name: "test",
						Pointer: &events.StreamPointer{
							First: true,
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(100 * time.Millisecond)

				// Verify that the mirror stream has the message
				info, err := js.StreamInfo("test-mirror")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})
		})

		Context("Non-mirrored stream sources", func() {
			It("can have source of another stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test2",
					Sources: []*events.StreamSource{
						{
							Name: "test",
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Sources).To(HaveLen(1))
				Expect(info.Config.Sources[0].Name).To(Equal("test"))
			})

			It("can have multiple sources of another stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test2",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test3",
					Sources: []*events.StreamSource{
						{
							Name: "test",
						},
						{
							Name: "test2",
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test3")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Sources).To(HaveLen(2))
				Expect(info.Config.Sources[0].Name).To(Equal("test"))
				Expect(info.Config.Sources[1].Name).To(Equal("test2"))
			})

			It("does not copy old data by default", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test2",
					Sources: []*events.StreamSource{
						{
							Name: "test",
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				info, err := js.StreamInfo("test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(0)))
			})

			It("can copy from specific id of another stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a few messages to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())
				e, err := manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test2",
					Sources: []*events.StreamSource{
						{
							Name: "test",
							Pointer: &events.StreamPointer{
								ID: e.ID,
							},
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				info, err := js.StreamInfo("test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})

			It("can copy from specific time of another stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a few messages to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(50 * time.Millisecond)
				now := time.Now()

				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test2",
					Sources: []*events.StreamSource{
						{
							Name: "test",
							Pointer: &events.StreamPointer{
								Time: now,
							},
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				info, err := js.StreamInfo("test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})

			It("can copy from start of other stream", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				// Create the source stream
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				// Publish a message to the source stream
				_, err = manager.Publish(ctx, &events.PublishConfig{
					Subject: "test",
					Data:    Data(&emptypb.Empty{}),
				})
				Expect(err).ToNot(HaveOccurred())

				// Create the stream with the source
				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test2",
					Sources: []*events.StreamSource{
						{
							Name: "test",
							Pointer: &events.StreamPointer{
								First: true,
							},
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				time.Sleep(200 * time.Millisecond)

				info, err := js.StreamInfo("test2")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.State.Msgs).To(Equal(uint64(1)))
			})
		})
	})

	Describe("Retention policies", func() {
		It("can create stream with max age", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
		})

		It("can update stream without max age and set max age", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
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
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 2 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxAge).To(Equal(2 * time.Hour))
		})

		It("can remove max age from stream", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(0 * time.Second))
		})

		It("can create stream with max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
		})

		It("can update stream without max messages and set max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
		})

		It("can update stream and change max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 200,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxMsgs).To(Equal(int64(200)))
		})

		It("can remove max messages from stream", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxMsgs).To(Equal(int64(-1)))
		})

		It("can create stream with max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		It("can update stream without max bytes and set max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		It("can update stream and change max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxBytes: 200,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxBytes).To(Equal(int64(200)))
		})

		It("can remove max bytes from stream", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxBytes).To(Equal(int64(-1)))
		})

		It("can create stream with max age and max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxAge:  1 * time.Hour,
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Name).To(Equal("test"))
			Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
		})

		It("can update stream with max age and max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxAge:  1 * time.Hour,
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxAge:  2 * time.Hour,
				MaxMsgs: 200,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(2 * time.Hour))
			Expect(info.Config.MaxMsgs).To(Equal(int64(200)))
		})

		It("can remove max age and max messages from stream", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxAge:  1 * time.Hour,
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(0 * time.Second))
			Expect(info.Config.MaxMsgs).To(Equal(int64(-1)))
		})

		It("can create stream with max age and add max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxAge:  1 * time.Hour,
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
		})

		It("can create stream with max age and replace with max messages", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(0 * time.Second))
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
		})

		It("can create stream with max age and max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxAge:   1 * time.Hour,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		It("can update stream with max age and max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxAge:   1 * time.Hour,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxAge:   2 * time.Hour,
				MaxBytes: 200,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(2 * time.Hour))
			Expect(info.Config.MaxBytes).To(Equal(int64(200)))
		})

		It("can remove max age and max bytes from stream", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxAge:   1 * time.Hour,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(0 * time.Second))
			Expect(info.Config.MaxBytes).To(Equal(int64(-1)))
		})

		It("can create stream with max age and add max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxAge:   1 * time.Hour,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(1 * time.Hour))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		It("can create stream with max age and replace with max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:   "test",
				MaxAge: 1 * time.Hour,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxAge).To(Equal(0 * time.Second))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		It("can create stream with max messages and max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxMsgs:  100,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		It("can update stream with max messages and max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxMsgs:  100,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxMsgs:  200,
				MaxBytes: 200,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxMsgs).To(Equal(int64(200)))
			Expect(info.Config.MaxBytes).To(Equal(int64(200)))
		})

		It("can remove max messages and max bytes from stream", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxMsgs:  100,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxMsgs).To(Equal(int64(-1)))
			Expect(info.Config.MaxBytes).To(Equal(int64(-1)))
		})

		It("can create stream with max messages and add max bytes", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:    "test",
				MaxMsgs: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:     "test",
				MaxMsgs:  100,
				MaxBytes: 100,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.MaxMsgs).To(Equal(int64(100)))
			Expect(info.Config.MaxBytes).To(Equal(int64(100)))
		})

		Describe("Discard policies", func() {
			It("default discard policy is old", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Discard).To(Equal(nats.DiscardOld))
			})

			It("can create stream with discard policy set to old", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name:          "test",
					DiscardPolicy: events.DiscardPolicyOld,
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Discard).To(Equal(nats.DiscardOld))
			})

			It("can create stream with discard policy set to new", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name:          "test",
					DiscardPolicy: events.DiscardPolicyNew,
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Discard).To(Equal(nats.DiscardNew))
			})

			It("can set discard policy to new and per subject", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
					},
					DiscardPolicy:        events.DiscardPolicyNew,
					DiscardNewPerSubject: true,
					MaxMsgsPerSubject:    100,
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Discard).To(Equal(nats.DiscardNew))
				Expect(info.Config.DiscardNewPerSubject).To(BeTrue())
			})

			It("can update discard policy", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name:          "test",
					DiscardPolicy: events.DiscardPolicyNew,
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name:          "test",
					DiscardPolicy: events.DiscardPolicyOld,
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := js.StreamInfo("test")
				Expect(err).ToNot(HaveOccurred())
				Expect(info.Config.Discard).To(Equal(nats.DiscardOld))
			})

			It("setting discard policy to new per subject fails if no max messages per subject", func(ctx context.Context) {
				_, err := js.StreamInfo("test")
				Expect(err).To(MatchError(nats.ErrStreamNotFound))

				_, err = manager.EnsureStream(ctx, &events.StreamConfig{
					Name: "test",
					Subjects: []string{
						"test",
					},
					DiscardPolicy:        events.DiscardPolicyNew,
					DiscardNewPerSubject: true,
				})
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("Storage", func() {
		It("defaults to file storage", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name: "test",
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Storage).To(Equal(nats.FileStorage))
		})

		It("can create stream with file storage", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:        "test",
				StorageType: events.StorageTypeFile,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Storage).To(Equal(nats.FileStorage))
		})

		It("can create stream with memory storage", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:        "test",
				StorageType: events.StorageTypeMemory,
			})
			Expect(err).ToNot(HaveOccurred())

			info, err := js.StreamInfo("test")
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Config.Storage).To(Equal(nats.MemoryStorage))
		})

		It("updating storage type of existing stream errors", func(ctx context.Context) {
			_, err := js.StreamInfo("test")
			Expect(err).To(MatchError(nats.ErrStreamNotFound))

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:        "test",
				StorageType: events.StorageTypeMemory,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = manager.EnsureStream(ctx, &events.StreamConfig{
				Name:        "test",
				StorageType: events.StorageTypeFile,
			})
			Expect(err).To(HaveOccurred())
		})
	})
})
