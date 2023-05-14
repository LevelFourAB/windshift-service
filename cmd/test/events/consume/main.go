package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"
	"windshift/service/internal/events/flowcontrol"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
	testv1 "windshift/service/internal/proto/windshift/test/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := eventsv1alpha1.NewEventsServiceClient(conn)

	_, err = client.EnsureStream(context.Background(), &eventsv1alpha1.EnsureStreamRequest{
		Name: "test",
		Source: &eventsv1alpha1.EnsureStreamRequest_Subjects_{
			Subjects: &eventsv1alpha1.EnsureStreamRequest_Subjects{
				Subjects: []string{"test"},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	c, err := client.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
		Stream:   "test",
		Subjects: []string{"test"},
		Pointer: &eventsv1alpha1.StreamPointer{
			Pointer: &eventsv1alpha1.StreamPointer_Start{
				Start: true,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	stream, err := client.Consume(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Send the initial subscription request
	err = stream.Send(&eventsv1alpha1.ConsumeRequest{
		Request: &eventsv1alpha1.ConsumeRequest_Subscribe_{
			Subscribe: &eventsv1alpha1.ConsumeRequest_Subscribe{
				Stream:   "test",
				Consumer: c.Id,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Receive the subscribe response
	_, err = stream.Recv()
	if err != nil {
		log.Fatal(err)
	}

	incoming := make(chan *eventsv1alpha1.ConsumeResponse)
	go func() {
		for {
			resp, err2 := stream.Recv()
			if err2 != nil {
				log.Println(err2)
			}

			incoming <- resp
		}
	}()

	semaphore := flowcontrol.NewDynamicSemaphore(10)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-incoming:
			if resp.GetEvent() != nil {
				// Acquire a semaphore slot
				semaphore.Acquire()
				go func() {
					defer semaphore.Release()
					event := resp.GetEvent()
					data, err := event.Data.UnmarshalNew()
					if err != nil {
						log.Println("Could not unmarshal event data:", err)
						return
					}

					switch d := data.(type) {
					case *testv1.StringValue:
						log.Println(event.Headers.Timestamp.AsTime(), d.Value)
					}

					// Fake some processing time
					time.Sleep((100 + time.Duration(r.Intn(100))) * time.Millisecond)

					// Accept the event
					err = stream.Send(&eventsv1alpha1.ConsumeRequest{
						Request: &eventsv1alpha1.ConsumeRequest_Accept_{
							Accept: &eventsv1alpha1.ConsumeRequest_Accept{
								Ids: []uint64{event.Id},
							},
						},
					})
					if err != nil {
						log.Fatal(err)
					}
				}()
			}
		}
	}
}
