package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
	testv1 "windshift/service/internal/proto/windshift/test/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	// Get a flag to determine if individual events should be printed
	printEvents := flag.Bool("print-events", false, "Print individual events")
	workLoadTime := flag.Int("work-load-time", 100, "Time to fake processing an event in milliseconds")
	workLoadRandomness := flag.Int("work-load-randomness", 0, "Randomness to add to the work load time in milliseconds")
	parallelism := flag.Int("parallelism", 1, "Number of parallel consumers")
	durable := flag.String("durable", "", "Durable consumer name")
	processingTimeout := flag.Int("processing-timeout", 30000, "Processing timeout in milliseconds")
	flag.Parse()

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

	var name *string
	if *durable != "" {
		name = durable
	}
	c, err := client.EnsureConsumer(ctx, &eventsv1alpha1.EnsureConsumerRequest{
		Name:     name,
		Stream:   "test",
		Subjects: []string{"test"},
		From: &eventsv1alpha1.StreamPointer{
			Pointer: &eventsv1alpha1.StreamPointer_Start{
				Start: true,
			},
		},
		ProcessingTimeout: durationpb.New(time.Duration(*processingTimeout) * time.Millisecond),
	})
	if err != nil {
		log.Fatal(err)
	}

	stream, err := client.Events(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Send the initial subscription request
	maxProcessingEvents := uint64(*parallelism)
	err = stream.Send(&eventsv1alpha1.EventsRequest{
		Request: &eventsv1alpha1.EventsRequest_Subscribe_{
			Subscribe: &eventsv1alpha1.EventsRequest_Subscribe{
				Stream:              "test",
				Consumer:            c.Id,
				MaxProcessingEvents: &maxProcessingEvents,
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

	incoming := make(chan *eventsv1alpha1.EventsResponse)
	go func() {
		for {
			resp, err2 := stream.Recv()
			if err2 != nil {
				log.Println(err2)
			}

			incoming <- resp
		}
	}()

	r := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

	pending := atomic.Int32{}
	processing := atomic.Int32{}
	processed := atomic.Int32{}
	go func() {
		// Print the counter every second
		timer := time.NewTicker(time.Second)
		for {
			<-timer.C
			current := processed.Swap(0)
			log.Println("Processed", current, "events, in progress=", processing.Load(), "pending=", pending.Load())
		}
	}()

	workQueue := make(chan *eventsv1alpha1.Event, 1000)
	for i := 0; i < *parallelism; i++ {
		go func() {
			for {
				event := <-workQueue
				pending.Add(-1)

				processing.Add(1)
				data, err := event.Data.UnmarshalNew()
				if err != nil {
					log.Println("Could not unmarshal event data:", err)
					return
				}

				switch d := data.(type) {
				case *testv1.StringValue:
					if *printEvents {
						log.Println("Received event", "id=", event.Id, "time=", event.Headers.Timestamp.AsTime(), "value=", d.Value)
					}
				}

				// Fake some processing time
				sleepInMS := *workLoadTime
				if *workLoadRandomness > 0 {
					sleepInMS += r.Intn(*workLoadRandomness)
				}
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)

				// Acknowledge the event
				err = stream.Send(&eventsv1alpha1.EventsRequest{
					Request: &eventsv1alpha1.EventsRequest_Ack_{
						Ack: &eventsv1alpha1.EventsRequest_Ack{
							Ids: []uint64{event.Id},
						},
					},
				})
				if err != nil {
					log.Fatal(err)
				}
				processed.Add(1)
				processing.Add(-1)
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-incoming:
			if resp.GetEvent() != nil {
				// Acquire a semaphore slot
				pending.Add(1)
				workQueue <- resp.GetEvent()
			}
		}
	}
}
