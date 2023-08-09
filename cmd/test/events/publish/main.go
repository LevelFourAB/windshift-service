package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
	testv1 "windshift/service/internal/proto/windshift/test/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
)

func main() {
	parallelism := flag.Int("parallelism", 1, "Number of parallel publishers")
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

	total := atomic.Int32{}
	processed := atomic.Int32{}
	go func() {
		// Print the counter every second
		timer := time.NewTicker(time.Second)
		for {
			<-timer.C
			current := processed.Swap(0)
			t := total.Add(current)
			log.Println("Generated", current, "events, total=", t)
		}
	}()

	current := atomic.Int32{}

	// Start a goroutine for each parallel publisher
	for j := 0; j < *parallelism; j++ {
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}

				v := current.Add(1)
				value := strconv.Itoa(int(v))

				data, err := anypb.New(&testv1.StringValue{
					Value: value,
				})
				if err != nil {
					log.Println(err)
					return
				}

				_, err = client.PublishEvent(ctx, &eventsv1alpha1.PublishEventRequest{
					Subject: "test",
					Data:    data,
				})
				if err != nil {
					log.Println("Could not send event", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}

				//log.Println("Sent event: ", v)
				processed.Add(1)
			}
		}()
	}

	<-ctx.Done()
}
