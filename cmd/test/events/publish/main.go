package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"
	testv1 "windshift/service/internal/proto/windshift/test/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
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

	i := 0
	for {
		if ctx.Err() != nil {
			return
		}

		value := strconv.Itoa(i)

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

		i++
		log.Println("Sent event: ", i)
	}
}
