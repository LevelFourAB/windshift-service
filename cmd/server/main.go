package main

import (
	"windshift/service/internal/api"
	eventsv1alpha1 "windshift/service/internal/api/events/v1alpha1"
	statev1alpha1 "windshift/service/internal/api/state/v1alpha1"
	"windshift/service/internal/events"
	"windshift/service/internal/nats"
	"windshift/service/internal/state"

	"github.com/levelfourab/sprout-go"
)

var version = "dev"

func main() {
	sprout.New("Windshift", version).With(
		nats.Module,
		events.Module,
		state.Module,
		api.Module,
		eventsv1alpha1.Module,
		statev1alpha1.Module,
	).Run()
}
