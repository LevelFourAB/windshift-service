package main

import (
	"github.com/levelfourab/windshift-server/internal/api"
	eventsv1alpha1 "github.com/levelfourab/windshift-server/internal/api/events/v1alpha1"
	statev1alpha1 "github.com/levelfourab/windshift-server/internal/api/state/v1alpha1"
	"github.com/levelfourab/windshift-server/internal/events"
	"github.com/levelfourab/windshift-server/internal/nats"
	"github.com/levelfourab/windshift-server/internal/state"

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
