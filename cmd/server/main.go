package main

import (
	"windshift/service/internal/api"
	"windshift/service/internal/api/v1alpha1"
	"windshift/service/internal/events"
	"windshift/service/internal/nats"

	"github.com/levelfourab/sprout-go"
)

var version = "dev"

func main() {
	sprout.New("Windshift", version).With(
		nats.Module,
		events.Module,
		api.Module,
		v1alpha1.Module,
	).Run()
}
