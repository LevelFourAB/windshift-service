# Windshift

Windshift is an event-stream framework using NATS Jetstream. This repository
contains the Windshift service, which is a gRPC server that provides an API for
building distributed event-driven systems.

## Features

- 🔗 gRPC API for managing event streams, consumers and publishing events
- 📨 Event handling
  - 🌊 Stream management, declare streams and bind subjects to them, with
    configurable retention and limits
  - 📤 Publish events to subjects, with idempotency and OpenTelemetry tracing
  - 📥 Durable consumers with distributed processing
  - 🕒 Ephemeral consumers for one of event processing
  - 🔄 Automatic redelivery of failed events, events can be accepted or rejected
    by consumers
  - 🔔 Ability to ping events if processing takes a long time
- 🔍 Observability via OpenTelemetry tracing

### Planned features

- Logging and dead-letter queues for events that fail processing
- Key-value store for state management
- Authentication and authorization for API access

## Environment variables

| Name          | Description             | Required | Default |
| ------------- | ----------------------- | -------- | ------- |
| `DEVELOPMENT` | Enable development mode | No       | `false` |
| `NATS_URL`    | URL of the NATS server  | Yes      |         |

## Events

Event handling in Windshift is fully based around streams of events, with
[NATS Jetstream](https://docs.nats.io/nats-concepts/jetstream) used as the
event store.

Streams can receive events from multiple subjects, but each subject can only
be bound to one stream. For example, if you have a stream called `orders` which
receives events from the `orders.created` subject, you cannot create another
stream that also receives events from `orders.created`.

The `windshift.events.v1alpha1.EventsService` is the main gRPC service for
working with streams, subscriptions and events.

### Streams

Streams can be created with the `EnsureStream` method. This method will create
a stream if it does not exist yet, or update the configuration of an existing
stream.

Example in pseudo-code:

```typescript
service.EnsureStream(windshift.events.v1alpha1.EnsureStreamRequest{
    name: "orders",
    subjects: [ "orders.*" ],
})
```

### Consumers

Consumers are used to process events from streams. Consumers can be durable or
ephemeral. The `EnsureConsumer` method is used to create a consumer.
This method will create a consumer if it does not exist yet, or update the
configuration of an existing consumer.

Example in pseudo-code:

```typescript
service.EnsureConsumer(windshift.events.v1alpha1.EnsureConsumerRequest{
    stream: "orders",
    name: "order-processor", // Provide a name to make it durable
    subjects: [ "orders.created" ],
})
```

## Working with the code

This project depends on [pre-commit](https://pre-commit.com/) to automate
code style, linting and commit messages. After checking out the project do:

```console
pre-commit install -t pre-commit -t commit-msg
```

### Commit messages

For commit messages [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
is used. This helps with ensuring a consistent style of commit messages and
provides a structure that can be used to automate releases and changelogs.

### Code style

`gofmt` and `goimports` are used to ensure consistent code style. The
`pre-commit` configuration will automatically run these tools on every commit.

### Tests

Tests use [Ginkgo](https://onsi.github.io/ginkgo/). Run them via the `gingko`
command:

```console
ginkgo run ./...
```

### Running locally

A `docker-compose.yml` file is provided to run dependencies locally.

```console
docker-compose up
```

It is recommended to create a `.envrc` file and to use [Direnv](https://direnv.net/)
to load environment variables.

Example `.envrc` file:

```sh
export DEVELOPMENT=true
export NATS_URL=nats://127.0.0.1:4222
```

To run the actual service:

```console
go run ./cmd/server
```
