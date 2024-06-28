# Windshift

Windshift is an event-stream framework using NATS Jetstream. This repository
contains the Windshift server, which provides a gRPC API for building
distributed event-driven systems.

Windshift provides a schema-first experience where data is represented by
Protobuf messages, to allow your events to evolve over time.

## Features

- 🔗 gRPC API for managing event streams, consumers and publishing events
- 📨 Event handling
  - 🌊 Stream management, declare streams and bind subjects to them, with
    configurable retention and limits
  - 📄 Event data in Protobuf format, for strong typing and schema evolution
  - 📤 Publish events to subjects, with idempotency and OpenTelemetry tracing
  - 📥 Durable consumers with distributed processing
  - 🕒 Ephemeral consumers for one off event processing
  - 🔄 Automatic redelivery of failed events, events can be acknowledged or
    rejected by consumers
  - 🔔 Ability to extend processing time by pinging events
- 💾 State storage
  - 🗄 Supports multiple key-value stores for storing state
  - 📄 Values in Protobuf format, for strong typing and schema evolution
  - 🔄 Optimistic concurrency control using compare and swap
- 🔍 Observability via OpenTelemetry tracing and metrics

### Planned features

- Logging and dead-letter queues for events that fail processing
- Authentication and authorization for API access

## Environment variables

| Name                                  | Description                                                                 | Required | Default                |
| ------------------------------------- | --------------------------------------------------------------------------- | -------- | ---------------------- |
| `DEVELOPMENT`                         | Enable development mode                                                     | No       | `false`                |
| `NATS_URL`                            | URL of the NATS server                                                      | Yes      |                        |
| `NATS_PUBLISH_ASYNC_MAX_PENDING`      | Maximum number of pending messages when publishing events                   | No       | `256`                  |
| `GRPC_PORT`                           | Port to listen on for gRPC requests                                         | No       | `8080`                 |
| `HEALTH_PORT`                         | Port to listen on for health checks                                         | No       | `8088`                 |
| `OTEL_PROPAGATORS`                    | The default propagators to use                                              | No       | `tracecontext,baggage` |
| `OTEL_EXPORTER_OTLP_ENDPOINT`         | The endpoint to send traces, metrics and logs to                            | No       |                        |
| `OTEL_EXPORTER_OTLP_TIMEOUT`          | The timeout in seconds for sending data                                     | No       | `10`                   |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`  | Custom endpoint to send traces to, overrides `OTEL_EXPORTER_OTLP_ENDPOINT`  | No       |                        |
| `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`   | Custom timeout in seconds for sending traces                                | No       | `10`                   |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Custom endpoint to send metrics to, overrides `OTEL_EXPORTER_OTLP_ENDPOINT` | No       |                        |
| `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT`  | Custom timeout in seconds for sending metrics                               | No       | `10`                   |
| `OTEL_METRIC_EXPORT_INTERVAL`         | The interval in seconds to export metrics                                   | No       | `60`                   |
| `OTEL_METRIC_EXPORT_TIMEOUT`          | The timeout in seconds for exporting metrics                                | No       | `30`                   |
| `OTEL_TRACING_LOG`                    | Enable logging mode for tracing                                             | No       | `false`                |

## Usage

### Via Docker

A Docker image is available on GitHub Container Registry:

```console
docker pull ghcr.io/levelfourab/windshift-server:latest
```

Run the server:

```console
docker run --rm -it -p 8080:8080 -e NATS_URL=nats://... ghcr.io/levelfourab/windshift-server:latest
```

### Via Kubernetes

Windshift is stateless and can run in multiple replicas safely.

### Health checks

The server exposes a health check server on port `8088` by default. This
server provides two endpoints:

- `/healthz` - Returns `200 OK` if the server is healthy
- `/readyz` - Returns `200 OK` if the server is ready to handle requests

The port of the health server can be configured via the `HEALTH_PORT` environment
variable.

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

### Defining streams

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

### Publishing events

Events can be published to subjects using the `Publish` method.

Example in pseudo-code:

```typescript
service.Publish(windshift.events.v1alpha1.PublishRequest{
    subject: "orders.created",
    data: protobufMessage,
})
```

Features:

- Timestamps for when the event occurred can be specified with `timestamp`.
- Idempotency keys can be specified using `idempotency_key`. If an event with
  the same idempotency key has already been published, the event will not be
  published again. The window for detecting duplicates can be configured via
  the stream.
- Optimistic concurrency control can be used via `expected_last_id`. If the
  last event in the stream does not have the specified id, the event will not
  be published.

### Defining consumers

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

### Consuming events

Events can be consumed by opening a bi-directional stream using the `Consume`
method. The first message sent to the stream should be a `Subscribe` with the
stream and consumer to subscribe to. After that, events will be sent to the
stream as they are published.

Example in pseudo-code:

```typescript
stream = service.Consume()
stream.Send(windowshift.events.v1alpha1.ConsumeRequest{
  subscribe: &windshift.events.v1alpha1.ConsumeRequest.Subscribe{
    stream: "orders",
    consumer: "order-processor",
  }
})

event = stream.Recv()
```

Events need to be acknowledge or rejected by sending an `Ack` or `Reject` to
the stream. If an event is not acknowledge or rejected within the configured
consumer timeout, it will be redelivered.

Example in pseudo-code:

```typescript
stream.Send(windshift.events.v1alpha1.ConsumeRequest{
  ack: &windshift.events.v1alpha1.ConsumeRequest.Ack{
    ids: [ event.id ],
  }
})
```

If more time is needed to process an event, a `Ping` can be sent to the stream.
This will extend the timeout for the event.

The server will respond with confirmations to `Ack`, `Reject` and `Ping`
messages. These confirmations contain information about if the message was
processed successfully, or if it failed.

## Storing state

Windshift provides the ability to define key-value stores for storing state.

The `windshift.state.v1alpha1.EventsService` is the main gRPC service for
working with state.

### Defining stores

Stores can be created with the `EnsureStore` method. This method will create
a store if it does not exist yet.

Example in pseudo-code:

```typescript
service.EnsureStore(windshift.state.v1alpha1.EnsureStoreRequest{
  name: "orders",
})
```

### Setting values

Values can be set using the `Set` method, either normally, only if the key does
not exist yet, or only if the key already exists in a specific revision.

Example in pseudo-code:

```typescript
result = service.Set(windshift.state.v1alpha1.SetRequest{
  store: "orders",
  key: "order-123",
  value: protobufMessage,
})

revision = result.revision
```

Setting only if the key does not exist yet:

```typescript
result = service.Set(windshift.state.v1alpha1.SetRequest{
  store: "orders",
  key: "order-123",
  value: protobufMessage,
  create_only: true,
})
```

Updating a value with a specific revision:

```typescript
result = service.Set(windshift.state.v1alpha1.SetRequest{
  store: "orders",
  key: "order-123",
  value: protobufMessage,
  last_revision: revision,
})
```

### Getting values

Values can be retrieved using the `Get` method.

Example in pseudo-code:

```typescript
result = service.Get(windshift.state.v1alpha1.GetRequest{
  store: "orders",
  key: "order-123",
})
```

### Deleting values

Values can be deleted using the `Delete` method, either normally or if the
specified revision matches the current revision.

Example in pseudo-code:

```typescript
service.Delete(windshift.state.v1alpha1.DeleteRequest{
  store: "orders",
  key: "order-123",
})
```

Delete only if the revision matches:

```typescript
service.Delete(windshift.state.v1alpha1.DeleteRequest{
  store: "orders",
  key: "order-123",
  last_revision: revision,
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
