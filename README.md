# Windshift

Windshift is an event-stream framework using NATS Jetstream. This repository
contains the Windshift service, which is a gRPC server that provides an API for
building distributed event-driven systems.

## Features

- Events
  - Streams
  - Subscriptions
    - Ephemeral - if a subscription does not have a name it will only
      receive events while it is active.
    - Durable - giving a subscription a name makes it durable, allowing
      for reconnection and distributed processing of events.
    - Event replay - subscribers can request to receive all events for a certain
      duration in the past.
  - Error handling
    - Logging of messages permanent fails
    - Optional support for dead letter queues per stream
- State

## Environment variables

| Name          | Description             | Required | Default |
| ------------- | ----------------------- | -------- | ------- |
| `DEVELOPMENT` | Enable development mode | No       | `false` |
| `NATS_URL`    | URL of the NATS server  | Yes      |         |

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
