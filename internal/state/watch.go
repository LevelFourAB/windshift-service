package state

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type KeyEvent interface {
	isKeyEvent()
}

type KeySetEvent struct {
	Key      string
	Revision uint64
	Value    []byte
}

func (KeySetEvent) isKeyEvent() {}

type KeyDeleteEvent struct {
	Key      string
	Revision uint64
}

func (KeyDeleteEvent) isKeyEvent() {}

type Watcher struct {
	ctx         context.Context
	natsWatcher jetstream.KeyWatcher
	stopCh      chan struct{}
}

func (w *Watcher) Updates() <-chan KeyEvent {
	ch := make(chan KeyEvent)
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				close(ch)
				return
			case <-w.stopCh:
				close(ch)
				return
			case e := <-w.natsWatcher.Updates():
				if e.Operation() == jetstream.KeyValueDelete || e.Operation() == jetstream.KeyValuePurge {
					ch <- KeyDeleteEvent{
						Key:      e.Key(),
						Revision: e.Revision(),
					}
				} else if e.Operation() == jetstream.KeyValuePut {
					ch <- KeySetEvent{
						Key:      e.Key(),
						Revision: e.Revision(),
						Value:    e.Value(),
					}
				}
			}
		}
	}()
	return ch
}

func (w *Watcher) Stop() error {
	close(w.stopCh)
	return w.natsWatcher.Stop()
}

// Watch watches the given store for changes.
func (m *Manager) Watch(ctx context.Context, store string, key string) (*Watcher, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"WATCH "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("watch"),
			semconv.DBStatement("watch "+key),
		),
	)
	defer span.End()

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		return nil, err
	}

	w, err := bucket.Watch(ctx, key)
	if err != nil {
		return nil, err
	}

	return &Watcher{
		ctx:         ctx,
		natsWatcher: w,
		stopCh:      make(chan struct{}),
	}, nil
}

func (m *Manager) WatchAll(ctx context.Context, store string) (*Watcher, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"WATCH "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("watch"),
			semconv.DBStatement("watch"),
		),
	)
	defer span.End()

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		return nil, err
	}

	w, err := bucket.WatchAll(ctx)
	if err != nil {
		return nil, err
	}

	return &Watcher{
		natsWatcher: w,
	}, nil
}
