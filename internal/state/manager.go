package state

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Manager struct {
	logger *zap.Logger
	tracer trace.Tracer

	js jetstream.JetStream

	stores *keyValueStoreCache
}

type StoreConfig struct {
	Name string
}

type Entry struct {
	Timestamp time.Time
	Revision  uint64
	Value     *anypb.Any
}

type SetResult struct {
	Revision uint64
}

// NewManager creates a new state manager.
func NewManager(
	logger *zap.Logger,
	tracer trace.Tracer,
	js jetstream.JetStream,
) (*Manager, error) {
	manager := &Manager{
		logger: logger,
		tracer: tracer,

		js: js,

		stores: newKeyValueStoreCache(10*time.Minute, func(ctx context.Context, name string) (jetstream.KeyValue, error) {
			res, err := js.KeyValue(ctx, name)
			if errors.Is(err, jetstream.ErrBucketNotFound) {
				return nil, errors.WithStack(ErrStoreNotFound)
			} else if err != nil {
				return nil, errors.WithStack(err)
			}

			return res, nil
		}),
	}

	return manager, nil
}

func (m *Manager) Destroy() {
	m.stores.Destroy()
}

func (m *Manager) EnsureStore(ctx context.Context, config *StoreConfig) error {
	ctx, span := m.tracer.Start(
		ctx,
		"windshift.state.EnsureStore",
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(config.Name),
			semconv.DBOperation("get"),
		),
	)
	defer span.End()

	if !IsValidStoreName(config.Name) {
		span.SetStatus(codes.Error, "invalid store name")
		return newValidationError("invalid store name: " + config.Name)
	}

	_, err := m.stores.Get(ctx, config.Name)
	if errors.Is(err, ErrStoreNotFound) {
		_, err = m.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: config.Name,
		})

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to create store")
			return errors.Wrap(err, "failed to create store")
		}
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return errors.Wrap(err, "failed to get store")
	}

	span.SetStatus(codes.Ok, "")
	return nil
}

// Get returns the value for the given key in the given store. If the key
// doesn't exist, ErrKeyNotFound is returned.
func (m *Manager) Get(ctx context.Context, store string, key string) (*Entry, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"GET "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("get"),
			semconv.DBStatement("get "+key),
		),
	)
	defer span.End()

	err := validatePreconditions(store, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return nil, err
	}

	entry, err := bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			span.SetStatus(codes.Error, "failed to get key")
			return nil, errors.WithStack(ErrKeyNotFound)
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get key")
		return nil, errors.Wrap(err, "failed to get key")
	}

	var value anypb.Any
	err = proto.Unmarshal(entry.Value(), &value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal value")
		return nil, errors.Wrap(err, "failed to unmarshal value")
	}

	span.SetStatus(codes.Ok, "")
	return &Entry{
		Timestamp: entry.Created(),
		Revision:  entry.Revision(),
		Value:     &value,
	}, nil
}

// Create creates the value for the given key in the given store. If the key
// already exists, ErrKeyAlreadyExists is returned.
func (m *Manager) Create(ctx context.Context, store string, key string, value *anypb.Any) (uint64, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"CREATE "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("create"),
			semconv.DBStatement("create "+key),
		),
	)
	defer span.End()

	err := validatePreconditions(store, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return 0, err
	}

	data, err := proto.Marshal(value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to marshal value")
		return 0, errors.Wrap(err, "failed to marshal value")
	}

	r, err := bucket.Create(ctx, key, data)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			span.SetStatus(codes.Error, "key already exists, can not create")
			return 0, errors.WithStack(ErrKeyAlreadyExists)
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to create")
		return 0, errors.Wrap(err, "failed to create")
	}

	span.SetStatus(codes.Ok, "")
	return r, nil
}

// Set sets the value for the given key in the given store.
func (m *Manager) Set(ctx context.Context, store string, key string, value *anypb.Any) (uint64, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"SET "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("set"),
			semconv.DBStatement("set "+key),
		),
	)
	defer span.End()

	err := validatePreconditions(store, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return 0, err
	}

	data, err := proto.Marshal(value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to marshal value")
		return 0, errors.Wrap(err, "failed to marshal value")
	}

	r, err := bucket.Put(ctx, key, data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to set value")
		return 0, errors.Wrap(err, "failed to set value")
	}

	span.SetAttributes(attribute.Int64("db.windshift.revision", int64(r)))
	span.SetStatus(codes.Ok, "")
	return r, nil
}

// Update updates the value for the given key in the given store only if the
// revision matches.
func (m *Manager) Update(ctx context.Context, store string, key string, value *anypb.Any, revision uint64) (uint64, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"UPDATE "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("update"),
			semconv.DBStatement("update "+key),
			attribute.Int64("db.windshift.revision", int64(revision)),
		),
	)
	defer span.End()

	err := validatePreconditions(store, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return 0, err
	}

	data, err := proto.Marshal(value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to marshal value")
		return 0, errors.Wrap(err, "failed to marshal value")
	}

	var apiError *jetstream.APIError
	r, err := bucket.Update(ctx, key, data, revision)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		span.SetStatus(codes.Error, "key not found, can not update")
		return 0, errors.WithStack(ErrKeyNotFound)
	} else if errors.As(err, &apiError) {
		if apiError.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			span.SetStatus(codes.Error, "revision mismatch, can not update")
			return 0, errors.WithStack(ErrRevisionMismatch)
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "bad request, can not update")
		return 0, errors.WithStack(err)
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to update value")
		return 0, errors.Wrap(err, "failed to update value")
	}

	span.SetAttributes(attribute.Int64("db.windshift.revision", int64(r)))
	span.SetStatus(codes.Ok, "")
	return r, nil
}

// Delete deletes the value for the given key in the given store.
func (m *Manager) Delete(ctx context.Context, store string, key string) error {
	ctx, span := m.tracer.Start(
		ctx,
		"DELETE "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("delete"),
			semconv.DBStatement("delete "+key),
		),
	)
	defer span.End()

	err := validatePreconditions(store, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return err
	}

	err = bucket.Delete(ctx, key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to delete key")
		return errors.Wrap(err, "failed to delete key")
	}

	span.SetStatus(codes.Ok, "")
	return nil
}

// DeleteWithRevision deletes the value for the given key in the given store
// only if the revision matches.
func (m *Manager) DeleteWithRevision(ctx context.Context, store string, key string, revision uint64) error {
	ctx, span := m.tracer.Start(
		ctx,
		"DELETE "+store,
		trace.WithAttributes(
			semconv.DBSystemKey.String("windshift"),
			semconv.DBName(store),
			semconv.DBOperation("delete"),
			semconv.DBStatement("delete "+key),
			attribute.Int64("db.windshift.revision", int64(revision)),
		),
	)
	defer span.End()

	err := validatePreconditions(store, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	bucket, err := m.stores.Get(ctx, store)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get store")
		return err
	}

	err = bucket.Delete(ctx, key, jetstream.LastRevision(revision))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to delete key")
		return errors.Wrap(err, "failed to delete key")
	}

	span.SetStatus(codes.Ok, "")
	return nil
}

func validatePreconditions(store string, key string) error {
	if !IsValidStoreName(store) {
		return newValidationError("invalid store name: " + store)
	}

	if !IsValidKey(key) {
		return newValidationError("invalid key: " + key)
	}
	return nil
}
