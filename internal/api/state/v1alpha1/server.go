package v1alpha1

import (
	"context"
	statev1alpha1 "windshift/service/internal/proto/windshift/state/v1alpha1"
	"windshift/service/internal/state"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StateServiceServer struct {
	statev1alpha1.UnimplementedStateServiceServer

	logger        *zap.Logger
	w3cPropagator propagation.TextMapPropagator

	state      *state.Manager
	globalStop chan struct{}
}

func newStateServiceServer(
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
	state *state.Manager,
) *StateServiceServer {
	server := &StateServiceServer{
		logger:        logger,
		w3cPropagator: propagation.TraceContext{},

		state:      state,
		globalStop: make(chan struct{}),
	}

	lifecycle.Append(fx.Hook{
		OnStop: func(context.Context) error {
			close(server.globalStop)
			return nil
		},
	})
	return server
}

func (s *StateServiceServer) EnsureStore(ctx context.Context, req *statev1alpha1.EnsureStoreRequest) (*statev1alpha1.EnsureStoreResponse, error) {
	err := s.state.EnsureStore(ctx, &state.StoreConfig{
		Name: req.Store,
	})

	if errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Canceled, "context canceled")
	} else if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.DeadlineExceeded, "timed out")
	} else if state.IsValidationError(err) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, err
	}

	return &statev1alpha1.EnsureStoreResponse{}, nil
}

func (s *StateServiceServer) Get(ctx context.Context, req *statev1alpha1.GetRequest) (*statev1alpha1.GetResponse, error) {
	value, err := s.state.Get(ctx, req.Store, req.Key)
	if errors.Is(err, state.ErrKeyNotFound) {
		// The key doesn't exist, return an empty response.
		return &statev1alpha1.GetResponse{
			Revision: 0,
		}, nil
	} else if errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Canceled, "context canceled")
	} else if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.DeadlineExceeded, "timed out")
	} else if state.IsValidationError(err) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, err
	}

	return &statev1alpha1.GetResponse{
		LastUpdated: timestamppb.New(value.Timestamp),
		Revision:    value.Revision,
		Value:       value.Value,
	}, nil
}

func (s *StateServiceServer) Set(ctx context.Context, req *statev1alpha1.SetRequest) (*statev1alpha1.SetResponse, error) {
	var revision uint64
	var err error
	if req.GetCreateOnly() {
		revision, err = s.state.Create(ctx, req.Store, req.Key, req.Value)
	} else if req.LastRevision != nil {
		revision, err = s.state.Update(ctx, req.Store, req.Key, req.Value, *req.LastRevision)
	} else {
		revision, err = s.state.Set(ctx, req.Store, req.Key, req.Value)
	}

	if errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Canceled, "context canceled")
	} else if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.DeadlineExceeded, "timed out")
	} else if state.IsValidationError(err) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, err
	}

	return &statev1alpha1.SetResponse{
		Revision: revision,
	}, nil
}

func (s *StateServiceServer) Delete(ctx context.Context, req *statev1alpha1.DeleteRequest) (*statev1alpha1.DeleteResponse, error) {
	var err error
	if req.LastRevision != nil {
		err = s.state.DeleteWithRevision(ctx, req.Store, req.Key, *req.LastRevision)
	} else {
		err = s.state.Delete(ctx, req.Store, req.Key)
	}

	if errors.Is(err, context.Canceled) {
		return nil, status.Error(codes.Canceled, "context canceled")
	} else if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.DeadlineExceeded, "timed out")
	} else if state.IsValidationError(err) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, err
	}

	return &statev1alpha1.DeleteResponse{}, nil
}
