package api

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
)

func newServer(
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
	config *Config,
) (*grpc.Server, error) {
	gRPCLogger := createLogger(logger)
	loggingOptions := []logging.Option{
		logging.WithLevels(loggingCodeToLevel),
	}
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			logging.UnaryServerInterceptor(gRPCLogger, loggingOptions...),
			recovery.UnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			logging.StreamServerInterceptor(gRPCLogger, loggingOptions...),
			recovery.StreamServerInterceptor(),
		),
	)

	// Make reflection available for gRPC tooling
	reflection.Register(server)

	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			logger.Info("Starting gRPC server", zap.Int("port", config.Port))
			listener, err := net.Listen("tcp", ":"+strconv.Itoa(config.Port))
			if err != nil {
				return err
			}

			go func() {
				if err := server.Serve(listener); err != nil {
					logger.Error("Could not start gRPC server")
				}
			}()

			return nil
		},
		OnStop: func(context.Context) error {
			didStop := make(chan struct{})
			go func() {
				server.GracefulStop()
				close(didStop)
			}()

			select {
			case <-didStop:
				// Graceful stop was successful
				return nil
			case <-time.After(5 * time.Second):
				logger.Warn("Could not gracefully stop gRPC server")
				server.Stop()
			}

			return nil
		},
	})
	return server, nil
}

// createLogger creates a logger that can be used with the gRPC logging
// middleware.
func createLogger(logger *zap.Logger) logging.Logger {
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	return logging.LoggerFunc(func(ctx context.Context, level logging.Level, msg string, fields ...any) {
		zapLevel := zap.DebugLevel
		switch level {
		case logging.LevelDebug:
			zapLevel = zap.DebugLevel
		case logging.LevelInfo:
			zapLevel = zap.InfoLevel
		case logging.LevelWarn:
			zapLevel = zap.WarnLevel
		case logging.LevelError:
			zapLevel = zap.ErrorLevel
		}

		entry := logger.Check(zapLevel, msg)
		if entry == nil {
			return
		}

		// If we are logging a message create the fields we want to write
		zapFields := make([]zap.Field, 0, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			key := fields[i].(string)
			switch v := fields[i+1].(type) {
			case string:
				zapFields = append(zapFields, zap.String(key, v))
			case int:
				zapFields = append(zapFields, zap.Int(key, v))
			case bool:
				zapFields = append(zapFields, zap.Bool(key, v))
			default:
				zapFields = append(zapFields, zap.Any(key, v))
			}
		}

		entry.Write(zapFields...)
	})
}

// loggingCodeToLevel converts a gRPC error code to a logging level. To avoid
// logging too much in production we map non-error codes to the debug level.
func loggingCodeToLevel(code codes.Code) logging.Level {
	switch code {
	case codes.OK, codes.NotFound, codes.Canceled, codes.AlreadyExists,
		codes.InvalidArgument, codes.Unauthenticated:
		return logging.LevelDebug
	case codes.DeadlineExceeded, codes.PermissionDenied,
		codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted,
		codes.OutOfRange, codes.Unavailable:
		return logging.LevelWarn
	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return logging.LevelError
	}

	// For unknown codes output things at an info level.
	return logging.LevelInfo
}
