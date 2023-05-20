package api

import (
	"context"
	"net"
	"strconv"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func newServer(
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
	config *Config,
) (*grpc.Server, error) {
	server := grpc.NewServer()

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
			server.GracefulStop()
			return nil
		},
	})
	return server, nil
}
