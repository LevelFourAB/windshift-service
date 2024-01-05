package v1alpha1_test

import (
	"context"
	"net"
	"os"
	"time"
	"windshift/service/internal/api/events/v1alpha1"
	"windshift/service/internal/events"
	eventsv1alpha1 "windshift/service/internal/proto/windshift/events/v1alpha1"

	"github.com/levelfourab/sprout-go"
	"github.com/levelfourab/sprout-go/test"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func GetClient() (eventsv1alpha1.EventsServiceClient, nats.JetStreamContext) {
	t := GinkgoT()
	var conn *grpc.ClientConn
	var nats *nats.Conn
	fx := fxtest.New(
		t,
		test.Module(t),
		events.Module,
		v1alpha1.Module,
		TestModule,
		fx.Populate(&conn, &nats),
	)
	fx.RequireStart()

	DeferCleanup(func() {
		fx.RequireStop()
	})

	js, err := nats.JetStream()
	Expect(err).ToNot(HaveOccurred())

	return eventsv1alpha1.NewEventsServiceClient(conn), js
}

var TestModule = fx.Module(
	"test",
	fx.Provide(sprout.Logger("grpc.test")),
	fx.Provide(func() *bufconn.Listener {
		return bufconn.Listen(10 * 1024 * 1024)
	}, fx.Private),
	fx.Provide(newServer),
	fx.Provide(newClient),
	fx.Provide(getNATS),
	fx.Provide(newJetStream),
)

func newServer(
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
	listener *bufconn.Listener,
) (*grpc.Server, error) {
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := server.Serve(listener); err != nil {
					logger.Error("Could not start gRPC server", zap.Error(err))
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

func newClient(
	_ *grpc.Server,
	logger *zap.Logger,
	listener *bufconn.Listener,
) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(
		context.Background(),
		"",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	DeferCleanup(func() {
		err := conn.Close()
		if err != nil {
			logger.Error("error closing connection", zap.Error(err))
		}
	})
	return conn, nil
}

func getNATS() *nats.Conn {
	tempDir, err := os.MkdirTemp("", "nats")
	Expect(err).ToNot(HaveOccurred())
	DeferCleanup(func() {
		os.RemoveAll(tempDir)
	})

	ns, err := server.NewServer(&server.Options{
		Port:       -1,
		JetStream:  true,
		StoreDir:   tempDir,
		DontListen: true,
	})
	Expect(err).ToNot(HaveOccurred())
	DeferCleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	go ns.Start()
	if !ns.ReadyForConnections(4 * time.Second) {
		Fail("unable to start nats server")
	}

	natsConn, err := nats.Connect(ns.ClientURL(), nats.InProcessServer(ns))
	Expect(err).ToNot(HaveOccurred())
	DeferCleanup(func() {
		natsConn.Close()
	})
	return natsConn
}

func newJetStream(conn *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(conn, jetstream.WithPublishAsyncMaxPending(256))
}

func Data(msg proto.Message) *anypb.Any {
	data, err := anypb.New(msg)
	Expect(err).ToNot(HaveOccurred())
	return data
}
