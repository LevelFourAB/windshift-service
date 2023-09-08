package events_test

import (
	"os"
	"time"
	"windshift/service/internal/events"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func GetNATS() *nats.Conn {
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

func GetJetStream() nats.JetStreamContext {
	natsConn := GetNATS()

	js, err := natsConn.JetStream()
	Expect(err).ToNot(HaveOccurred())
	return js
}

func createManagerAndJetStream() (*events.Manager, jetstream.JetStream) {
	natsConn := GetNATS()

	js, err := jetstream.New(natsConn)
	Expect(err).ToNot(HaveOccurred())

	manager, err := events.NewManager(
		zaptest.NewLogger(GinkgoT()),
		otel.Tracer("tests"),
		natsConn,
	)
	Expect(err).ToNot(HaveOccurred())

	return manager, js
}

func Data(msg proto.Message) *anypb.Any {
	data, err := anypb.New(msg)
	Expect(err).ToNot(HaveOccurred())
	return data
}
