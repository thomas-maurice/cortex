// Package natstest starts an embedded NATS server with JetStream for tests. It is
// imported only from _test.go files, so nats-server never reaches a binary.
package natstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// Start runs a throwaway JetStream-enabled server (random port, temp store dir) and
// returns a connected JetStream handle. Everything is torn down with the test.
func Start(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "embedded nats not ready")
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })

	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(15*time.Second))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return nc, js
}
