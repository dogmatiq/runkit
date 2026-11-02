package runkit_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStaticRemoteEngines(t *testing.T) {
	t.Run("it reports each connection available", func(t *testing.T) {
		a := newClientConn(t)
		b := newClientConn(t)

		reported := make(chan grpc.ClientConnInterface, 10)
		discover := runkit.StaticRemoteEngines(a, b)

		xtesting.GoE(t, func(ctx context.Context) error {
			return discover(
				ctx,
				func(conn grpc.ClientConnInterface) func() {
					reported <- conn
					return func() {}
				},
			)
		})

		for _, want := range []grpc.ClientConnInterface{a, b} {
			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for connection to be reported available")
			case got := <-reported:
				if got != want {
					t.Fatal("unexpected connection")
				}
			}
		}
	})

	t.Run("it panics if a connection is nil", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.StaticRemoteEngines(newClientConn(t), nil)
	})
}

func TestWithRemoteEngineDiscoverer(t *testing.T) {
	t.Run("it panics if the discoverer is nil", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithRemoteEngineDiscoverer(nil)
	})
}

// newClientConn returns a gRPC client connection that is never used.
func newClientConn(t testing.TB) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(
		"127.0.0.1:0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}
