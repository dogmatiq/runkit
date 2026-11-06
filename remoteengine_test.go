package runkit_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"google.golang.org/grpc"
)

func TestStaticRemoteEngines(t *testing.T) {
	t.Run("it adds each connection", func(t *testing.T) {
		a := xtesting.DialGRPCAddr(t, "127.0.0.1:0")
		b := xtesting.DialGRPCAddr(t, "127.0.0.1:0")

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
				t.Fatal("timeout waiting for connection to be added")
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

		runkit.StaticRemoteEngines(xtesting.DialGRPCAddr(t, "127.0.0.1:0"), nil)
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
