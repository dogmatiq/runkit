package xtesting

import (
	"context"
	"testing"

	"github.com/dogmatiq/runkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// Dial returns an insecure gRPC client connection to the given engines, which
// must be listening for gRPC requests.
//
// The connection sends requests to the first of the engines, and moves to the
// next, in the order given, if that engine stops. It is closed when the test
// ends.
func Dial(t testing.TB, engines ...*runkit.Engine) *grpc.ClientConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var state resolver.State

	for _, e := range engines {
		addr, err := e.ListenAddr(ctx)
		if err != nil {
			t.Fatalf("unable to get the engine's listen address: %v", err)
		}

		state.Addresses = append(
			state.Addresses,
			resolver.Address{
				Addr: addr.String(),
			},
		)
	}

	r := manual.NewBuilderWithScheme("xtesting")
	r.InitialState(state)

	conn, err := grpc.NewClient(
		r.Scheme()+":///engines",
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("unable to create gRPC client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}
