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

// DialGRPC returns an insecure gRPC client connection to the given engines,
// which must be listening for gRPC requests.
//
// The connection sends requests to the first of the engines, and moves to the
// next, in the order given, if that engine stops. It is closed when the test
// ends.
func DialGRPC(t testing.TB, engines ...*runkit.Engine) *grpc.ClientConn {
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

	return DialGRPCAddr(
		t,
		r.Scheme()+":///engines",
		grpc.WithResolvers(r),
	)
}

// DialGRPCAddr returns an insecure gRPC client connection to the given target,
// which is usually the address of a gRPC server. The options are applied after
// the defaults, so they may override them.
//
// It is closed when the test ends.
func DialGRPCAddr(
	t testing.TB,
	target string,
	options ...grpc.DialOption,
) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(
		target,
		append(
			[]grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
			options...,
		)...,
	)
	if err != nil {
		t.Fatalf("unable to create gRPC client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}
