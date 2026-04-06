package runkit

import (
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

func TestFromEnvironment(t *testing.T) {
	wantSite := "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
	wantNode := "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"

	t.Setenv("DOGMA_SITE_ID", wantSite)
	t.Setenv("DOGMA_NODE_ID", wantNode)

	t.Run("it sets the site ID from the environment", func(t *testing.T) {
		e := New(FromEnvironment())

		want, err := uuidpb.Parse(wantSite)
		if err != nil {
			t.Fatal(err)
		}

		if e.siteID.AsString() != want.AsString() {
			t.Fatalf("got site ID %s, want %s", e.siteID.AsString(), want.AsString())
		}
	})

	t.Run("it sets the node ID from the environment", func(t *testing.T) {
		e := New(
			WithSiteID(wantSite),
			FromEnvironment(),
		)

		want, err := uuidpb.Parse(wantNode)
		if err != nil {
			t.Fatal(err)
		}

		if e.nodeID.AsString() != want.AsString() {
			t.Fatalf("got node ID %s, want %s", e.nodeID.AsString(), want.AsString())
		}
	})

	t.Run("explicit WithSiteID wins over environment", func(t *testing.T) {
		explicit := "22222222-2222-4222-8222-222222222222"
		e := New(
			FromEnvironment(),
			WithSiteID(explicit),
		)

		want, err := uuidpb.Parse(explicit)
		if err != nil {
			t.Fatal(err)
		}

		if e.siteID.AsString() != want.AsString() {
			t.Fatalf("got site ID %s, want %s", e.siteID.AsString(), want.AsString())
		}
	})

	t.Run("explicit WithNodeID wins over environment", func(t *testing.T) {
		explicit := "33333333-3333-4333-8333-333333333333"
		e := New(
			WithSiteID(wantSite),
			FromEnvironment(),
			WithNodeID(explicit),
		)

		want, err := uuidpb.Parse(explicit)
		if err != nil {
			t.Fatal(err)
		}

		if e.nodeID.AsString() != want.AsString() {
			t.Fatalf("got node ID %s, want %s", e.nodeID.AsString(), want.AsString())
		}
	})
}
