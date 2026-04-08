package runkit

import (
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

func TestFromEnvironment(t *testing.T) {
	wantSiteName := "test-site"
	wantSite := "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
	wantNode := "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"

	t.Setenv("DOGMA_SITE_NAME", wantSiteName)
	t.Setenv("DOGMA_SITE_KEY", wantSite)
	t.Setenv("DOGMA_NODE_ID", wantNode)

	t.Run("it sets the site identity from the environment", func(t *testing.T) {
		e := New(FromEnvironment())

		want, err := uuidpb.Parse(wantSite)
		if err != nil {
			t.Fatal(err)
		}

		if e.site.Name != wantSiteName {
			t.Fatalf("got site name %q, want %q", e.site.Name, wantSiteName)
		}

		if !e.site.Key.Equal(want) {
			t.Fatalf("got site key %s, want %s", e.site.Key, want)
		}
	})

	t.Run("it sets the node ID from the environment", func(t *testing.T) {
		e := New(
			WithSite("test-site", wantSite),
			FromEnvironment(),
		)

		want, err := uuidpb.Parse(wantNode)
		if err != nil {
			t.Fatal(err)
		}

		if !e.nodeID.Equal(want) {
			t.Fatalf("got node ID %s, want %s", e.nodeID, want)
		}
	})

	t.Run("explicit WithSite wins over environment", func(t *testing.T) {
		explicit := "22222222-2222-4222-8222-222222222222"
		e := New(
			FromEnvironment(),
			WithSite("explicit-site", explicit),
		)

		want, err := uuidpb.Parse(explicit)
		if err != nil {
			t.Fatal(err)
		}

		if !e.site.Key.Equal(want) {
			t.Fatalf("got site key %s, want %s", e.site.Key, want)
		}
	})

	t.Run("explicit WithNodeID wins over environment", func(t *testing.T) {
		explicit := "33333333-3333-4333-8333-333333333333"
		e := New(
			WithSite("test-site", wantSite),
			FromEnvironment(),
			WithNodeID(explicit),
		)

		want, err := uuidpb.Parse(explicit)
		if err != nil {
			t.Fatal(err)
		}

		if !e.nodeID.Equal(want) {
			t.Fatalf("got node ID %s, want %s", e.nodeID, want)
		}
	})
}
