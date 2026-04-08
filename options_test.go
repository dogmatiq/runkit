package runkit_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/runkit"
)

func TestWithSite(t *testing.T) {
	t.Run("it panics if the name is empty", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithSite("", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
	})

	t.Run("it panics if the key is not a valid UUID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithSite("my-site", "not-a-uuid")
	})
}

func TestWithNodeID(t *testing.T) {
	t.Run("it panics if the ID is not a valid UUID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithNodeID("not-a-uuid")
	})
}

func TestWithApplication(t *testing.T) {
	t.Run("it panics if the application is nil", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithApplication(nil)
	})

	t.Run("it panics if an application with the same identity key is already registered", func(t *testing.T) {
		app1 := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("app1", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
			},
		}

		app2 := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("app2", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		New(
			WithApplication(app1),
			WithApplication(app2),
		)
	})
}
