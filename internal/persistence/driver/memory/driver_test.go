package memory_test

import (
	"bytes"
	"net/url"
	"strings"
	"testing"

	. "github.com/dogmatiq/runkit/internal/persistence/driver/memory"
)

func TestNewProvider(t *testing.T) {
	t.Run("when the URL is valid", func(t *testing.T) {
		u, err := url.Parse("memory:///mysilo")
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewProvider(u)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"missing silo name", "memory://"},
			{"empty path", "memory:///"},
			{"non-empty host", "memory://myname"},
			{"query parameters present", "memory://?foo=bar"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				u, err := url.Parse(tc.URL)
				if err != nil {
					t.Fatal(err)
				}
				_, err = NewProvider(u)
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
			})
		}
	})

	t.Run("when two providers use the same silo", func(t *testing.T) {
		t.Run("they share KV state", func(t *testing.T) {
			silo := strings.ReplaceAll(t.Name(), "/", "-")

			u1, err := url.Parse("memory:///" + silo)
			if err != nil {
				t.Fatal(err)
			}
			p1, err := NewProvider(u1)
			if err != nil {
				t.Fatal(err)
			}

			u2, err := url.Parse("memory:///" + silo)
			if err != nil {
				t.Fatal(err)
			}
			p2, err := NewProvider(u2)
			if err != nil {
				t.Fatal(err)
			}

			store1, err := p1.KVStore(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			ks1, err := store1.Open(t.Context(), "test")
			if err != nil {
				t.Fatal(err)
			}
			defer ks1.Close()

			key := []byte("key")
			want := []byte("value")
			if _, err := ks1.Set(t.Context(), key, want, ""); err != nil {
				t.Fatal(err)
			}

			store2, err := p2.KVStore(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			ks2, err := store2.Open(t.Context(), "test")
			if err != nil {
				t.Fatal(err)
			}
			defer ks2.Close()

			got, _, err := ks2.Get(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("expected shared KV state: got %q, want %q", got, want)
			}
		})
	})
}
