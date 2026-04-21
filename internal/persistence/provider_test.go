package persistence_test

import (
	"testing"

	. "github.com/dogmatiq/runkit/internal/persistence"
)

func TestNewProvider(t *testing.T) {
	t.Run("when the URL is valid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"memory", "memory:///mysilo"},
			{"postgres", "postgres://user:pass@host/db"},
			{"postgresql", "postgresql://user:pass@host/db"},
			{"dynamodb", "dynamodb:///myapp"},
			{"s3", "s3:///my-bucket"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := NewProvider(tc.URL)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"unknown scheme", "unknown://foo"},
			{"missing scheme", "no-double-slash"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := NewProvider(tc.URL)
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
			})
		}
	})
}
