package dynamodb_test

import (
	"net/url"
	"testing"

	. "github.com/dogmatiq/runkit/internal/persistence/driver/dynamodb"
)

func TestNewProvider(t *testing.T) {
	t.Run("when the URL is valid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"table prefix in path", "dynamodb:///myapp"},
			{"region parameter", "dynamodb:///myapp?region=us-east-1"},
			{"role_arn parameter", "dynamodb:///myapp?role_arn=arn:aws:iam::123456789012:role/MyRole"},
			{"custom endpoint", "dynamodb://localhost:8000/myapp"},
			{"custom endpoint with insecure", "dynamodb://localhost:8000/myapp?insecure=true"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				u, err := url.Parse(tc.URL)
				if err != nil {
					t.Fatal(err)
				}
				_, err = NewProvider(u)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"unknown parameter", "dynamodb:///myapp?unknown=x"},
			{"insecure without host", "dynamodb:///myapp?insecure=true"},
			{"missing table prefix", "dynamodb://"},
			{"empty path", "dynamodb:///"},
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
}
