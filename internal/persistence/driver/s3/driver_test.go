package s3_test

import (
	"net/url"
	"testing"

	. "github.com/dogmatiq/runkit/internal/persistence/driver/s3"
)

func TestNewProvider(t *testing.T) {
	t.Run("when the URL is valid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"bucket name in path", "s3:///my-bucket"},
			{"region parameter", "s3:///my-bucket?region=us-east-1"},
			{"role_arn parameter", "s3:///my-bucket?role_arn=arn:aws:iam::123456789012:role/MyRole"},
			{"custom endpoint", "s3://endpoint.host/my-bucket"},
			{"custom endpoint with insecure", "s3://endpoint.host/my-bucket?insecure=true"},
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
			{"insecure without host", "s3:///my-bucket?insecure=true"},
			{"unknown parameter", "s3:///my-bucket?unknown=x"},
			{"missing bucket name", "s3://"},
			{"empty path", "s3:///"},
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
