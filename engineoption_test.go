package runkit_test

import (
	"testing"

	"github.com/dogmatiq/runkit"
)

func TestWithMessageQueueConcurrency(t *testing.T) {
	t.Run("it panics if the concurrency is zero", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithMessageQueueConcurrency(0)
	})
}

func TestWithMessageQueueContentionDelay(t *testing.T) {
	t.Run("it panics if the delay is zero", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithMessageQueueContentionDelay(0)
	})
}
