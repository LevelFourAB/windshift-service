package events_test

import (
	"time"
	"windshift/service/internal/events"

	. "github.com/onsi/ginkgo/v2"
	//. "github.com/onsi/gomega"
)

var _ = Describe("Limiter", func() {
	It("wait does not block if not at limit", func() {
		limiter := events.NewLimiter(2)

		select {
		case <-limiter.Wait():
		case <-time.After(10 * time.Millisecond):
			Fail("expected channel to be closed")
		}
	})

	It("wait blocks if at limit", func() {
		limiter := events.NewLimiter(2)

		limiter.Add()
		limiter.Add()

		select {
		case <-limiter.Wait():
			Fail("expected channel to be open")
		case <-time.After(10 * time.Millisecond):
		}
	})

	It("wait unblocks when below limit", func() {
		limiter := events.NewLimiter(2)

		limiter.Add()
		limiter.Add()

		ch := limiter.Wait()

		limiter.Remove()

		select {
		case <-ch:
		case <-time.After(10 * time.Millisecond):
			Fail("expected channel to be closed")
		}
	})

	It("wait unblocks when below limit after several calls", func() {
		limiter := events.NewLimiter(2)

		limiter.Add()
		limiter.Add()

		ch := limiter.Wait()

		limiter.Remove()
		limiter.Remove()

		select {
		case <-ch:
		case <-time.After(10 * time.Millisecond):
			Fail("expected channel to be closed")
		}
	})
})
