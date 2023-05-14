package flowcontrol_test

import (
	"sync/atomic"
	"time"
	"windshift/service/internal/events/flowcontrol"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Semaphore", func() {
	It("can be acquired and released", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(1)
		ds.Acquire()
		ds.Release()
	})

	It("can be acquired and released multiple times", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(2)
		ds.Acquire()
		ds.Acquire()
		ds.Release()
		ds.Release()
	})

	It("acquiring more than the semaphore size blocks", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(1)
		ds.Acquire()

		var blocked = atomic.Bool{}
		blocked.Store(true)
		go func() {
			ds.Acquire()
			blocked.Store(false)
		}()

		// Wait for the goroutine to block
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeTrue())

		ds.Release()
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeFalse())

		ds.Release()
	})

	It("can be resized", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(1)
		ds.Acquire()

		var blocked = atomic.Bool{}
		blocked.Store(true)
		go func() {
			ds.Acquire()
			blocked.Store(false)
		}()

		// Wait for the goroutine to block
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeTrue())

		ds.SetLimit(2)
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeFalse())

		ds.Release()
		ds.Release()
	})
})
