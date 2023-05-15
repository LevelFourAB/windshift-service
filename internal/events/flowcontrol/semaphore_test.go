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
		Expect(ds.Available()).To(Equal(1))

		release := ds.Acquire()
		Expect(ds.Available()).To(Equal(0))
		release()
	})

	It("can be acquired and released multiple times", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(2)
		release1 := ds.Acquire()
		Expect(ds.Available()).To(Equal(1))

		release2 := ds.Acquire()
		Expect(ds.Available()).To(Equal(0))

		release1()
		Expect(ds.Available()).To(Equal(1))

		release2()
		Expect(ds.Available()).To(Equal(2))
	})

	It("acquiring more than the semaphore size blocks", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(1)
		release1 := ds.Acquire()

		var blocked = atomic.Bool{}
		blocked.Store(true)
		go func() {
			release2 := ds.Acquire()
			blocked.Store(false)
			release2()
		}()

		// Wait for the goroutine to block
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeTrue())

		release1()
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeFalse())
	})

	It("can be resized", SpecTimeout(100*time.Millisecond), func(_ SpecContext) {
		ds := flowcontrol.NewDynamicSemaphore(1)
		release1 := ds.Acquire()

		var blocked = atomic.Bool{}
		blocked.Store(true)
		go func() {
			release2 := ds.Acquire()
			blocked.Store(false)
			release2()
		}()

		// Wait for the goroutine to block
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeTrue())

		ds.SetLimit(2)
		time.Sleep(10 * time.Millisecond)
		Expect(blocked.Load()).To(BeFalse())

		release1()
	})
})
