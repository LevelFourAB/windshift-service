package events_test

import (
	"time"
	"windshift/service/internal/events"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BatchSizer", func() {
	It("returns minimum batch size to start with", func() {
		sizer := events.NewBatchSizer(10, 100)

		Expect(sizer.Get(100)).To(Equal(10))
	})

	It("calling Get before interval is over increases batch size", func() {
		sizer := events.NewBatchSizer(10, 100)

		v1 := sizer.Get(100)
		sizer.Processed()

		v2 := sizer.Get(100)
		Expect(v2).To(BeNumerically(">", v1))
	})

	It("calling Get after interval is over decreases batch size", func() {
		sizer := events.NewBatchSizer(10, 100)

		_ = sizer.Get(100)
		sizer.Processed()
		v2 := sizer.Get(100)
		sizer.Processed()

		// Override the interval to be very small
		sizer.Interval = time.Millisecond
		time.Sleep(2 * time.Millisecond)

		v3 := sizer.Get(100)
		Expect(v3).To(BeNumerically("<", v2))
	})
})
