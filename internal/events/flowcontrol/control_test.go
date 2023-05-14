package flowcontrol_test

import (
	"time"
	"windshift/service/internal/events/flowcontrol"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FlowControl", func() {
	It("returns minimum batch size to start with", func() {
		fc := flowcontrol.NewFlowControl(1*time.Second, 10, 100)

		Expect(fc.GetBatchSize()).To(Equal(10))
	})

	It("calling GetBatchSize before interval is over increases batch size", func() {
		fc := flowcontrol.NewFlowControl(1*time.Second, 10, 100)

		v1 := fc.GetBatchSize()
		for i := 0; i < v1; i++ {
			fc.SentEvent()
		}

		v2 := fc.GetBatchSize()
		Expect(v2).To(BeNumerically(">", v1))
	})

	It("calling GetBatchSize after interval is over decreases batch size", func() {
		fc := flowcontrol.NewFlowControl(100*time.Millisecond, 10, 100)

		// Request a batch size to increase from minimum
		v1 := fc.GetBatchSize()
		for i := 0; i < v1; i++ {
			fc.SentEvent()
		}

		// Request another batch to increase further or we will return the
		// same minimum batch size.
		v2 := fc.GetBatchSize()
		for i := 0; i < v2; i++ {
			fc.SentEvent()
		}

		time.Sleep(100 * time.Millisecond)

		v3 := fc.GetBatchSize()
		Expect(v3).To(BeNumerically("<", v2))
	})

	It("calling GetBatchSize after having processed less events than requested returns same batch size", func() {
		fc := flowcontrol.NewFlowControl(1*time.Second, 10, 100)

		v1 := fc.GetBatchSize()
		for i := 0; i < v1-1; i++ {
			fc.SentEvent()
		}

		v2 := fc.GetBatchSize()
		Expect(v2).To(Equal(v1))
	})
})
