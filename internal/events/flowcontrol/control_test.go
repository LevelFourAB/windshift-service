package flowcontrol_test

import (
	"windshift/service/internal/events/flowcontrol"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FlowControl", func() {
	It("returns minimum batch size to start with", func() {
		fc := flowcontrol.NewFlowControl(10, 100)

		Expect(fc.GetBatchSize()).To(BeNumerically("==", 10))
	})

	It("calling GetBatchSize after processing all events increases batch size", func() {
		fc := flowcontrol.NewFlowControl(10, 100)

		v1 := fc.GetBatchSize()
		for i := 0; i < v1; i++ {
			fc.AcquireSendLock()()
		}

		v2 := fc.GetBatchSize()
		Expect(v2).To(BeNumerically(">", v1))
	})

	It("calling GetBatchSize after not processing less than 90% of events decreases batch size", func() {
		fc := flowcontrol.NewFlowControl(10, 100)

		// Request a batch size to increase from minimum
		v1 := fc.GetBatchSize()
		for i := 0; i < v1; i++ {
			fc.AcquireSendLock()()
		}

		// Request another batch to increase further or we will return the
		// same minimum batch size.
		v2 := fc.GetBatchSize()
		cutoff := int(float32(v2)*0.9) - 1
		for i := 0; i < cutoff; i++ {
			fc.AcquireSendLock()()
		}

		v3 := fc.GetBatchSize()
		Expect(v3).To(BeNumerically("<", v2))
	})
})
