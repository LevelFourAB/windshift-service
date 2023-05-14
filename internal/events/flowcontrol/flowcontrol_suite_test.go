package flowcontrol_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFlowcontrol(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Flowcontrol Suite")
}
