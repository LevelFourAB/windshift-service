package api

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// vtMessage is the VT specific interface for protobuf messages.
type vtMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

// codec is a custom implementation of a gRPC codec. It supports both
// protobuf and VTprotobuf messages.
type codec struct {
}

func (c codec) Marshal(v interface{}) ([]byte, error) {
	if vt, ok := v.(vtMessage); ok {
		return vt.MarshalVT()
	}

	if pb, ok := v.(proto.Message); ok {
		return proto.Marshal(pb)
	}

	return nil, fmt.Errorf("unsupported type: %T", v)
}

func (c codec) Unmarshal(data []byte, v interface{}) error {
	if vt, ok := v.(vtMessage); ok {
		return vt.UnmarshalVT(data)
	}

	if pb, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, pb)
	}

	return fmt.Errorf("unsupported type: %T", v)
}

func (c codec) Name() string {
	return "proto"
}
