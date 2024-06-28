package v1alpha1

import (
	eventsv1alpha1 "github.com/levelfourab/windshift-server/internal/proto/windshift/events/v1alpha1"
)

// eventTracingHeaders is a wrapper around eventsv1alpha1.Headers that helps
// with injecting W3C Tracing Context headers into the header format that
// events.v1alpha1 expects.
type eventTracingHeaders struct {
	headers *eventsv1alpha1.Headers
}

func (h eventTracingHeaders) Get(key string) string {
	switch key {
	case "traceparent":
		if h.headers.TraceParent == nil {
			return ""
		}
		return *h.headers.TraceParent
	case "tracestate":
		if h.headers.TraceState == nil {
			return ""
		}
		return *h.headers.TraceState
	}
	return ""
}

func (h eventTracingHeaders) Set(key string, value string) {
	switch key {
	case "traceparent":
		h.headers.TraceParent = &value
	case "tracestate":
		h.headers.TraceState = &value
	}
}

func (h eventTracingHeaders) Keys() []string {
	res := make([]string, 0, 2)
	if h.headers.TraceParent != nil {
		res = append(res, "traceparent")
	}
	if h.headers.TraceState != nil {
		res = append(res, "tracestate")
	}
	return res
}
