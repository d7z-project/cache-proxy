package utils

import (
	"net/http"
)

type StatsRecorder interface {
	RecordUpstream(instance, mode, method string, status int)
}

type StatsTransport struct {
	Base     http.RoundTripper
	Recorder StatsRecorder
	Instance string
	Mode     string
}

func NewStatsTransport(base http.RoundTripper) *StatsTransport {
	return &StatsTransport{Base: base}
}

func (t *StatsTransport) Bind(recorder StatsRecorder, instance, mode string) {
	t.Recorder = recorder
	t.Instance = instance
	t.Mode = mode
}

func (t *StatsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if t.Recorder != nil {
		if err != nil {
			t.Recorder.RecordUpstream(t.Instance, t.Mode, req.Method, 0)
		} else {
			t.Recorder.RecordUpstream(t.Instance, t.Mode, req.Method, resp.StatusCode)
		}
	}
	return resp, err
}
