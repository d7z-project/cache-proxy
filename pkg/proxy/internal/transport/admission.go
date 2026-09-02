package transport

import proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"

type AdmissionClass = proxyruntime.AdmissionClass
type UpstreamGate = proxyruntime.UpstreamGate

const (
	AdmissionForeground = proxyruntime.AdmissionForeground
	AdmissionRefresh    = proxyruntime.AdmissionRefresh
)
