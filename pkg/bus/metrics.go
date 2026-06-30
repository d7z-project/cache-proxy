package bus

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	published   *prometheus.CounterVec
	delivered   *prometheus.CounterVec
	dropped     *prometheus.CounterVec
	subscribers *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		return nil
	}
	m := &metrics{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_bus_events_published_total",
			Help: "Total bus events published by type.",
		}, []string{"event_type"}),
		delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_bus_events_delivered_total",
			Help: "Total bus event deliveries to subscribers by type.",
		}, []string{"event_type"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_proxy_bus_events_dropped_total",
			Help: "Total bus events dropped by type and reason.",
		}, []string{"event_type", "reason"}),
		subscribers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_proxy_bus_subscribers",
			Help: "Current bus subscriber count by event type.",
		}, []string{"event_type"}),
	}
	reg.MustRegister(m.published, m.delivered, m.dropped, m.subscribers)
	return m
}
