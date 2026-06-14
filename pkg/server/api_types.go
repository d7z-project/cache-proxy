package server

import "gopkg.d7z.net/cache-proxy/pkg/config"

type instanceDocumentResponse struct {
	Generation uint64              `json:"generation"`
	Spec       config.InstanceSpec `json:"spec"`
}

type exportResponse struct {
	Generation uint64                `json:"generation"`
	Global     *config.GlobalConfig  `json:"global"`
	Instances  []config.InstanceSpec `json:"instances"`
}

type saveInstanceRequest struct {
	Generation uint64               `json:"generation"`
	Spec       *config.InstanceSpec `json:"spec"`
}

type importInstancesRequest struct {
	Generation uint64                `json:"generation"`
	Replace    bool                  `json:"replace"`
	Instances  []config.InstanceSpec `json:"instances"`
}

type runtimeResponse struct {
	Bind          string `json:"bind"`
	Backend       string `json:"backend"`
	AuthEnabled   bool   `json:"authEnabled"`
	MetricsPath   string `json:"metricsPath"`
	GCInterval    string `json:"gcInterval"`
	ConfigVersion int    `json:"configVersion"`
	Generation    uint64 `json:"generation"`
	Instances     int    `json:"instances"`
	Handlers      int    `json:"handlers"`
	Requests      uint64 `json:"requests"`
	Errors        uint64 `json:"errors"`
	Upstreams     uint64 `json:"upstreams"`
}

type instancesCollectionResponse struct {
	Generation uint64                   `json:"generation"`
	Items      []config.InstanceSummary `json:"items"`
}

type deleteInstanceResponse struct {
	Generation uint64 `json:"generation"`
	Deleted    string `json:"deleted"`
}

type importInstancesResponse struct {
	Generation uint64 `json:"generation"`
	Imported   int    `json:"imported"`
}

type resetSystemResponse struct {
	Generation uint64 `json:"generation"`
}
