package health

import "time"

type Config struct {
	Enabled bool `yaml:"enabled"`

	ProbeInterval    time.Duration `yaml:"probe_interval"`
	ProbeTimeout     time.Duration `yaml:"probe_timeout"`
	ProbePath        string        `yaml:"probe_path,omitempty"`
	MaxDynamicPaths  int           `yaml:"max_dynamic_paths"`

	FailureThreshold int           `yaml:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold"`

	DegradeLatency time.Duration `yaml:"degrade_latency"`
	MinWeight      float64       `yaml:"min_weight"`
	EwmaAlpha      float64       `yaml:"ewma_alpha"`

	RemovalThreshold int           `yaml:"removal_threshold"`
	BlockInterval    time.Duration `yaml:"block_interval"`
	MinNotFoundAge   time.Duration `yaml:"min_not_found_age"`
}

func DefaultConfig() Config {
	return Config{
		Enabled: true,

		ProbeInterval:   30 * time.Second,
		ProbeTimeout:    5 * time.Second,
		MaxDynamicPaths: 8,

		FailureThreshold: 3,
		SuccessThreshold: 2,

		DegradeLatency: 2 * time.Second,
		MinWeight:      0.1,
		EwmaAlpha:      0.2,

		RemovalThreshold: 3,
		BlockInterval:    time.Hour,
		MinNotFoundAge:   10 * time.Minute,
	}
}
