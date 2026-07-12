package health

import (
	"errors"
	"time"
)

type ResourceState int

const (
	RPending ResourceState = iota
	RActive
	RSuspect
	RBlocked
	RRemoved
)

func (s ResourceState) String() string {
	switch s {
	case RPending:
		return "pending"
	case RActive:
		return "active"
	case RSuspect:
		return "suspect"
	case RBlocked:
		return "blocked"
	case RRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

var (
	ErrResourceNotFound       = errors.New("resource upstream not found")
	ErrResourceForbidden      = errors.New("resource upstream forbidden")
	ErrResourceTransient      = errors.New("resource upstream transient failure")
	ErrRefreshAlreadyRunning  = errors.New("resource refresh already running")
	ErrRefreshBlockedUntil    = errors.New("resource refresh blocked until next retry window")
	ErrRefreshResourceRemoved = errors.New("resource refresh rejected because resource was removed")
)

type ProbeTarget struct {
	Path string `yaml:"path"`
}

type ResourceHealth struct {
	Path       string
	State      ResourceState
	Generation uint64
	Refreshing bool

	ConsecutiveNotFound  int
	ConsecutiveInvalid   int
	ConsecutiveTransient int

	DiscoveredAt    time.Time
	LastRefreshAt   time.Time
	LastSuccessAt   time.Time
	NextRefreshAt   time.Time
	FirstNotFoundAt time.Time
	LastError       string

	LastTargets  []ProbeTarget
	UpstreamURLs []string
}

func (rh *ResourceHealth) snapshot() ResourceHealth {
	copied := *rh
	copied.LastTargets = append([]ProbeTarget(nil), rh.LastTargets...)
	copied.UpstreamURLs = append([]string(nil), rh.UpstreamURLs...)
	return copied
}

type ResourceSnapshot struct {
	Path                 string        `yaml:"path"`
	State                string        `yaml:"state"`
	Refreshing           bool          `yaml:"refreshing,omitempty"`
	LastRefreshAt        time.Time     `yaml:"last_refresh_at"`
	LastSuccessAt        time.Time     `yaml:"last_success_at"`
	NextRefreshAt        time.Time     `yaml:"next_refresh_at"`
	FirstNotFoundAt      time.Time     `yaml:"first_not_found_at"`
	ConsecutiveNotFound  int           `yaml:"consecutive_not_found"`
	ConsecutiveInvalid   int           `yaml:"consecutive_invalid"`
	ConsecutiveTransient int           `yaml:"consecutive_transient"`
	LastError            string        `yaml:"last_error,omitempty"`
	LastTargets          []ProbeTarget `yaml:"last_targets,omitempty"`
	UpstreamURLs         []string      `yaml:"upstream_urls,omitempty"`
}

func (rh *ResourceHealth) Snapshot() ResourceSnapshot {
	return ResourceSnapshot{
		Path:                 rh.Path,
		State:                rh.State.String(),
		Refreshing:           rh.Refreshing,
		LastRefreshAt:        rh.LastRefreshAt,
		LastSuccessAt:        rh.LastSuccessAt,
		NextRefreshAt:        rh.NextRefreshAt,
		FirstNotFoundAt:      rh.FirstNotFoundAt,
		ConsecutiveNotFound:  rh.ConsecutiveNotFound,
		ConsecutiveInvalid:   rh.ConsecutiveInvalid,
		ConsecutiveTransient: rh.ConsecutiveTransient,
		LastError:            rh.LastError,
		LastTargets:          append([]ProbeTarget(nil), rh.LastTargets...),
		UpstreamURLs:         append([]string(nil), rh.UpstreamURLs...),
	}
}

func ResourceFromSnapshot(snapshot ResourceSnapshot) *ResourceHealth {
	rh := &ResourceHealth{
		Path:                 snapshot.Path,
		LastRefreshAt:        snapshot.LastRefreshAt,
		LastSuccessAt:        snapshot.LastSuccessAt,
		NextRefreshAt:        snapshot.NextRefreshAt,
		FirstNotFoundAt:      snapshot.FirstNotFoundAt,
		ConsecutiveNotFound:  snapshot.ConsecutiveNotFound,
		ConsecutiveInvalid:   snapshot.ConsecutiveInvalid,
		ConsecutiveTransient: snapshot.ConsecutiveTransient,
		LastError:            snapshot.LastError,
		LastTargets:          append([]ProbeTarget(nil), snapshot.LastTargets...),
		UpstreamURLs:         append([]string(nil), snapshot.UpstreamURLs...),
	}
	switch snapshot.State {
	case "active":
		rh.State = RActive
	case "suspect":
		rh.State = RSuspect
	case "blocked":
		rh.State = RBlocked
	case "removed":
		rh.State = RRemoved
	default:
		rh.State = RPending
	}
	return rh
}
