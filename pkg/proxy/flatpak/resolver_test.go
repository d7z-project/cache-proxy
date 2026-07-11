package flatpak

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestRouteForPathClassifiesObjects(t *testing.T) {
	policy := &Policy{}
	applyDefaults(policy)
	route, err := routeForPath(policy, "objects/ab/"+strings.Repeat("1", 62)+".commit")
	require.NoError(t, err)
	require.Equal(t, "flatpak/objects/ab/"+strings.Repeat("1", 62)+".commit", route.ObjectPath)
	require.Equal(t, config.PolicyImmutable, route.Policy)
}

func TestRouteForPathRejectsObjectTraversal(t *testing.T) {
	policy := &Policy{}
	applyDefaults(policy)
	_, err := routeForPath(policy, "objects/ab/../"+strings.Repeat("1", 62)+".commit")
	require.Error(t, err)
}

func TestRouteForPathCachesDeltasByDefault(t *testing.T) {
	policy := &Policy{}
	applyDefaults(policy)
	route, err := routeForPath(policy, "deltas/aa/bb/superblock")
	require.NoError(t, err)
	require.Equal(t, "flatpak/deltas/aa/bb/superblock", route.ObjectPath)
	require.Equal(t, config.PolicyImmutable, route.Policy)
}

func TestRouteForPathBypassesDeltasWhenDisabled(t *testing.T) {
	policy := &Policy{}
	applyDefaults(policy)
	disabled := false
	policy.CacheDeltas = &disabled
	route, err := routeForPath(policy, "deltas/aa/bb/superblock")
	require.NoError(t, err)
	require.Equal(t, config.PolicyBypass, route.Policy)
}

func TestRouteForPathUsesDeltaExpireAfter(t *testing.T) {
	policy := &Policy{}
	applyDefaults(policy)
	policy.DeltaExpireAfter = config.Expiration(2 * time.Hour)
	route, err := routeForPath(policy, "deltas/aa/bb/superblock")
	require.NoError(t, err)
	require.Equal(t, config.Expiration(2*time.Hour), route.ExpireAfter)
}

func TestValidateRejectsNeverDeltaExpireAfter(t *testing.T) {
	policy := &Policy{}
	applyDefaults(policy)
	policy.DeltaExpireAfter = config.ExpirationNever
	err := validatePolicy(policy, config.DefaultExpireAfter)
	require.Error(t, err)
	require.ErrorContains(t, err, "delta_expire_after")
}
