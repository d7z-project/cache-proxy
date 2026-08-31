package filerepo

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestPackageSidecarUsesStableImmutableRoute(t *testing.T) {
	handler := &IndexedHandler{
		mode: LayoutPacman,
		rootSnapshots: map[string]*LiveSnapshot{
			"core": {RootID: "core", Generation: "current", RootPath: "core/os/x86_64", Published: time.Now()},
		},
	}
	policy := &Policy{}
	ApplyPolicyDefaults(policy)
	resolver := &generationResolver{handler: handler, policy: policy}
	req, err := http.NewRequest(http.MethodGet, "http://cache.example/core/os/x86_64/linux-1-1-x86_64.pkg.tar.zst.sig", nil)
	require.NoError(t, err)

	route, err := resolver.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, config.PolicyImmutable, route.Policy)
	require.Equal(t, handler.contentPath(ResourceSidecar, "core/os/x86_64/linux-1-1-x86_64.pkg.tar.zst.sig"), route.ObjectPath)

	handler.rootSnapshots["core"].Generation = "next"
	next, err := resolver.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, route.ObjectPath, next.ObjectPath)
}

func TestValidatePackagePolicyReportsFieldsInDeclarationOrder(t *testing.T) {
	policy := &Policy{
		ArtifactPolicy:  "invalid-artifact",
		AuxiliaryPolicy: "invalid-auxiliary",
	}

	err := ValidatePackagePolicy(config.ModeDEB, policy)
	require.EqualError(t, err, `artifact: invalid deb policy "invalid-artifact"`)
}
