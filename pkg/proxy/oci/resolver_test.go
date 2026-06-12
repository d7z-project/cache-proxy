package oci

import (
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
)

func TestConvertOCILookupPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "repo only", input: "library/alpine", want: "v2/library/alpine/tags/list"},
		{name: "repo with tag", input: "library/alpine:latest", want: "v2/library/alpine/manifests/latest"},
		{name: "repo with digest", input: "library/alpine@sha256:abc123", want: "v2/library/alpine/blobs/sha256:abc123"},
		{name: "simple repo", input: "nginx", want: "v2/nginx/tags/list"},
		{name: "simple repo with tag", input: "nginx:1.25", want: "v2/nginx/manifests/1.25"},
		{name: "nested repo", input: "org/project/image:v1", want: "v2/org/project/image/manifests/v1"},
		{name: "empty input", input: "", wantErr: true},
		{name: "invalid digest", input: "alpine@invalid", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertOCILookupPath(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveLookupRoutesCorrectly(t *testing.T) {
	cfg := &config.OCIConfig{
		DefaultPolicy: config.PolicyRevalidate,
		Rules: []config.OCICacheRule{
			{Match: "library/*", Policy: config.PolicyImmutable},
			{Match: "org/**", Policy: config.PolicyBypass},
		},
	}
	r := New(cfg)

	tests := []struct {
		name       string
		input      string
		wantPolicy string
		wantObject string
	}{
		{name: "repo tags", input: "library/alpine", wantPolicy: "immutable", wantObject: "oci/tags/library/alpine/list"},
		{name: "repo manifest", input: "library/alpine:latest", wantPolicy: "immutable", wantObject: "oci/manifests/library/alpine/" + proxy.HashKey("latest")},
		{name: "repo blob", input: "library/alpine@sha256:abc", wantPolicy: "immutable", wantObject: "oci/blobs/sha256/abc"},
		{name: "org repo", input: "org/project/app:v1", wantPolicy: "bypass"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route, err := r.ResolveLookup(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.wantPolicy, route.Policy)
			if tt.wantObject != "" {
				require.Equal(t, tt.wantObject, route.ObjectPath)
			}
		})
	}
}
