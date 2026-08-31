package flatpak

import "testing"

func FuzzMetadataStateDecoders(f *testing.F) {
	f.Add([]byte("version: 1\ngeneration: gen\nsnapshot_sha256: sha256:00\n"))
	f.Add([]byte("version: 1\ngeneration: gen\nupstream: https://example.test\npublished_at: 2026-01-01T00:00:00Z\nanchor_set_sha256: sha256:00\nobjects: {}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		_ = decodeStrictYAML(data, &metadataCurrentReference{})
		_ = decodeStrictYAML(data, &metadataManifest{})
	})
}
