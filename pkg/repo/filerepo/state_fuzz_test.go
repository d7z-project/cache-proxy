package filerepo

import "testing"

func FuzzGenerationStateDecoders(f *testing.F) {
	for _, seed := range []string{
		"version: 2\nroot_id: root\ngeneration: gen\nsnapshot_sha256: sha256:00\ncleanup_index_sha256: sha256:00\n",
		"version: 3\nroot_id: root\nroot_path: ''\ngeneration: gen\nupstream: https://example.test\npublished_at: 2026-01-01T00:00:00Z\nanchor_set_sha256: sha256:00\nanchors: []\nobjects: {}\nartifact_count: 0\ntargets: []\ncleanup_index_sha256: sha256:00\n",
		"version: 2\nroot_id: root\ngeneration: gen\nupstream: https://example.test\ncreated_at: 2026-01-01T00:00:00Z\nphase: metadata_fetch\nentry_cursor: 0\nparse_cursor: 0\n",
		"version: 2\nrevision: 1\nroots: []\n",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		for _, value := range []any{
			&currentReference{},
			&LiveSnapshot{},
			&refreshStagingState{},
			&persistedState{},
		} {
			_ = strictYAML(data, value)
		}
	})
}
