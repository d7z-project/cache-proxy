package flatpak

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func FuzzOSTreePathClassification(f *testing.F) {
	for _, seed := range []string{"summary", "summary.idx", "summaries/156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e.gz", "objects/ab/cdef.file", "objects/00/00000000000000000000000000000000000000000000000000000000000000.commitmeta", "deltas/ab/cd/superblock", "delta-indexes/_1/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuqg.index", "config", "refs/heads/stable"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 4096 {
			t.Skip()
		}
		_, _ = metadataAnchorPath(path)
		_ = isDescriptorPath(path)
		_ = isDeltaPath(path)
		_ = isDeltaIndexPath(path)
		_ = isObjectPath(path)
		_, _, _ = objectDigestFromPath(path)
	})
}

func FuzzSummaryIndex(f *testing.F) {
	seed, err := base64.StdEncoding.DecodeString("eDg2XzY0AAAVbP0Wwl8G7AU97WocH1TpOfNjZz2j9N7vypLh13MGXiAgBysAAAAAb3N0cmVlLnN1bW1hcnkubW9kZQAAAAAAYXJjaGl2ZS16MgAAcxQAAG9zdHJlZS5zdW1tYXJ5LnRvbWJzdG9uZS1jb21taXRzAAAAAAAAAAAAAGIhAAAAAG9zdHJlZS5zdW1tYXJ5LmluZGV4ZWQtZGVsdGFzAAAAAQBiHgAAAABvc3RyZWUuc3VtbWFyeS5sYXN0LW1vZGlmaWVkAAAAAAAAAABqmDRAAHQdAAAAAAB4YS5jYWNoZS12ZXJzaW9uAAAAAAAAAAACAAAAAHURJlR8q88sAA==")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		_, _ = readSummaryIndex(bytes.NewReader(data), int64(len(data)))
	})
}

func FuzzVerifyOSTreeFileObject(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 8))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		_ = verifyOSTreeFileObject(bytes.NewReader(data), strings.Repeat("0", 64))
	})
}
