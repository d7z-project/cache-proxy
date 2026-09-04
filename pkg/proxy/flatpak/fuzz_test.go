package flatpak

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

func FuzzOSTreePathClassification(f *testing.F) {
	for _, seed := range []string{"summary", "summary.idx", "summaries/156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e.gz", "summaries/0000000000000000000000000000000000000000000000000000000000000000-156cfd16c25f06ec053ded6a1c1f54e939f363673da3f4deefca92e1d773065e.delta", "objects/ab/cdef.file", "objects/00/00000000000000000000000000000000000000000000000000000000000000.commitmeta", "deltas/ab/cd/superblock", "delta-indexes/_1/CNHDS81donGnhBJHDT9ww12oUNEP9E2v1eWqzmuqg.index", "config", "refs/heads/stable"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 4096 {
			t.Skip()
		}
		_, metadata := metadataAnchorPath(path)
		_, _ = indexedSummaryDigestFromPath(path)
		_ = isDescriptorPath(path)
		delta := isDeltaPath(path)
		_ = isDeltaIndexPath(path)
		_ = isObjectPath(path)
		_, _, _ = objectDigestFromPath(path)
		if isIndexedSummaryDeltaPath(path) && (metadata || !delta) {
			t.Fatal("indexed summary delta classifications overlap or are incomplete")
		}
	})
}

func FuzzVerifyIndexedSummary(f *testing.F) {
	body := []byte("flatpak indexed summary")
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		f.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		f.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	f.Add(compressed.Bytes(), digest)
	f.Add([]byte{}, strings.Repeat("0", 64))
	f.Fuzz(func(t *testing.T, data []byte, digest string) {
		if len(data) > 1<<20 || len(digest) > 128 {
			t.Skip()
		}
		_ = verifyIndexedSummary(bytes.NewReader(data), digest, 1<<20)
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
		_, _ = parseSummaryIndexDigest(bytes.NewReader(data), int64(len(data)))
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
