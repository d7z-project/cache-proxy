package rpm

import (
	"bytes"
	"compress/gzip"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzRepomd(f *testing.F) {
	f.Add(`<repomd><data type="primary"><checksum type="sha256">abc</checksum><location href="repodata/primary.xml"/></data></repomd>`)
	f.Add(`<repomd><data><location href="../../escape"/>`)
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256<<10 {
			t.Skip()
		}
		items, err := parseRepomdReader(context.Background(), strings.NewReader(input))
		if err == nil {
			require.LessOrEqual(t, len(items), len(input)+1)
		}
	})
}

func FuzzRPMMetadataDecompression(f *testing.F) {
	plain := []byte(`<metadata><package><location href="Packages/demo.rpm"/></package></metadata>`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write(plain)
	_ = writer.Close()
	f.Add(plain, "repodata/primary.xml")
	f.Add(compressed.Bytes(), "repodata/primary.xml.gz")
	f.Fuzz(func(_ *testing.T, data []byte, location string) {
		if len(data) > 1<<20 || len(location) > 256 {
			return
		}
		item := repomdItem{Type: "primary", Location: location, Size: -1, OpenSize: -1}
		_ = inspectOpenMetadataReader(context.Background(), bytes.NewReader(data), int64(len(data)), item)
	})
}
