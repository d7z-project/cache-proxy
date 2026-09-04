package flatpak

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyIndexedSummary(t *testing.T) {
	body := []byte("indexed summary")
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.NoError(t, verifyIndexedSummary(bytes.NewReader(compressed.Bytes()), digest, int64(len(body))))
	require.Error(t, verifyIndexedSummary(bytes.NewReader(compressed.Bytes()), digest, int64(len(body)-1)))
	require.Error(t, verifyIndexedSummary(bytes.NewReader(compressed.Bytes()), fmt.Sprintf("%064x", 0), int64(len(body))))
	require.Error(t, verifyIndexedSummary(bytes.NewReader([]byte("not gzip")), digest, int64(len(body))))
}
