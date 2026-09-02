package rpm

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepomdRequiresBoundedDataClosure(t *testing.T) {
	for _, input := range []string{"", "<not-repomd/>", "<repomd/>", "<repomd><data type='primary'/></repomd>"} {
		_, err := parseRepomdReader(context.Background(), strings.NewReader(input))
		require.Error(t, err, input)
	}

	entry := "<data type='primary'><checksum type='sha256'>abc</checksum><location href='repodata/primary.xml'/></data>"
	_, err := parseRepomdReader(context.Background(), strings.NewReader("<repomd>"+strings.Repeat(entry, maxRepomdItems+1)+"</repomd>"))
	require.Error(t, err)
}

func TestRPMChecksumSupportsRepomdAlgorithms(t *testing.T) {
	for algorithm, expected := range map[string]string{
		"sha224": hex.EncodeToString(func() []byte { sum := sha256.Sum224([]byte("body")); return sum[:] }()),
		"sha384": hex.EncodeToString(func() []byte { sum := sha512.Sum384([]byte("body")); return sum[:] }()),
	} {
		hash, err := rpmChecksum(algorithm)
		require.NoError(t, err)
		_, err = hash.Write([]byte("body"))
		require.NoError(t, err)
		require.Equal(t, expected, hex.EncodeToString(hash.Sum(nil)))
	}
}

func TestRepomdParserBoundsNestingAndHonorsCancellation(t *testing.T) {
	nested := "<repomd>" + strings.Repeat("<unknown>", maxRepomdDepth) + strings.Repeat("</unknown>", maxRepomdDepth) + "</repomd>"
	_, err := parseRepomdReader(context.Background(), strings.NewReader(nested))
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = parseRepomdReader(ctx, strings.NewReader("<repomd/>"))
	require.ErrorIs(t, err, context.Canceled)
}
