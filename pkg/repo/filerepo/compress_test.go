package filerepo

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type trackingReadSeekCloser struct {
	*bytes.Reader
	closed bool
}

func (r *trackingReadSeekCloser) Close() error {
	r.closed = true
	return nil
}

func TestOpenCompressedClosesUnderlyingReader(t *testing.T) {
	plain := &trackingReadSeekCloser{Reader: bytes.NewReader([]byte("plain"))}
	reader, err := OpenCompressed(plain, "plain.txt")
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.True(t, plain.closed)

	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, err = zw.Write([]byte("gzip"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	compressed := &trackingReadSeekCloser{Reader: bytes.NewReader(gz.Bytes())}
	reader, err = OpenCompressed(compressed, "index.gz")
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.True(t, compressed.closed)
}
