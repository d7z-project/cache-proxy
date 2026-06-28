package filerepo

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

func OpenCompressed(reader io.ReadSeeker, name string) (io.ReadCloser, error) {
	header := make([]byte, 6)
	n, _ := reader.Read(header)
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	header = header[:n]
	switch {
	case strings.HasSuffix(name, ".gz"), looksLikeGzip(header):
		return gzip.NewReader(reader)
	case strings.HasSuffix(name, ".xz"), looksLikeXZ(header):
		decoded, err := xz.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(decoded), nil
	case strings.HasSuffix(name, ".zst"), strings.HasSuffix(name, ".zstd"), looksLikeZstd(header):
		decoded, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return decoded.IOReadCloser(), nil
	default:
		return io.NopCloser(reader), nil
	}
}

func looksLikeGzip(body []byte) bool {
	return len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b
}

func looksLikeXZ(body []byte) bool {
	return len(body) >= 6 && bytes.Equal(body[:6], []byte{0xfd, '7', 'z', 'X', 'Z', 0x00})
}

func looksLikeZstd(body []byte) bool {
	return len(body) >= 4 && bytes.Equal(body[:4], []byte{0x28, 0xb5, 0x2f, 0xfd})
}
