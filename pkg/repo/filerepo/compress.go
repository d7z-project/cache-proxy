package filerepo

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

type readSeekCloser struct {
	io.Reader
	close func() error
}

func (r *readSeekCloser) Close() error {
	if r.close == nil {
		return nil
	}
	return r.close()
}

func OpenCompressed(reader io.ReadSeekCloser, name string) (io.ReadCloser, error) {
	header := make([]byte, 6)
	n, _ := reader.Read(header)
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		_ = reader.Close()
		return nil, err
	}
	header = header[:n]
	switch {
	case strings.HasSuffix(name, ".gz"), looksLikeGzip(header):
		decoded, err := gzip.NewReader(reader)
		if err != nil {
			_ = reader.Close()
			return nil, err
		}
		return &readSeekCloser{
			Reader: decoded,
			close: func() error {
				err := decoded.Close()
				if closeErr := reader.Close(); err == nil {
					err = closeErr
				}
				return err
			},
		}, nil
	case strings.HasSuffix(name, ".xz"), looksLikeXZ(header):
		decoded, err := xz.NewReader(reader)
		if err != nil {
			_ = reader.Close()
			return nil, err
		}
		return &readSeekCloser{Reader: decoded, close: reader.Close}, nil
	case strings.HasSuffix(name, ".zst"), strings.HasSuffix(name, ".zstd"), looksLikeZstd(header):
		decoded, err := zstd.NewReader(reader)
		if err != nil {
			_ = reader.Close()
			return nil, err
		}
		body := decoded.IOReadCloser()
		return &readSeekCloser{
			Reader: body,
			close: func() error {
				err := body.Close()
				if closeErr := reader.Close(); err == nil {
					err = closeErr
				}
				return err
			},
		}, nil
	default:
		return reader, nil
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
