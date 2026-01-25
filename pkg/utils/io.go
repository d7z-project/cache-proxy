package utils

import "io"

type ReadCloserWrapper struct {
	r io.Reader
	c io.Closer
}

func NewReadCloserWrapper(r io.Reader, c io.Closer) *ReadCloserWrapper {
	return &ReadCloserWrapper{
		r: r,
		c: c,
	}
}

func (r *ReadCloserWrapper) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *ReadCloserWrapper) Close() error {
	return r.c.Close()
}
