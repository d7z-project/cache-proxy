package utils

import (
	"context"
	"io"
)

type ContextReadCloser struct {
	ctx context.Context
	rc  io.ReadCloser
}

func NewContextReadCloser(ctx context.Context, rc io.ReadCloser) *ContextReadCloser {
	return &ContextReadCloser{ctx: ctx, rc: rc}
}

func (c *ContextReadCloser) Read(p []byte) (int, error) {
	select {
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	default:
	}
	n, err := c.rc.Read(p)
	if err != nil {
		return n, err
	}
	select {
	case <-c.ctx.Done():
		return n, c.ctx.Err()
	default:
	}
	return n, nil
}

func (c *ContextReadCloser) Close() error {
	return c.rc.Close()
}
