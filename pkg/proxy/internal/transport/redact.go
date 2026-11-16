package transport

import (
	"errors"
	"net/url"
)

// RedactHTTPError removes URL credentials and query data while preserving the
// standard error chain used by errors.Is and errors.As.
func RedactHTTPError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	redacted := *urlErr
	redacted.URL = RedactedURLString(urlErr.URL)
	if redacted.URL == "" {
		redacted.URL = "<redacted>"
	}
	redacted.Err = RedactHTTPError(urlErr.Err)
	return &redacted
}

func RedactedURLString(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return RedactedURL(parsed)
}
