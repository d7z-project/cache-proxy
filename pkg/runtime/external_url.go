package runtime

import (
	"context"
	"net/http"
	"strings"
)

type externalBaseKey struct{}

func WithExternalBaseURL(request *http.Request, value string) *http.Request {
	if request == nil {
		return nil
	}
	return request.WithContext(context.WithValue(request.Context(), externalBaseKey{}, strings.TrimRight(value, "/")))
}

func ExternalBaseURL(request *http.Request) string {
	if request == nil {
		return ""
	}
	value, _ := request.Context().Value(externalBaseKey{}).(string)
	return value
}
