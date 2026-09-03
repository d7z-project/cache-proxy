package storeio

import (
	"errors"
	"net/url"
	"strings"
)

func CleanURLPath(target *url.URL) (string, error) {
	decoded, err := DecodeURLPath(target)
	if err != nil {
		return "", err
	}
	cleaned := strings.TrimPrefix(decoded, "/")
	if cleaned == "" {
		return "", nil
	}
	trailingSlash := strings.HasSuffix(cleaned, "/")
	cleaned = strings.TrimSuffix(cleaned, "/")
	cleaned, err = CleanRelative(cleaned)
	if err != nil {
		return "", err
	}
	if trailingSlash {
		cleaned += "/"
	}
	return cleaned, nil
}

// DecodeURLPath validates the escaped request path before returning its
// absolute decoded form. Equivalent percent encodings have the same result.
func DecodeURLPath(target *url.URL) (string, error) {
	if target == nil {
		return "", errors.New("invalid request URL")
	}
	escaped := target.EscapedPath()
	lowerEscaped := strings.ToLower(escaped)
	if strings.Contains(lowerEscaped, "%2f") || strings.Contains(lowerEscaped, "%5c") || strings.Contains(lowerEscaped, "%00") {
		return "", errors.New("invalid escaped path")
	}
	decoded, err := url.PathUnescape(escaped)
	if err != nil {
		return "", errors.New("invalid escaped path")
	}
	if !strings.HasPrefix(decoded, "/") {
		return "", errors.New("request path is not absolute")
	}
	if strings.Contains(decoded, "\\") || strings.ContainsRune(decoded, '\x00') {
		return "", errors.New("invalid decoded path")
	}
	return decoded, nil
}

func CleanRelative(value string) (string, error) {
	if strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') {
		return "", errors.New("invalid relative path")
	}
	cleaned := value
	if cleaned == "" || strings.HasPrefix(cleaned, "/") {
		return "", errors.New("invalid relative path")
	}
	for _, segment := range strings.Split(cleaned, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errors.New("invalid relative path")
		}
	}
	return cleaned, nil
}
