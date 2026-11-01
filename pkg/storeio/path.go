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
	relative := strings.TrimPrefix(decoded, "/")
	if relative == "" {
		return "", nil
	}
	trailingSlash := strings.HasSuffix(relative, "/")
	relative = strings.TrimSuffix(relative, "/")
	segments := strings.Split(relative, "/")
	cleaned := segments[:0]
	for _, segment := range segments {
		switch segment {
		case "":
			return "", errors.New("invalid request path")
		case ".":
			continue
		case "..":
			return "", errors.New("parent path segment is not allowed")
		default:
			cleaned = append(cleaned, segment)
		}
	}
	result := strings.Join(cleaned, "/")
	if trailingSlash && result != "" {
		result += "/"
	}
	return result, nil
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
