package storeio

import (
	"errors"
	"net/url"
	"strings"
)

func CleanURLPath(target *url.URL) (string, error) {
	decoded, err := DecodeCanonicalURLPath(target)
	if err != nil {
		return "", err
	}
	return CleanRelative(strings.TrimPrefix(decoded, "/"))
}

// DecodeCanonicalURLPath validates the escaped request path before returning
// its absolute decoded form. Protocols that allow a trailing slash can apply
// their own segment rules to the result.
func DecodeCanonicalURLPath(target *url.URL) (string, error) {
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
	if escaped != canonicalEscapedPath(decoded) {
		return "", errors.New("request path is not canonical")
	}
	return decoded, nil
}

func canonicalEscapedPath(value string) string {
	const hex = "0123456789ABCDEF"
	var result strings.Builder
	result.Grow(len(value))
	for index := 0; index < len(value); index++ {
		current := value[index]
		if current == '/' || current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' || current >= '0' && current <= '9' || strings.ContainsRune("-._~!$&'()*+,;=:@", rune(current)) {
			result.WriteByte(current)
			continue
		}
		result.WriteByte('%')
		result.WriteByte(hex[current>>4])
		result.WriteByte(hex[current&0x0f])
	}
	return result.String()
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
