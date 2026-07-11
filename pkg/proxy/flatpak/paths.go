package flatpak

import (
	"path"
	"strings"
)

var objectSuffixes = map[string]struct{}{
	".commit":  {},
	".dirtree": {},
	".dirmeta": {},
	".filez":   {},
}

func isMetadataPath(cleanPath string) bool {
	return cleanPath == "summary" || cleanPath == "summary.sig" || cleanPath == "config"
}

func isDescriptorPath(cleanPath string) bool {
	return strings.HasSuffix(cleanPath, ".flatpakrepo") || strings.HasSuffix(cleanPath, ".flatpakref")
}

func isDeltaPath(cleanPath string) bool {
	return cleanPath == "deltas" || strings.HasPrefix(cleanPath, "deltas/")
}

func isObjectPath(cleanPath string) bool {
	parts := strings.Split(cleanPath, "/")
	if len(parts) != 3 || parts[0] != "objects" {
		return false
	}
	if len(parts[1]) != 2 || !isLowerHex(parts[1]) {
		return false
	}
	base := path.Base(parts[2])
	ext := path.Ext(base)
	if _, ok := objectSuffixes[ext]; !ok {
		return false
	}
	digest := strings.TrimSuffix(base, ext)
	return len(digest) == 62 && isLowerHex(digest)
}

func objectDigestFromPath(cleanPath string) (string, string, bool) {
	if !isObjectPath(cleanPath) {
		return "", "", false
	}
	parts := strings.Split(cleanPath, "/")
	ext := path.Ext(parts[2])
	return parts[1] + strings.TrimSuffix(parts[2], ext), ext, true
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
