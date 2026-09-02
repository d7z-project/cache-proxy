package flatpak

import (
	"path"
	"strings"
)

var objectSuffixes = map[string]struct{}{
	".commit":     {},
	".commitmeta": {},
	".dirtree":    {},
	".dirmeta":    {},
	".filez":      {},
}

var verifiedObjectSuffixes = map[string]struct{}{
	".commit":  {},
	".dirtree": {},
	".dirmeta": {},
	".filez":   {},
}

func metadataAnchorPath(cleanPath string) (string, bool) {
	switch cleanPath {
	case "summary", "summary.sig":
		return "summary", true
	case "summary.idx", "summary.idx.sig":
		return "summary.idx", true
	case "config":
		return "config", true
	}
	if !strings.HasPrefix(cleanPath, "summaries/") {
		return "", false
	}
	name := strings.TrimPrefix(cleanPath, "summaries/")
	switch {
	case strings.HasSuffix(name, ".gz"):
		digest := strings.TrimSuffix(name, ".gz")
		return "summary.idx", len(digest) == 64 && isLowerHex(digest)
	case strings.HasSuffix(name, ".idx.sig"):
		digest := strings.TrimSuffix(name, ".idx.sig")
		return "summary.idx", len(digest) == 64 && isLowerHex(digest)
	case strings.HasSuffix(name, ".delta"):
		digests := strings.Split(strings.TrimSuffix(name, ".delta"), "-")
		return "summary.idx", len(digests) == 2 && len(digests[0]) == 64 && len(digests[1]) == 64 && isLowerHex(digests[0]) && isLowerHex(digests[1])
	default:
		return "", false
	}
}

func isDescriptorPath(cleanPath string) bool {
	return strings.HasSuffix(cleanPath, ".flatpakrepo") || strings.HasSuffix(cleanPath, ".flatpakref")
}

func isDeltaPath(cleanPath string) bool {
	return cleanPath == "deltas" || strings.HasPrefix(cleanPath, "deltas/") || isDeltaIndexPath(cleanPath)
}

func isDeltaIndexPath(cleanPath string) bool {
	parts := strings.Split(cleanPath, "/")
	if len(parts) != 3 || parts[0] != "delta-indexes" || len(parts[1]) != 2 || !strings.HasSuffix(parts[2], ".index") {
		return false
	}
	encoded := parts[1] + strings.TrimSuffix(parts[2], ".index")
	if len(encoded) != 43 {
		return false
	}
	for _, char := range encoded {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '+' {
			return false
		}
	}
	return true
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
	if _, ok := verifiedObjectSuffixes[ext]; !ok {
		return "", "", false
	}
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
