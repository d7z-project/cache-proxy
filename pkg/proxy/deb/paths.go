package deb

import (
	"path"
	"sort"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

var debPackageSidecarSuffixes = []string{".gpg", ".sig", ".asc", ".sha256", ".sha512", ".md5sum"}

type inspector struct{}

func (inspector) FinalizeRoot(root filerepo.RepositoryRoot) filerepo.RepositoryRoot {
	repoPath := root.Path
	if repoPath == "" {
		repoPath = "/"
	}
	attrs := []filerepo.RepositoryAttribute{{LabelKey: "repo_path", Value: repoPath}}
	switch root.Layout {
	case filerepo.LayoutDebDistribution:
		if root.Suite != "" {
			attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "suite", Value: root.Suite})
		}
		if len(root.Components) > 0 {
			attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "components", Value: strings.Join(root.Components, ", ")})
		}
		if len(root.Architectures) > 0 {
			attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "architectures", Value: strings.Join(root.Architectures, ", ")})
		}
		if root.Source {
			attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "source_packages", Value: "yes"})
		}
	case filerepo.LayoutDebFlat:
		if root.Source {
			attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "source_packages", Value: "yes"})
		}
	}
	root.Attributes = attrs
	return root
}

func (inspector) InspectPath(cleanPath string) filerepo.DiscoveryResult {
	if result, ok := analyzeDistributionMetadataPath(cleanPath); ok {
		return result
	}
	if result, ok := analyzeFlatMetadataPath(cleanPath); ok {
		return result
	}
	if strings.Contains(strings.Trim(strings.TrimSpace(cleanPath), "/"), "/by-hash/") {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}
	}
	if isDebArtifactPath(cleanPath) {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceArtifact, Role: filerepo.DiscoveryIgnore}
	}
	if isDebAuxiliaryPath(cleanPath) {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceSidecar, Role: filerepo.DiscoveryIgnore}
	}
	return filerepo.DiscoveryResult{Class: filerepo.ResourceUnknown, Role: filerepo.DiscoveryIgnore}
}

func analyzeDistributionMetadataPath(cleanPath string) (filerepo.DiscoveryResult, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	foundDistribution := false
	for i := len(parts) - 3; i >= 0; i-- {
		if parts[i] != "dists" || parts[i+1] == "" {
			continue
		}
		foundDistribution = true
		suite := parts[i+1]
		rootPath := strings.Join(parts[:i+2], "/")
		root := debDistributionRoot(rootPath, suite, nil, nil, false)
		if i+2 == len(parts)-1 && (parts[i+2] == "InRelease" || parts[i+2] == "Release") {
			return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryCreateRoot, Root: root}, true
		}
		if i+4 >= len(parts) {
			continue
		}
		component := parts[i+2]
		segment := parts[i+3]
		fileName := parts[i+4]
		switch {
		case strings.HasPrefix(segment, "binary-") && strings.HasPrefix(fileName, "Packages"):
			arch := strings.TrimPrefix(segment, "binary-")
			if component == "" || arch == "" {
				return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}, true
			}
			root = debDistributionRoot(rootPath, suite, []string{component}, []string{arch}, false)
			return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryUpdateRoot, Root: root}, true
		case segment == "source" && strings.HasPrefix(fileName, "Sources"):
			if component == "" {
				return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}, true
			}
			root = debDistributionRoot(rootPath, suite, []string{component}, nil, true)
			return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryUpdateRoot, Root: root}, true
		}
	}
	if foundDistribution {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}, true
	}
	return filerepo.DiscoveryResult{}, false
}

func analyzeFlatMetadataPath(cleanPath string) (filerepo.DiscoveryResult, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	fileName := path.Base(trimmed)
	kind := debFlatMetadataKind(fileName)
	if kind == "" {
		return filerepo.DiscoveryResult{}, false
	}
	rootPath := strings.Trim(strings.TrimSpace(path.Dir(trimmed)), "/")
	if rootPath == "." {
		rootPath = ""
	}
	root := debFlatRoot(rootPath, kind == "sources")
	switch kind {
	case "release", "packages", "sources":
		root.Targets = []filerepo.MetadataTarget{debFlatIndexTarget(rootPath, kind)}
		root.PrimaryMetadata = []string{root.Targets[0].URL}
		if kind == "release" {
			root.PrimaryMetadata = []string{path.Join(rootPath, "InRelease"), path.Join(rootPath, "Release")}
		}
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryCreateRoot, Root: root}, true
	default:
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}, true
	}
}

func isDebArtifactPath(cleanPath string) bool {
	cleanPath = strings.Trim(strings.TrimSpace(cleanPath), "/")
	isArtifact := strings.HasSuffix(cleanPath, ".deb") || strings.HasSuffix(cleanPath, ".udeb") || strings.HasSuffix(cleanPath, ".ddeb") || strings.HasSuffix(cleanPath, ".dsc") || strings.Contains(cleanPath, ".orig.tar.") || strings.Contains(cleanPath, ".debian.tar.") || strings.HasSuffix(cleanPath, ".diff.gz")
	if cleanPath == "" || !isArtifact {
		return false
	}
	return cleanPath == "pool" || strings.HasPrefix(cleanPath, "pool/") || strings.Contains(cleanPath, "/pool/")
}

func isDebAuxiliaryPath(cleanPath string) bool {
	for _, suffix := range debPackageSidecarSuffixes {
		if strings.HasSuffix(cleanPath, suffix) {
			return true
		}
	}
	return false
}

func debDistributionRoot(rootPath, suite string, components, arches []string, source bool) filerepo.RepositoryRoot {
	sort.Strings(components)
	sort.Strings(arches)
	root := filerepo.RepositoryRoot{
		ID:              filerepo.RepositoryID(filerepo.LayoutDebDistribution, rootPath),
		Path:            rootPath,
		DisplayName:     suite,
		Layout:          filerepo.LayoutDebDistribution,
		PrimaryMetadata: []string{path.Join(rootPath, "InRelease"), path.Join(rootPath, "Release")},
		Targets: []filerepo.MetadataTarget{{
			URL:        path.Join(rootPath, "InRelease"),
			Candidates: []string{path.Join(rootPath, "Release")},
			Kind:       "release",
		}},
		Suite:         suite,
		Components:    append([]string(nil), components...),
		Architectures: append([]string(nil), arches...),
		Source:        source,
	}
	return root
}

func debFlatRoot(rootPath string, source bool) filerepo.RepositoryRoot {
	displayName := rootPath
	if displayName == "" {
		displayName = "/"
	}
	root := filerepo.RepositoryRoot{
		ID:          filerepo.RepositoryID(filerepo.LayoutDebFlat, rootPath),
		Path:        rootPath,
		DisplayName: displayName,
		Layout:      filerepo.LayoutDebFlat,
		Source:      source,
	}
	return root
}

func debFlatMetadataKind(fileName string) string {
	switch {
	case fileName == "InRelease" || fileName == "Release":
		return "release"
	case strings.HasPrefix(fileName, "Packages"):
		return "packages"
	case strings.HasPrefix(fileName, "Sources"):
		return "sources"
	default:
		return ""
	}
}

func debFlatIndexTarget(rootPath, kind string) filerepo.MetadataTarget {
	switch kind {
	case "release":
		return filerepo.MetadataTarget{
			URL:        path.Join(rootPath, "InRelease"),
			Candidates: []string{path.Join(rootPath, "Release")},
			Kind:       "release",
		}
	case "sources":
		return filerepo.MetadataTarget{
			URL:        path.Join(rootPath, "Sources.xz"),
			Candidates: []string{path.Join(rootPath, "Sources.gz"), path.Join(rootPath, "Sources")},
			Kind:       "sources",
		}
	default:
		return filerepo.MetadataTarget{
			URL:        path.Join(rootPath, "Packages.xz"),
			Candidates: []string{path.Join(rootPath, "Packages.gz"), path.Join(rootPath, "Packages")},
			Kind:       "packages",
		}
	}
}
