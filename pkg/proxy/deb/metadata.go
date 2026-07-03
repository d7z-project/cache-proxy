package deb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

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
		return filerepo.DiscoveryResult{Class: filerepo.ResourceAuxiliary, Role: filerepo.DiscoveryIgnore}
	}
	return filerepo.DiscoveryResult{Class: filerepo.ResourceUnknown, Role: filerepo.DiscoveryIgnore}
}

func analyzeDistributionMetadataPath(cleanPath string) (filerepo.DiscoveryResult, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	for i := len(parts) - 3; i >= 0; i-- {
		if parts[i] != "dists" || parts[i+1] == "" {
			continue
		}
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
	if cleanPath == "" || !(strings.HasSuffix(cleanPath, ".deb") || strings.HasSuffix(cleanPath, ".udeb") || strings.HasSuffix(cleanPath, ".ddeb") || strings.HasSuffix(cleanPath, ".dsc") || strings.Contains(cleanPath, ".orig.tar.") || strings.Contains(cleanPath, ".debian.tar.") || strings.HasSuffix(cleanPath, ".diff.gz")) {
		return false
	}
	return cleanPath == "pool" || strings.HasPrefix(cleanPath, "pool/") || strings.Contains(cleanPath, "/pool/")
}

func isDebAuxiliaryPath(cleanPath string) bool {
	return strings.HasSuffix(cleanPath, ".gpg") || strings.HasSuffix(cleanPath, ".sig") || strings.HasSuffix(cleanPath, ".asc") || strings.HasSuffix(cleanPath, ".sha256") || strings.HasSuffix(cleanPath, ".sha512") || strings.HasSuffix(cleanPath, ".md5sum")
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

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifactCount := 0
	for _, target := range session.Targets() {
		switch target.Kind {
		case "release":
			blob, err := session.Fetch(ctx, target)
			if err != nil {
				return nil, err
			}
			snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: true}
			for _, candidate := range append([]string{target.URL}, target.Candidates...) {
				if candidate != blob.Path {
					snapshot.Metadata[candidate] = filerepo.MetadataObject{Path: blob.Path, Required: false}
				}
			}
			if strings.HasSuffix(blob.Path, "/Release") {
				for _, companionPath := range filerepo.DeduceCompanions(blob.Path) {
					if companion, err := session.FetchDerived(ctx, companionPath); err != nil {
						return nil, err
					} else if companion.Path != "" {
						snapshot.Metadata[companion.Path] = companion
					}
				}
			}
			releaseSums, err := parseReleaseSHA256(blob)
			if err != nil {
				session.Release(target)
				return nil, err
			}
			session.Release(target)

			targets := releaseIndexTargets(blob.Path, releaseSums)
			if len(targets) == 0 {
				return nil, fmt.Errorf("%s: Release contains no package indexes", blob.Path)
			}
			for _, indexTarget := range targets {
				indexBlob, err := session.Fetch(ctx, indexTarget)
				if err != nil {
					return nil, err
				}
				if err := verifyReleaseChecksum(releaseSums, indexBlob.Path, indexBlob); err != nil {
					return nil, err
				}
				snapshot.Metadata[indexBlob.Path] = filerepo.MetadataObject{Path: indexBlob.Path, Required: true}
				for _, candidate := range append([]string{indexTarget.URL}, indexTarget.Candidates...) {
					if candidate != indexBlob.Path {
						snapshot.Metadata[candidate] = filerepo.MetadataObject{Path: indexBlob.Path, Required: false}
					}
				}
				blobReader, err := indexBlob.Open()
				if err != nil {
					session.Release(indexTarget)
					return nil, err
				}
				reader, err := filerepo.OpenCompressed(blobReader, indexBlob.Path)
				if err != nil {
					session.Release(indexTarget)
					return nil, err
				}
				if indexTarget.Kind == "packages" {
					artifactCount, err = parsePackages(reader, paths, artifactCount)
				} else {
					artifactCount, err = parseSources(reader, paths, artifactCount)
				}
				_ = reader.Close()
				session.Release(indexTarget)
				if err != nil {
					return nil, err
				}
			}
		case "packages", "sources":
			indexBlob, err := session.Fetch(ctx, target)
			if err != nil {
				return nil, err
			}
			snapshot.Metadata[indexBlob.Path] = filerepo.MetadataObject{Path: indexBlob.Path, Required: true}
			for _, candidate := range append([]string{target.URL}, target.Candidates...) {
				if candidate != indexBlob.Path {
					snapshot.Metadata[candidate] = filerepo.MetadataObject{Path: indexBlob.Path, Required: false}
				}
			}
			rootPath := strings.Trim(strings.TrimSpace(path.Dir(indexBlob.Path)), "/")
			if rootPath == "." {
				rootPath = ""
			}
			releaseTarget := debFlatIndexTarget(rootPath, "release")
			if releaseBlob, err := session.Fetch(ctx, releaseTarget); err == nil {
				snapshot.Metadata[releaseBlob.Path] = filerepo.MetadataObject{Path: releaseBlob.Path, Required: false}
				for _, candidate := range append([]string{releaseTarget.URL}, releaseTarget.Candidates...) {
					if candidate != releaseBlob.Path {
						snapshot.Metadata[candidate] = filerepo.MetadataObject{Path: releaseBlob.Path, Required: false}
					}
				}
				if strings.HasSuffix(releaseBlob.Path, "Release") {
					for _, companionPath := range filerepo.DeduceCompanions(releaseBlob.Path) {
						if companion, err := session.FetchDerived(ctx, companionPath); err != nil {
							return nil, err
						} else if companion.Path != "" {
							snapshot.Metadata[companion.Path] = companion
						}
					}
				}
				if releaseSums, err := parseReleaseSHA256(releaseBlob); err != nil {
					session.Release(releaseTarget)
					return nil, err
				} else if len(releaseSums) > 0 {
					if err := verifyReleaseChecksum(releaseSums, indexBlob.Path, indexBlob); err != nil {
						session.Release(releaseTarget)
						return nil, err
					}
				}
				session.Release(releaseTarget)
			}
			blobReader, err := indexBlob.Open()
			if err != nil {
				session.Release(target)
				return nil, err
			}
			reader, err := filerepo.OpenCompressed(blobReader, indexBlob.Path)
			if err != nil {
				session.Release(target)
				return nil, err
			}
			if target.Kind == "packages" {
				artifactCount, err = parsePackages(reader, paths, artifactCount)
			} else {
				artifactCount, err = parseSources(reader, paths, artifactCount)
			}
			_ = reader.Close()
			session.Release(target)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%s: unsupported seed metadata target kind %q", target.URL, target.Kind)
		}
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func rebuildCleanupIndex(_ context.Context, session *filerepo.LocalSession, paths *filerepo.PathIndexBuilder) error {
	artifactCount := 0
	for _, target := range session.Targets() {
		switch target.Kind {
		case "release":
			blob, err := session.Fetch(target)
			if err != nil {
				return err
			}
			releaseSums, err := parseReleaseSHA256(blob)
			if err != nil {
				blob.Close()
				return err
			}
			targets := releaseIndexTargets(blob.Path, releaseSums)
			for _, indexTarget := range targets {
				indexBlob, err := session.Fetch(indexTarget)
				if err != nil {
					blob.Close()
					return err
				}
				blobReader, err := indexBlob.Open()
				if err != nil {
					indexBlob.Close()
					blob.Close()
					return err
				}
				reader, err := filerepo.OpenCompressed(blobReader, indexBlob.Path)
				if err != nil {
					indexBlob.Close()
					blob.Close()
					return err
				}
				if indexTarget.Kind == "packages" {
					artifactCount, err = parsePackages(reader, paths, artifactCount)
				} else {
					artifactCount, err = parseSources(reader, paths, artifactCount)
				}
				_ = reader.Close()
				indexBlob.Close()
				if err != nil {
					blob.Close()
					return err
				}
			}
			blob.Close()
		case "packages", "sources":
			indexBlob, err := session.Fetch(target)
			if err != nil {
				return err
			}
			blobReader, err := indexBlob.Open()
			if err != nil {
				indexBlob.Close()
				return err
			}
			reader, err := filerepo.OpenCompressed(blobReader, indexBlob.Path)
			if err != nil {
				indexBlob.Close()
				return err
			}
			if target.Kind == "packages" {
				artifactCount, err = parsePackages(reader, paths, artifactCount)
			} else {
				artifactCount, err = parseSources(reader, paths, artifactCount)
			}
			_ = reader.Close()
			indexBlob.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func releaseIndexTargets(releasePath string, sums map[string]string) []filerepo.MetadataTarget {
	suitePrefix := strings.TrimSuffix(releasePath, "/InRelease")
	suitePrefix = strings.TrimSuffix(suitePrefix, "/Release")
	type group struct {
		kind  string
		paths map[string]string
	}
	groups := map[string]*group{}
	for item := range sums {
		kind, base, ok := releaseIndexBase(item)
		if !ok {
			continue
		}
		g := groups[base]
		if g == nil {
			g = &group{kind: kind, paths: map[string]string{}}
			groups[base] = g
		}
		g.paths[item] = path.Join(suitePrefix, item)
	}
	var targets []filerepo.MetadataTarget
	for _, base := range sortedKeys(groups) {
		g := groups[base]
		var selected string
		for _, suffix := range []string{".xz", ".gz", ""} {
			if full := g.paths[base+suffix]; full != "" {
				selected = full
				break
			}
		}
		if selected == "" {
			continue
		}
		var candidates []string
		for _, suffix := range []string{".xz", ".gz", ""} {
			full := g.paths[base+suffix]
			if full != "" && full != selected {
				candidates = append(candidates, full)
			}
		}
		targets = append(targets, filerepo.MetadataTarget{URL: selected, Candidates: candidates, Kind: g.kind})
	}
	return targets
}

func releaseIndexBase(item string) (kind, base string, ok bool) {
	switch {
	case strings.HasSuffix(item, "/Packages"):
		return "packages", item, true
	case strings.HasSuffix(item, "/Packages.gz"):
		return "packages", strings.TrimSuffix(item, ".gz"), true
	case strings.HasSuffix(item, "/Packages.xz"):
		return "packages", strings.TrimSuffix(item, ".xz"), true
	case strings.HasSuffix(item, "/Sources"):
		return "sources", item, true
	case strings.HasSuffix(item, "/Sources.gz"):
		return "sources", strings.TrimSuffix(item, ".gz"), true
	case strings.HasSuffix(item, "/Sources.xz"):
		return "sources", strings.TrimSuffix(item, ".xz"), true
	default:
		return "", "", false
	}
}

func sortedKeys[T any](items map[string]T) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func parsePackages(input io.Reader, paths *filerepo.PathIndexBuilder, count int) (int, error) {
	return parseDebStanzas(input, count, func(fields map[string]string) int {
		filename := strings.TrimSpace(fields["Filename"])
		if filename == "" {
			return 0
		}
		paths.Add(filename)
		paths.AddAuxiliary(filename)
		return 1
	})
}

func parseSources(input io.Reader, paths *filerepo.PathIndexBuilder, count int) (int, error) {
	return parseDebStanzas(input, count, func(fields map[string]string) int {
		directory := strings.TrimSpace(fields["Directory"])
		if directory == "" {
			return 0
		}
		added := 0
		for _, line := range strings.Split(fields["Checksums-Sha256"], "\n") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			artifactPath := path.Join(directory, parts[2])
			paths.Add(artifactPath)
			paths.AddAuxiliary(artifactPath)
			added++
		}
		return added
	})
}

func parseReleaseSHA256(blob filerepo.MetadataBlob) (map[string]string, error) {
	result := map[string]string{}
	reader, err := blob.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, 10<<20)
	inSHA256 := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "SHA256:") {
			inSHA256 = true
			continue
		}
		if inSHA256 && !strings.HasPrefix(line, " ") {
			break
		}
		if !inSHA256 {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 3 {
			result[parts[2]] = parts[0]
		}
	}
	return result, scanner.Err()
}

func verifyReleaseChecksum(sums map[string]string, cleanPath string, blob filerepo.MetadataBlob) error {
	if len(sums) == 0 {
		return fmt.Errorf("%s: Release SHA256 section is missing", cleanPath)
	}
	expected := sums[cleanPath]
	if expected == "" {
		trimmed := cleanPath
		if index := strings.LastIndex(trimmed, "/dists/"); index >= 0 {
			trimmed = trimmed[index+len("/dists/"):]
		} else {
			trimmed = strings.TrimPrefix(trimmed, "dists/")
		}
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) == 2 {
			trimmed = parts[1]
		}
		expected = sums[trimmed]
	}
	if expected == "" {
		return fmt.Errorf("%s: missing Release SHA256", cleanPath)
	}
	reader, err := blob.Open()
	if err != nil {
		return err
	}
	defer reader.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return err
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("%s: Release SHA256 mismatch", cleanPath)
	}
	return nil
}

func parseDebStanzas(input io.Reader, count int, apply func(map[string]string) int) (int, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(nil, 10<<20)
	fields := map[string]string{}
	currentKey := ""
	flush := func() {
		if len(fields) == 0 {
			return
		}
		count += apply(fields)
		for key := range fields {
			delete(fields, key)
		}
		currentKey = ""
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			if currentKey != "" {
				fields[currentKey] += "\n" + strings.TrimSpace(line)
			}
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		currentKey = key
		fields[key] = strings.TrimSpace(value)
	}
	flush()
	return count, scanner.Err()
}
