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

type discoverer struct{}

func (discoverer) Discover(cleanPath string) filerepo.DiscoveryResult {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	for i := len(parts) - 3; i >= 0; i-- {
		if parts[i] != "dists" || parts[i+1] == "" {
			continue
		}
		suite := parts[i+1]
		rootPath := strings.Join(parts[:i+2], "/")
		root := debRepositoryRoot(rootPath, suite, nil, nil, false)
		if i+2 == len(parts)-1 && (parts[i+2] == "InRelease" || parts[i+2] == "Release") {
			return filerepo.DiscoveryResult{Matched: true, Role: filerepo.DiscoveryCreateRoot, Root: root}
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
				return filerepo.DiscoveryResult{}
			}
			root = debRepositoryRoot(rootPath, suite, []string{component}, []string{arch}, false)
			return filerepo.DiscoveryResult{Matched: true, Role: filerepo.DiscoveryUpdateRoot, Root: root}
		case segment == "source" && strings.HasPrefix(fileName, "Sources"):
			if component == "" {
				return filerepo.DiscoveryResult{}
			}
			root = debRepositoryRoot(rootPath, suite, []string{component}, nil, true)
			return filerepo.DiscoveryResult{Matched: true, Role: filerepo.DiscoveryUpdateRoot, Root: root}
		}
	}
	return filerepo.DiscoveryResult{}
}

func debRepositoryRoot(rootPath, suite string, components, arches []string, source bool) filerepo.RepositoryRoot {
	sort.Strings(components)
	sort.Strings(arches)
	root := filerepo.RepositoryRoot{
		ID:              rootPath,
		Path:            rootPath,
		DisplayName:     suite,
		PrimaryMetadata: []string{path.Join(rootPath, "InRelease"), path.Join(rootPath, "Release")},
		Targets: []filerepo.MetadataTarget{{
			URL:        path.Join(rootPath, "InRelease"),
			Candidates: []string{path.Join(rootPath, "Release")},
			Kind:       "release",
		}},
		Kind:          "deb",
		Suite:         suite,
		Components:    append([]string(nil), components...),
		Architectures: append([]string(nil), arches...),
		Source:        source,
	}
	root.Attributes = []filerepo.RepositoryAttribute{
		{LabelKey: "repo_path", Value: rootPath},
		{LabelKey: "suite", Value: suite},
	}
	if len(components) > 0 {
		root.Attributes = append(root.Attributes, filerepo.RepositoryAttribute{LabelKey: "components", Value: strings.Join(components, ", ")})
	}
	if len(arches) > 0 {
		root.Attributes = append(root.Attributes, filerepo.RepositoryAttribute{LabelKey: "architectures", Value: strings.Join(arches, ", ")})
	}
	if source {
		root.Attributes = append(root.Attributes, filerepo.RepositoryAttribute{LabelKey: "source_packages", Value: "yes"})
	}
	return root
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifactCount := 0
	for _, target := range session.Targets() {
		if target.Kind != "release" {
			return nil, fmt.Errorf("%s: unsupported seed metadata target kind %q", target.URL, target.Kind)
		}
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
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func rebuildCleanupIndex(_ context.Context, session *filerepo.LocalSession, paths *filerepo.PathIndexBuilder) error {
	artifactCount := 0
	for _, target := range session.Targets() {
		if target.Kind != "release" {
			continue
		}
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
	trimmed := strings.TrimPrefix(cleanPath, "dists/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 2 {
		trimmed = parts[1]
	}
	expected := sums[trimmed]
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
