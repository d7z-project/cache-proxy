package deb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

type rootSpec struct {
	Suite         string
	Components    []string
	Architectures []string
	Source        bool
}

func (s *rootSpec) Key() string { return s.Suite }

func (s *rootSpec) SubPath() string {
	if len(s.Components) > 0 && len(s.Architectures) > 0 {
		return "dists/" + s.Suite + "/" + s.Components[0] + "/binary-" + s.Architectures[0]
	}
	if s.Source {
		return "dists/" + s.Suite + "/" + s.Components[0] + "/source"
	}
	return "dists/" + s.Suite
}

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	return []filerepo.MetadataTarget{{
		URL:        path.Join("dists", s.Suite, "InRelease"),
		Candidates: []string{path.Join("dists", s.Suite, "Release")},
		Kind:       "release",
	}}
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	candidate, ok := other.(*rootSpec)
	if !ok || s.Suite != candidate.Suite {
		return false
	}
	changed := false
	for _, component := range candidate.Components {
		if !slices.Contains(s.Components, component) {
			s.Components = append(s.Components, component)
			changed = true
		}
	}
	for _, arch := range candidate.Architectures {
		if !slices.Contains(s.Architectures, arch) {
			s.Architectures = append(s.Architectures, arch)
			changed = true
		}
	}
	if candidate.Source && !s.Source {
		s.Source = true
		changed = true
	}
	return changed
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 || parts[0] != "dists" {
		return nil, false
	}
	suite := parts[1]
	if suite == "" {
		return nil, false
	}
	if len(parts) == 3 && (parts[2] == "InRelease" || parts[2] == "Release") {
		return &rootSpec{Suite: suite}, true
	}
	if len(parts) < 5 {
		return nil, false
	}
	component := parts[2]
	segment := parts[3]
	fileName := parts[4]
	switch {
	case strings.HasPrefix(segment, "binary-") && strings.HasPrefix(fileName, "Packages"):
		arch := strings.TrimPrefix(segment, "binary-")
		if component == "" || arch == "" {
			return nil, false
		}
		return &rootSpec{Suite: suite, Components: []string{component}, Architectures: []string{arch}}, true
	case segment == "source" && strings.HasPrefix(fileName, "Sources"):
		if component == "" {
			return nil, false
		}
		return &rootSpec{Suite: suite, Components: []string{component}, Source: true}, true
	default:
		return nil, false
	}
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
				return nil, err
			}
			reader, err := filerepo.OpenCompressed(blobReader, indexBlob.Path)
			if err != nil {
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
		defer blob.Close()
		releaseSums, err := parseReleaseSHA256(blob)
		if err != nil {
			return err
		}
		targets := releaseIndexTargets(blob.Path, releaseSums)
		for _, indexTarget := range targets {
			indexBlob, err := session.Fetch(indexTarget)
			if err != nil {
				return err
			}
			defer indexBlob.Close()
			blobReader, err := indexBlob.Open()
			if err != nil {
				return err
			}
			reader, err := filerepo.OpenCompressed(blobReader, indexBlob.Path)
			if err != nil {
				return err
			}
			if indexTarget.Kind == "packages" {
				artifactCount, err = parsePackages(reader, paths, artifactCount)
			} else {
				artifactCount, err = parseSources(reader, paths, artifactCount)
			}
			_ = reader.Close()
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
	slices.Sort(keys)
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
