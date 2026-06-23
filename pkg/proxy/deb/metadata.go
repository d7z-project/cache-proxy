package deb

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"path"
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

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	targets := []filerepo.MetadataTarget{{
		URL:        path.Join("dists", s.Suite, "InRelease"),
		Candidates: []string{path.Join("dists", s.Suite, "Release")},
		Kind:       "release",
	}}
	for _, component := range s.Components {
		for _, arch := range s.Architectures {
			basePath := path.Join("dists", s.Suite, component, "binary-"+arch, "Packages")
			targets = append(targets, filerepo.MetadataTarget{
				URL:        basePath + ".xz",
				Candidates: []string{basePath + ".gz", basePath},
				Kind:       "packages",
			})
		}
		if s.Source {
			basePath := path.Join("dists", s.Suite, component, "source", "Sources")
			targets = append(targets, filerepo.MetadataTarget{
				URL:        basePath + ".xz",
				Candidates: []string{basePath + ".gz", basePath},
				Kind:       "sources",
			})
		}
	}
	return targets
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	candidate, ok := other.(*rootSpec)
	if !ok || s.Suite != candidate.Suite {
		return false
	}
	changed := false
	for _, component := range candidate.Components {
		if !containsString(s.Components, component) {
			s.Components = append(s.Components, component)
			changed = true
		}
	}
	for _, arch := range candidate.Architectures {
		if !containsString(s.Architectures, arch) {
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

func containsString(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata:  map[string]struct{}{},
		Artifacts: map[string]string{},
		Auxiliary: map[string]string{},
	}
	for _, target := range session.Targets() {
		blob, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[blob.Path] = struct{}{}
		switch target.Kind {
		case "release":
			continue
		case "packages", "sources":
			reader, err := filerepo.OpenCompressed(blob.Body, blob.Path)
			if err != nil {
				return nil, err
			}
			if target.Kind == "packages" {
				err = parsePackages(reader, snapshot)
			} else {
				err = parseSources(reader, snapshot)
			}
			_ = reader.Close()
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%s: unsupported metadata target kind %q", blob.Path, target.Kind)
		}
	}
	return snapshot, nil
}

func parsePackages(input io.Reader, snapshot *filerepo.LiveSnapshot) error {
	return parseDebStanzas(input, func(fields map[string]string) {
		filename := strings.TrimSpace(fields["Filename"])
		if filename == "" {
			return
		}
		snapshot.Artifacts[filename] = strings.TrimSpace(fields["SHA256"])
	})
}

func parseSources(input io.Reader, snapshot *filerepo.LiveSnapshot) error {
	return parseDebStanzas(input, func(fields map[string]string) {
		directory := strings.TrimSpace(fields["Directory"])
		if directory == "" {
			return
		}
		for _, line := range strings.Split(fields["Checksums-Sha256"], "\n") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			snapshot.Artifacts[path.Join(directory, parts[2])] = parts[0]
		}
	})
}

func parseDebStanzas(input io.Reader, apply func(map[string]string)) error {
	scanner := bufio.NewScanner(input)
	fields := map[string]string{}
	currentKey := ""
	flush := func() {
		if len(fields) == 0 {
			return
		}
		apply(fields)
		fields = map[string]string{}
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
	return scanner.Err()
}
