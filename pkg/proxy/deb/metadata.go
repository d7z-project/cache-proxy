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

func metadataTargets(repositories []Repository) ([]filerepo.MetadataTarget, []string, error) {
	if len(repositories) == 0 {
		return nil, nil, fmt.Errorf("deb repositories must not be empty")
	}
	targets := []filerepo.MetadataTarget{}
	upstreams := make([]string, 0, len(repositories))
	seenUpstreams := map[string]struct{}{}
	for i, repo := range repositories {
		baseURL := strings.TrimRight(strings.TrimSpace(repo.URL), "/")
		if baseURL == "" {
			return nil, nil, fmt.Errorf("deb repositories[%d].url is empty", i)
		}
		suites := repo.Suites
		if repo.Suite != "" {
			if len(suites) != 0 {
				return nil, nil, fmt.Errorf("deb repositories[%d] must not set both suite and suites", i)
			}
			suites = []string{repo.Suite}
		}
		if len(suites) == 0 {
			return nil, nil, fmt.Errorf("deb repositories[%d].suites must not be empty", i)
		}
		if len(repo.Components) == 0 {
			return nil, nil, fmt.Errorf("deb repositories[%d].components must not be empty", i)
		}
		if len(repo.Architectures) == 0 {
			return nil, nil, fmt.Errorf("deb repositories[%d].architectures must not be empty", i)
		}
		if _, ok := seenUpstreams[baseURL]; !ok {
			seenUpstreams[baseURL] = struct{}{}
			upstreams = append(upstreams, baseURL)
		}
		for _, suite := range suites {
			suite = strings.Trim(strings.TrimSpace(suite), "/")
			if suite == "" {
				return nil, nil, fmt.Errorf("deb repositories[%d] contains empty suite", i)
			}
			targets = append(targets, filerepo.MetadataTarget{
				URL:        path.Join("dists", suite, "InRelease"),
				Candidates: []string{path.Join("dists", suite, "Release")},
				Kind:       "release",
			})
			for _, component := range repo.Components {
				component = strings.Trim(strings.TrimSpace(component), "/")
				if component == "" {
					return nil, nil, fmt.Errorf("deb repositories[%d] contains empty component", i)
				}
				for _, arch := range repo.Architectures {
					arch = strings.Trim(strings.TrimSpace(arch), "/")
					if arch == "" {
						return nil, nil, fmt.Errorf("deb repositories[%d] contains empty architecture", i)
					}
					basePath := path.Join("dists", suite, component, "binary-"+arch, "Packages")
					targets = append(targets, filerepo.MetadataTarget{
						URL:        basePath + ".xz",
						Candidates: []string{basePath + ".gz", basePath},
						Kind:       "packages",
					})
				}
				if repo.Source {
					basePath := path.Join("dists", suite, component, "source", "Sources")
					targets = append(targets, filerepo.MetadataTarget{
						URL:        basePath + ".xz",
						Candidates: []string{basePath + ".gz", basePath},
						Kind:       "sources",
					})
				}
			}
		}
	}
	return targets, upstreams, nil
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
