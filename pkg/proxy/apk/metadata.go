package apk

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

type rootSpec struct {
	Branch string
	Repo   string
	Arch   string
}

func (s *rootSpec) Key() string {
	return strings.Join([]string{s.Branch, s.Repo, s.Arch}, "|")
}

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	return []filerepo.MetadataTarget{{URL: path.Join(s.Branch, s.Repo, s.Arch, "APKINDEX.tar.gz")}}
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	return false
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 || parts[3] != "APKINDEX.tar.gz" {
		return nil, false
	}
	for _, part := range parts[:3] {
		if part == "" {
			return nil, false
		}
	}
	return &rootSpec{Branch: parts[0], Repo: parts[1], Arch: parts[2]}, true
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata:  map[string]filerepo.MetadataObject{},
		Artifacts: map[string]filerepo.RepoObject{},
	}
	for _, target := range session.Targets() {
		blob, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: true}
		for _, companionPath := range filerepo.DeduceCompanions(blob.Path) {
			if companion, err := session.FetchDerived(ctx, companionPath); err != nil {
				return nil, err
			} else if companion.Path != "" {
				snapshot.Metadata[companion.Path] = companion
			}
		}
		reader, err := filerepo.OpenCompressed(blob.Body, blob.Path)
		if err != nil {
			return nil, err
		}
		tarReader := tar.NewReader(reader)
		found := false
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = reader.Close()
				return nil, err
			}
			if path.Base(header.Name) != "APKINDEX" {
				continue
			}
			found = true
			if err := parseIndex(path.Dir(blob.Path), tarReader, snapshot); err != nil {
				_ = reader.Close()
				return nil, err
			}
			break
		}
		_ = reader.Close()
		session.Release(target)
		if !found {
			return nil, fmt.Errorf("%s: APKINDEX entry not found", blob.Path)
		}
	}
	return snapshot, nil
}

func parseIndex(basePath string, input io.Reader, snapshot *filerepo.LiveSnapshot) error {
	var name string
	var version string
	var checksum string
	flush := func() {
		if name == "" || version == "" {
			return
		}
		artifactPath := path.Join(basePath, name+"-"+version+".apk")
		snapshot.Artifacts[artifactPath] = filerepo.RepoObject{Path: artifactPath, Identity: checksum, ContentHash: checksum}
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(nil, 10<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			name, version, checksum = "", "", ""
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch key {
		case "P":
			name = value
		case "V":
			version = value
		case "C":
			checksum = value
		}
	}
	flush()
	return scanner.Err()
}
