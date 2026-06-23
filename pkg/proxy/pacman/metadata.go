package pacman

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

type rootSpec struct {
	Repo string
	Arch string
}

func (s *rootSpec) Key() string {
	return strings.Join([]string{s.Repo, s.Arch}, "|")
}

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	return []filerepo.MetadataTarget{{
		URL:  path.Join(s.Repo, "os", s.Arch, s.Repo+".db"),
		Repo: s.Repo,
		Arch: s.Arch,
	}}
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	return false
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 || parts[1] != "os" {
		return nil, false
	}
	repoName, arch, fileName := parts[0], parts[2], parts[3]
	if repoName == "" || arch == "" {
		return nil, false
	}
	if fileName != repoName+".db" && fileName != repoName+".db.sig" && fileName != repoName+".files" && fileName != repoName+".files.sig" {
		return nil, false
	}
	return &rootSpec{Repo: repoName, Arch: arch}, true
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
			if path.Base(header.Name) != "desc" {
				continue
			}
			found = true
			filename, checksum, err := parseDesc(tarReader)
			if err != nil {
				_ = reader.Close()
				return nil, err
			}
			if filename == "" {
				continue
			}
			artifactPath := path.Join(path.Dir(blob.Path), filename)
			snapshot.Artifacts[artifactPath] = checksum
			snapshot.Auxiliary[artifactPath+".sig"] = checksum
		}
		_ = reader.Close()
		if !found {
			return nil, fmt.Errorf("%s: desc entries not found", blob.Path)
		}
	}
	return snapshot, nil
}

func parseDesc(input io.Reader) (string, string, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return "", "", err
	}
	var filename string
	var checksum string
	parts := strings.Split(string(data), "\n")
	for i := 0; i < len(parts); i++ {
		switch strings.TrimSpace(parts[i]) {
		case "%FILENAME%":
			if i+1 < len(parts) {
				filename = strings.TrimSpace(parts[i+1])
			}
		case "%SHA256SUM%":
			if i+1 < len(parts) {
				checksum = strings.TrimSpace(parts[i+1])
			}
		}
	}
	return filename, checksum, nil
}
