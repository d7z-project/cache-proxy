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
	Repo      string
	StorePath string
}

func (s *rootSpec) Key() string {
	return s.Repo
}

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	return []filerepo.MetadataTarget{{
		URL:  s.StorePath,
		Repo: s.Repo,
	}}
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	return false
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || parts[0] == "" {
		return nil, false
	}
	fileName := parts[len(parts)-1]

	var repoName string
	switch {
	case strings.HasSuffix(fileName, ".db"):
		repoName = strings.TrimSuffix(fileName, ".db")
	case strings.HasSuffix(fileName, ".db.sig"):
		repoName = strings.TrimSuffix(fileName, ".db.sig")
	case strings.HasSuffix(fileName, ".files"):
		repoName = strings.TrimSuffix(fileName, ".files")
	case strings.HasSuffix(fileName, ".files.sig"):
		repoName = strings.TrimSuffix(fileName, ".files.sig")
	default:
		return nil, false
	}
	if repoName == "" {
		return nil, false
	}
	return &rootSpec{Repo: repoName, StorePath: trimmed}, true
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata:  map[string]filerepo.MetadataObject{},
		Artifacts: map[string]filerepo.RepoObject{},
		Auxiliary: map[string]filerepo.RepoObject{},
	}
	for _, target := range session.Targets() {
		blob, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: true}

		dbPath := strings.TrimSuffix(blob.Path, ".db")
		for _, suffix := range []string{".files", ".files.sig"} {
			companionPath := dbPath + suffix
			if companion, err := session.FetchDerived(ctx, companionPath); err != nil {
				return nil, err
			} else if companion.Path != "" {
				snapshot.Metadata[companion.Path] = companion
			}
		}
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
			snapshot.Artifacts[artifactPath] = filerepo.RepoObject{Path: artifactPath, Identity: checksum, ContentHash: checksum}
			sigPath := artifactPath + ".sig"
			snapshot.Auxiliary[sigPath] = filerepo.RepoObject{Path: sigPath, Identity: checksum}
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
