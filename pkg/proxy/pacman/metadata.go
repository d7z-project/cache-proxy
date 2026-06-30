package pacman

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
	Repo      string
	StorePath string
}

func (s *rootSpec) Key() string     { return s.Repo }
func (s *rootSpec) SubPath() string { return s.StorePath }

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

	if !strings.HasSuffix(fileName, ".db") {
		return nil, false
	}
	repoName := strings.TrimSuffix(fileName, ".db")
	if repoName == "" {
		return nil, false
	}
	return &rootSpec{Repo: repoName, StorePath: trimmed}, true
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifactCount := 0
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

		blobReader, err := blob.Open()
		if err != nil {
			return nil, err
		}
		reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
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
			filename, err := parseDesc(tarReader)
			if err != nil {
				_ = reader.Close()
				return nil, err
			}
			if filename == "" {
				continue
			}
			artifactPath := path.Join(path.Dir(blob.Path), filename)
			paths.Add(artifactPath)
			paths.AddAuxiliary(artifactPath)
			artifactCount++
		}
		_ = reader.Close()
		session.Release(target)
		if !found {
			return nil, fmt.Errorf("%s: desc entries not found", blob.Path)
		}
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func rebuildCleanupIndex(_ context.Context, session *filerepo.LocalSession, paths *filerepo.PathIndexBuilder) error {
	for _, target := range session.Targets() {
		blob, err := session.Fetch(target)
		if err != nil {
			return err
		}
		defer blob.Close()
		blobReader, err := blob.Open()
		if err != nil {
			return err
		}
		reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
		if err != nil {
			return err
		}
		tarReader := tar.NewReader(reader)
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = reader.Close()
				return err
			}
			if path.Base(header.Name) != "desc" {
				continue
			}
			filename, err := parseDesc(tarReader)
			if err != nil {
				_ = reader.Close()
				return err
			}
			if filename != "" {
				artifactPath := path.Join(path.Dir(blob.Path), filename)
				paths.Add(artifactPath)
				paths.AddAuxiliary(artifactPath)
			}
		}
		_ = reader.Close()
	}
	return nil
}

func parseDesc(input io.Reader) (string, error) {
	var filename string
	scanner := bufio.NewScanner(input)
	scanner.Buffer(nil, 1<<20)
	nextFilename := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case nextFilename:
			filename = line
			nextFilename = false
		case line == "%FILENAME%":
			nextFilename = true
		}
	}
	return filename, scanner.Err()
}
