package pacman

import (
	"archive/tar"
	"bufio"
	"context"
	"io"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

type discoverer struct{}

func (discoverer) Discover(cleanPath string) filerepo.DiscoveryResult {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || parts[0] == "" {
		return filerepo.DiscoveryResult{}
	}
	fileName := parts[len(parts)-1]
	if !strings.HasSuffix(fileName, ".db") {
		return filerepo.DiscoveryResult{}
	}
	if len(parts) < 4 || parts[len(parts)-3] != "os" {
		return filerepo.DiscoveryResult{}
	}
	repoName := strings.TrimSuffix(fileName, ".db")
	if repoName == "" {
		return filerepo.DiscoveryResult{}
	}
	rootPath := strings.Join(parts[:len(parts)-1], "/")
	arch := parts[len(parts)-2]
	if arch == "" {
		return filerepo.DiscoveryResult{}
	}
	return filerepo.DiscoveryResult{
		Matched: true,
		Role:    filerepo.DiscoveryCreateRoot,
		Root: filerepo.RepositoryRoot{
			ID:              rootPath,
			Path:            rootPath,
			DisplayName:     rootPath,
			PrimaryMetadata: []string{path.Join(rootPath, repoName+".db")},
			Targets: []filerepo.MetadataTarget{{
				URL:  path.Join(rootPath, repoName+".db"),
				Repo: repoName,
				Arch: arch,
			}},
			Kind: "pacman",
			Repo: repoName,
			Arch: arch,
			Attributes: []filerepo.RepositoryAttribute{
				{LabelKey: "repo_path", Value: rootPath},
				{LabelKey: "repository", Value: repoName},
				{LabelKey: "architecture", Value: arch},
			},
		},
	}
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
			session.Release(target)
			return nil, err
		}
		reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
		if err != nil {
			session.Release(target)
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
				session.Release(target)
				return nil, err
			}
			if path.Base(header.Name) != "desc" {
				continue
			}
			found = true
			filename, err := parseDesc(tarReader)
			if err != nil {
				_ = reader.Close()
				session.Release(target)
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
			snapshot.ArtifactCount = artifactCount
			return snapshot, nil
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
		blobReader, err := blob.Open()
		if err != nil {
			blob.Close()
			return err
		}
		reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
		if err != nil {
			blob.Close()
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
				blob.Close()
				return err
			}
			if path.Base(header.Name) != "desc" {
				continue
			}
			filename, err := parseDesc(tarReader)
			if err != nil {
				_ = reader.Close()
				blob.Close()
				return err
			}
			if filename != "" {
				artifactPath := path.Join(path.Dir(blob.Path), filename)
				paths.Add(artifactPath)
				paths.AddAuxiliary(artifactPath)
			}
		}
		_ = reader.Close()
		blob.Close()
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
