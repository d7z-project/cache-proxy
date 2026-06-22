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

func metadataTargets(repositories []Repository) ([]filerepo.MetadataTarget, []string, error) {
	if len(repositories) == 0 {
		return nil, nil, fmt.Errorf("pacman repositories must not be empty")
	}
	targets := []filerepo.MetadataTarget{}
	upstreams := make([]string, 0, len(repositories))
	seenUpstreams := map[string]struct{}{}
	for i, repo := range repositories {
		baseURL := strings.TrimRight(strings.TrimSpace(repo.URL), "/")
		if baseURL == "" {
			return nil, nil, fmt.Errorf("pacman repositories[%d].url is empty", i)
		}
		if len(repo.Repos) == 0 {
			return nil, nil, fmt.Errorf("pacman repositories[%d].repos must not be empty", i)
		}
		if len(repo.Architectures) == 0 {
			return nil, nil, fmt.Errorf("pacman repositories[%d].architectures must not be empty", i)
		}
		if _, ok := seenUpstreams[baseURL]; !ok {
			seenUpstreams[baseURL] = struct{}{}
			upstreams = append(upstreams, baseURL)
		}
		for _, repoName := range repo.Repos {
			repoName = strings.Trim(strings.TrimSpace(repoName), "/")
			if repoName == "" {
				return nil, nil, fmt.Errorf("pacman repositories[%d] contains empty repo", i)
			}
			for _, arch := range repo.Architectures {
				arch = strings.Trim(strings.TrimSpace(arch), "/")
				if arch == "" {
					return nil, nil, fmt.Errorf("pacman repositories[%d] contains empty architecture", i)
				}
				targets = append(targets, filerepo.MetadataTarget{
					URL:  path.Join(repoName, "os", arch, repoName+".db"),
					Repo: repoName,
					Arch: arch,
				})
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
