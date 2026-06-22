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

func metadataTargets(repositories []Repository) ([]filerepo.MetadataTarget, []string, error) {
	if len(repositories) == 0 {
		return nil, nil, fmt.Errorf("apk repositories must not be empty")
	}
	targets := make([]filerepo.MetadataTarget, 0, len(repositories))
	upstreams := make([]string, 0, len(repositories))
	seenUpstreams := map[string]struct{}{}
	for i, repo := range repositories {
		baseURL := strings.TrimRight(strings.TrimSpace(repo.URL), "/")
		if baseURL == "" {
			return nil, nil, fmt.Errorf("apk repositories[%d].url is empty", i)
		}
		branches := repo.Branches
		if repo.Branch != "" {
			if len(branches) != 0 {
				return nil, nil, fmt.Errorf("apk repositories[%d] must not set both branch and branches", i)
			}
			branches = []string{repo.Branch}
		}
		if len(branches) == 0 {
			return nil, nil, fmt.Errorf("apk repositories[%d].branches must not be empty", i)
		}
		if len(repo.Repos) == 0 {
			return nil, nil, fmt.Errorf("apk repositories[%d].repos must not be empty", i)
		}
		if len(repo.Architectures) == 0 {
			return nil, nil, fmt.Errorf("apk repositories[%d].architectures must not be empty", i)
		}
		if _, ok := seenUpstreams[baseURL]; !ok {
			seenUpstreams[baseURL] = struct{}{}
			upstreams = append(upstreams, baseURL)
		}
		for _, branch := range branches {
			branch = strings.Trim(strings.TrimSpace(branch), "/")
			if branch == "" {
				return nil, nil, fmt.Errorf("apk repositories[%d] contains empty branch", i)
			}
			for _, name := range repo.Repos {
				name = strings.Trim(strings.TrimSpace(name), "/")
				if name == "" {
					return nil, nil, fmt.Errorf("apk repositories[%d] contains empty repo", i)
				}
				for _, arch := range repo.Architectures {
					arch = strings.Trim(strings.TrimSpace(arch), "/")
					if arch == "" {
						return nil, nil, fmt.Errorf("apk repositories[%d] contains empty architecture", i)
					}
					targets = append(targets, filerepo.MetadataTarget{
						URL: path.Join(branch, name, arch, "APKINDEX.tar.gz"),
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
		snapshot.Artifacts[artifactPath] = checksum
		for _, suffix := range []string{".sig", ".asc", ".sha256", ".sha512"} {
			snapshot.Auxiliary[artifactPath+suffix] = checksum
		}
	}
	scanner := bufio.NewScanner(input)
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
