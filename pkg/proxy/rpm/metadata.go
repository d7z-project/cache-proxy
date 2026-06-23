package rpm

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

func metadataTargets(repositories []Repository) ([]filerepo.MetadataTarget, []string, error) {
	if len(repositories) == 0 {
		return nil, nil, fmt.Errorf("rpm repositories must not be empty")
	}
	var targets []filerepo.MetadataTarget
	upstreams := make([]string, 0, len(repositories))
	seenUpstreams := map[string]struct{}{}
	for i, repo := range repositories {
		baseURL := strings.TrimRight(strings.TrimSpace(repo.URL), "/")
		if baseURL == "" {
			return nil, nil, fmt.Errorf("rpm repositories[%d].url is empty", i)
		}
		paths := repo.Paths
		if repo.Path != "" {
			if len(paths) != 0 {
				return nil, nil, fmt.Errorf("rpm repositories[%d] must not set both path and paths", i)
			}
			paths = []string{repo.Path}
		}
		if len(paths) == 0 {
			return nil, nil, fmt.Errorf("rpm repositories[%d].paths must not be empty", i)
		}
		if _, ok := seenUpstreams[baseURL]; !ok {
			seenUpstreams[baseURL] = struct{}{}
			upstreams = append(upstreams, baseURL)
		}
		for _, repoPath := range paths {
			repoPath = strings.Trim(strings.TrimSpace(repoPath), "/")
			if repoPath == "" {
				return nil, nil, fmt.Errorf("rpm repositories[%d] contains empty path", i)
			}
			targets = append(targets, filerepo.MetadataTarget{
				URL: path.Join(repoPath, "repodata", "repomd.xml"),
			})
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
		repomd, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[repomd.Path] = struct{}{}
		var root struct {
			Data []struct {
				Type     string `xml:"type,attr"`
				Location struct {
					Href string `xml:"href,attr"`
				} `xml:"location"`
			} `xml:"data"`
		}
		if err := xml.Unmarshal(repomd.Body, &root); err != nil {
			return nil, err
		}
		repoRoot := strings.TrimSuffix(repomd.Path, "/repodata/repomd.xml")
		foundPrimary := false
		for _, item := range root.Data {
			if item.Type != "primary" || item.Location.Href == "" {
				continue
			}
			foundPrimary = true
			primaryPath := path.Join(repoRoot, item.Location.Href)
			primary, err := session.Fetch(ctx, filerepo.MetadataTarget{URL: primaryPath})
			if err != nil {
				return nil, err
			}
			snapshot.Metadata[primary.Path] = struct{}{}
			reader, err := filerepo.OpenCompressed(primary.Body, primary.Path)
			if err != nil {
				return nil, err
			}
			err = parsePrimary(reader, snapshot)
			_ = reader.Close()
			if err != nil {
				return nil, err
			}
		}
		if !foundPrimary {
			return nil, fmt.Errorf("%s: primary metadata not found", repomd.Path)
		}
	}
	return snapshot, nil
}

func parsePrimary(input io.Reader, snapshot *filerepo.LiveSnapshot) error {
	var metadata struct {
		Packages []struct {
			Checksum string `xml:"checksum"`
			Location struct {
				Href string `xml:"href,attr"`
			} `xml:"location"`
		} `xml:"package"`
	}
	if err := xml.NewDecoder(input).Decode(&metadata); err != nil {
		return err
	}
	for _, pkg := range metadata.Packages {
		if pkg.Location.Href == "" {
			continue
		}
		snapshot.Artifacts[pkg.Location.Href] = strings.TrimSpace(pkg.Checksum)
		for _, suffix := range []string{".sig", ".asc", ".sha256", ".sha512", ".md5"} {
			snapshot.Auxiliary[pkg.Location.Href+suffix] = strings.TrimSpace(pkg.Checksum)
		}
	}
	return nil
}
