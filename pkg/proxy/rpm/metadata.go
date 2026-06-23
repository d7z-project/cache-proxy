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

type rootSpec struct {
	RepoPath string
}

func (s *rootSpec) Key() string { return s.RepoPath }

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	return []filerepo.MetadataTarget{{URL: path.Join(s.RepoPath, "repodata", "repomd.xml")}}
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	return false
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	if !strings.HasSuffix(trimmed, "/repodata/repomd.xml") {
		return nil, false
	}
	repoPath := strings.TrimSuffix(trimmed, "/repodata/repomd.xml")
	repoPath = strings.Trim(repoPath, "/")
	if repoPath == "" {
		return nil, false
	}
	return &rootSpec{RepoPath: repoPath}, true
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
		snapshot.Metadata[repomd.Path+".asc"] = struct{}{}
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
			if item.Location.Href == "" {
				continue
			}
			metadataPath := path.Join(repoRoot, item.Location.Href)
			snapshot.Metadata[metadataPath] = struct{}{}
			if item.Type != "primary" {
				continue
			}
			foundPrimary = true
			primary, err := session.Fetch(ctx, filerepo.MetadataTarget{URL: metadataPath})
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
