package rpm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
		Metadata:  map[string]filerepo.MetadataObject{},
		Artifacts: map[string]filerepo.RepoObject{},
		Auxiliary: map[string]filerepo.RepoObject{},
	}
	for _, target := range session.Targets() {
		repomd, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[repomd.Path] = filerepo.MetadataObject{Path: repomd.Path, Required: true}
		ascPath := repomd.Path + ".asc"
		if _, err := session.Fetch(ctx, filerepo.MetadataTarget{URL: ascPath}); err != nil {
			return nil, err
		}
		snapshot.Metadata[ascPath] = filerepo.MetadataObject{Path: ascPath, Required: true}
		var root struct {
			Data []struct {
				Type     string `xml:"type,attr"`
				Checksum struct {
					Type  string `xml:"type,attr"`
					Value string `xml:",chardata"`
				} `xml:"checksum"`
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
			itemHref := item.Location.Href
			if itemHref == "" {
				continue
			}
			metadataPath := path.Join(repoRoot, itemHref)
			snapshot.Metadata[metadataPath] = filerepo.MetadataObject{Path: metadataPath, Required: item.Type == "primary"}

			blob, err := session.Fetch(ctx, filerepo.MetadataTarget{URL: metadataPath})
			if err != nil {
				if item.Type == "primary" {
					return nil, err
				}
				continue
			}
			if err := verifyRepomdChecksum(metadataPath, item.Checksum.Type, strings.TrimSpace(item.Checksum.Value), blob.Body); err != nil {
				return nil, err
			}
			snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: item.Type == "primary"}

			if item.Type != "primary" {
				continue
			}
			foundPrimary = true

			reader, err := filerepo.OpenCompressed(blob.Body, metadataPath)
			if err != nil {
				return nil, err
			}
			err = parsePrimary(reader, snapshot, repoRoot)
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

func parsePrimary(input io.Reader, snapshot *filerepo.LiveSnapshot, repoRoot string) error {
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
		artifactPath := path.Join(repoRoot, pkg.Location.Href)
		identity := strings.TrimSpace(pkg.Checksum)
		snapshot.Artifacts[artifactPath] = filerepo.RepoObject{Path: artifactPath, Identity: identity}
		for _, suffix := range []string{".sig", ".asc", ".sha256", ".sha512", ".md5"} {
			auxPath := artifactPath + suffix
			snapshot.Auxiliary[auxPath] = filerepo.RepoObject{Path: auxPath, Identity: identity}
		}
	}
	return nil
}

func verifyRepomdChecksum(path, sumType, expected string, body []byte) error {
	if expected == "" {
		return fmt.Errorf("%s: missing repomd checksum", path)
	}
	if sumType != "" && sumType != "sha256" {
		return nil
	}
	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("%s: repomd checksum mismatch", path)
	}
	return nil
}
