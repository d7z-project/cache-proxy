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

type discoverer struct{}

func (discoverer) Discover(cleanPath string) filerepo.DiscoveryResult {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	if !strings.HasSuffix(trimmed, "/repodata/repomd.xml") {
		return filerepo.DiscoveryResult{}
	}
	repoPath := strings.TrimSuffix(trimmed, "/repodata/repomd.xml")
	repoPath = strings.Trim(repoPath, "/")
	if repoPath == "" {
		return filerepo.DiscoveryResult{}
	}
	return filerepo.DiscoveryResult{
		Matched: true,
		Role:    filerepo.DiscoveryCreateRoot,
		Root: filerepo.RepositoryRoot{
			ID:              repoPath,
			Path:            repoPath,
			DisplayName:     repoPath,
			PrimaryMetadata: []string{path.Join(repoPath, "repodata", "repomd.xml")},
			Targets:         []filerepo.MetadataTarget{{URL: path.Join(repoPath, "repodata", "repomd.xml")}},
			Kind:            "rpm",
			Attributes: []filerepo.RepositoryAttribute{
				{LabelKey: "repo_path", Value: repoPath},
				{LabelKey: "primary_metadata", Value: path.Join(repoPath, "repodata", "repomd.xml")},
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
		repomd, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[repomd.Path] = filerepo.MetadataObject{Path: repomd.Path, Required: true}
		for _, companionPath := range append(filerepo.DeduceCompanions(repomd.Path), repomd.Path+".key") {
			if companion, err := session.FetchDerived(ctx, companionPath); err != nil {
				return nil, err
			} else if companion.Path != "" {
				snapshot.Metadata[companion.Path] = companion
			}
		}
		items, err := parseRepomd(repomd)
		if err != nil {
			session.Release(target)
			return nil, err
		}
		session.Release(target)
		repoRoot := strings.TrimSuffix(repomd.Path, "/repodata/repomd.xml")
		foundPrimary := false
		for _, item := range items {
			if item.Location == "" {
				continue
			}
			metadataPath := path.Join(repoRoot, item.Location)
			metaTarget := filerepo.MetadataTarget{URL: metadataPath}

			blob, err := session.Fetch(ctx, metaTarget)
			if err != nil {
				if item.Type == "primary" {
					return nil, err
				}
				continue
			}
			if err := verifyRepomdChecksum(metadataPath, item.SumType, item.Checksum, blob); err != nil {
				return nil, err
			}
			snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: item.Type == "primary"}

			if item.Type != "primary" {
				session.Release(metaTarget)
				continue
			}
			foundPrimary = true

			blobReader, err := blob.Open()
			if err != nil {
				session.Release(metaTarget)
				return nil, err
			}
			reader, err := filerepo.OpenCompressed(blobReader, metadataPath)
			if err != nil {
				session.Release(metaTarget)
				return nil, err
			}
			added, err := parsePrimary(reader, paths, repoRoot)
			artifactCount += added
			_ = reader.Close()
			session.Release(metaTarget)
			if err != nil {
				return nil, err
			}
		}
		if !foundPrimary {
			return nil, fmt.Errorf("%s: primary metadata not found", repomd.Path)
		}
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func rebuildCleanupIndex(_ context.Context, session *filerepo.LocalSession, paths *filerepo.PathIndexBuilder) error {
	for _, target := range session.Targets() {
		repomd, err := session.Fetch(target)
		if err != nil {
			return err
		}
		root, err := parseRepomd(repomd)
		if err != nil {
			repomd.Close()
			return err
		}
		repoRoot := strings.TrimSuffix(repomd.Path, "/repodata/repomd.xml")
		for _, item := range root {
			if item.Type != "primary" || item.Location == "" {
				continue
			}
			blob, err := session.Fetch(filerepo.MetadataTarget{URL: path.Join(repoRoot, item.Location)})
			if err != nil {
				repomd.Close()
				return err
			}
			blobReader, err := blob.Open()
			if err != nil {
				blob.Close()
				repomd.Close()
				return err
			}
			reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
			if err != nil {
				blob.Close()
				repomd.Close()
				return err
			}
			_, err = parsePrimary(reader, paths, repoRoot)
			_ = reader.Close()
			blob.Close()
			repomd.Close()
			return err
		}
		repomd.Close()
	}
	return nil
}

type repomdItem struct {
	Type     string
	Location string
	Checksum string
	SumType  string
}

func parseRepomd(blob filerepo.MetadataBlob) ([]repomdItem, error) {
	reader, err := blob.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	decoder := xml.NewDecoder(reader)
	var items []repomdItem
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return items, nil
		}
		if err != nil {
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "data" {
			continue
		}
		var node struct {
			Type     string `xml:"type,attr"`
			Checksum struct {
				Type  string `xml:"type,attr"`
				Value string `xml:",chardata"`
			} `xml:"checksum"`
			Location struct {
				Href string `xml:"href,attr"`
			} `xml:"location"`
		}
		if err := decoder.DecodeElement(&node, &start); err != nil {
			return nil, err
		}
		items = append(items, repomdItem{
			Type:     node.Type,
			Location: node.Location.Href,
			Checksum: strings.TrimSpace(node.Checksum.Value),
			SumType:  node.Checksum.Type,
		})
	}
}

func parsePrimary(input io.Reader, paths *filerepo.PathIndexBuilder, repoRoot string) (int, error) {
	decoder := xml.NewDecoder(input)
	count := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "package" {
			continue
		}

		href, checksum, err := parsePrimaryPackage(decoder)
		if err != nil {
			return count, err
		}
		if href == "" {
			continue
		}
		artifactPath := path.Join(repoRoot, href)
		_ = checksum
		paths.Add(artifactPath)
		paths.AddAuxiliary(artifactPath)
		count++
	}
}

func parsePrimaryPackage(decoder *xml.Decoder) (string, string, error) {
	var href string
	var checksum string
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", "", err
		}
		switch item := token.(type) {
		case xml.StartElement:
			switch item.Name.Local {
			case "location":
				for _, attr := range item.Attr {
					if attr.Name.Local == "href" {
						href = attr.Value
						break
					}
				}
			case "checksum":
				var value string
				if err := decoder.DecodeElement(&value, &item); err != nil {
					return "", "", err
				}
				checksum = value
			default:
				if err := decoder.Skip(); err != nil {
					return "", "", err
				}
			}
		case xml.EndElement:
			if item.Name.Local == "package" {
				return href, checksum, nil
			}
		}
	}
}

func verifyRepomdChecksum(path, sumType, expected string, blob filerepo.MetadataBlob) error {
	if expected == "" {
		return fmt.Errorf("%s: missing repomd checksum", path)
	}
	if sumType != "" && sumType != "sha256" {
		return nil
	}
	reader, err := blob.Open()
	if err != nil {
		return err
	}
	defer reader.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, reader); err != nil {
		return err
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("%s: repomd checksum mismatch", path)
	}
	return nil
}
