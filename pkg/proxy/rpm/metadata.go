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

type inspector struct{}

func (inspector) FinalizeRoot(root filerepo.RepositoryRoot) filerepo.RepositoryRoot {
	repoPath := root.Path
	if repoPath == "" {
		repoPath = "/"
	}
	attrs := []filerepo.RepositoryAttribute{{LabelKey: "repo_path", Value: repoPath}}
	if len(root.PrimaryMetadata) > 0 {
		attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "primary_metadata", Value: root.PrimaryMetadata[0]})
	}
	root.Attributes = attrs
	return root
}

func (inspector) InspectPath(cleanPath string) filerepo.DiscoveryResult {
	switch {
	case strings.HasPrefix(cleanPath, "repodata/"), strings.Contains(cleanPath, "/repodata/"), strings.HasSuffix(cleanPath, "/repomd.xml"), cleanPath == "repomd.xml", strings.HasSuffix(cleanPath, "/mirrorlist"), cleanPath == "mirrorlist", strings.HasSuffix(cleanPath, "/metalink"), cleanPath == "metalink":
		return analyzeMetadataPath(cleanPath)
	case strings.HasSuffix(cleanPath, ".rpm"), strings.HasSuffix(cleanPath, ".drpm"):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceArtifact, Role: filerepo.DiscoveryIgnore}
	case strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5"):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceAuxiliary, Role: filerepo.DiscoveryIgnore}
	default:
		return filerepo.DiscoveryResult{Class: filerepo.ResourceUnknown, Role: filerepo.DiscoveryIgnore}
	}
}

func analyzeMetadataPath(cleanPath string) filerepo.DiscoveryResult {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	if trimmed != "repodata/repomd.xml" && !strings.HasSuffix(trimmed, "/repodata/repomd.xml") {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}
	}
	repoPath := strings.TrimSuffix(trimmed, "/repodata/repomd.xml")
	if repoPath == trimmed {
		repoPath = ""
	}
	repoPath = strings.Trim(repoPath, "/")
	displayName := repoPath
	if displayName == "" {
		displayName = "/"
	}
	return filerepo.DiscoveryResult{
		Class: filerepo.ResourceMetadata,
		Role:  filerepo.DiscoveryCreateRoot,
		Root: filerepo.RepositoryRoot{
			ID:              filerepo.RepositoryID(filerepo.LayoutRPM, repoPath),
			Path:            repoPath,
			DisplayName:     displayName,
			Layout:          filerepo.LayoutRPM,
			PrimaryMetadata: []string{path.Join(repoPath, "repodata", "repomd.xml")},
			Targets:         []filerepo.MetadataTarget{{URL: path.Join(repoPath, "repodata", "repomd.xml")}},
		},
	}
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifactCount := 0
	for _, target := range session.Targets() {
		added, err := buildRepomdTarget(ctx, session, snapshot, target, paths)
		if err != nil {
			return nil, err
		}
		artifactCount += added
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func buildRepomdTarget(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	target filerepo.MetadataTarget,
	paths *filerepo.PathIndexBuilder,
) (int, error) {
	repomd, err := session.Fetch(ctx, target)
	if err != nil {
		return 0, err
	}
	defer session.Release(target)

	snapshot.Metadata[repomd.Path] = filerepo.MetadataObject{Path: repomd.Path, Required: true}
	for _, companionPath := range append(filerepo.DeduceCompanions(repomd.Path), repomd.Path+".key") {
		companion, err := session.FetchDerived(ctx, companionPath)
		if err != nil {
			return 0, err
		}
		if companion.Path != "" {
			snapshot.Metadata[companion.Path] = companion
		}
	}
	items, err := parseRepomd(repomd)
	if err != nil {
		return 0, err
	}

	repoRoot := strings.TrimSuffix(repomd.Path, "/repodata/repomd.xml")
	foundPrimary := false
	artifactCount := 0
	for _, item := range items {
		if item.Location == "" {
			continue
		}
		added, err := buildRepomdItem(ctx, session, snapshot, item, paths, repoRoot)
		if err != nil {
			return 0, err
		}
		if item.Type == "primary" {
			foundPrimary = true
			artifactCount += added
		}
	}
	if !foundPrimary {
		return 0, fmt.Errorf("%s: primary metadata not found", repomd.Path)
	}
	return artifactCount, nil
}

func buildRepomdItem(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	item repomdItem,
	paths *filerepo.PathIndexBuilder,
	repoRoot string,
) (int, error) {
	metadataPath := path.Join(repoRoot, item.Location)
	metaTarget := filerepo.MetadataTarget{URL: metadataPath}
	blob, err := session.Fetch(ctx, metaTarget)
	if err != nil {
		if item.Type != "primary" {
			return 0, nil
		}
		return 0, err
	}
	defer session.Release(metaTarget)

	if err := verifyRepomdChecksum(metadataPath, item.SumType, item.Checksum, blob); err != nil {
		return 0, err
	}
	snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: item.Type == "primary"}
	if item.Type != "primary" {
		return 0, nil
	}

	blobReader, err := blob.Open()
	if err != nil {
		return 0, err
	}
	reader, err := filerepo.OpenCompressed(blobReader, metadataPath)
	if err != nil {
		_ = blobReader.Close()
		return 0, err
	}
	defer reader.Close()
	return parsePrimary(reader, paths, repoRoot)
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
