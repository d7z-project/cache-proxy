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

var (
	pacmanDBSuffixes = []string{
		".db", ".db.tar", ".db.tar.bz2", ".db.tar.gz", ".db.tar.lrz",
		".db.tar.lz", ".db.tar.lz4", ".db.tar.lzo", ".db.tar.xz", ".db.tar.zst", ".db.tar.Z",
	}
	pacmanFilesSuffixes = []string{
		".files", ".files.tar", ".files.tar.bz2", ".files.tar.gz", ".files.tar.lrz",
		".files.tar.lz", ".files.tar.lz4", ".files.tar.lzo", ".files.tar.xz", ".files.tar.zst", ".files.tar.Z",
	}
)

type inspector struct{}

func (inspector) FinalizeRoot(root filerepo.RepositoryRoot) filerepo.RepositoryRoot {
	repoPath := root.Path
	if repoPath == "" {
		repoPath = "/"
	}
	attrs := []filerepo.RepositoryAttribute{{LabelKey: "repo_path", Value: repoPath}}
	if root.Repo != "" {
		attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "repository", Value: root.Repo})
	}
	if root.Arch != "" {
		attrs = append(attrs, filerepo.RepositoryAttribute{LabelKey: "architecture", Value: root.Arch})
	}
	root.Attributes = attrs
	return root
}

func (inspector) InspectPath(cleanPath string) filerepo.DiscoveryResult {
	switch {
	case pacmanDBName(path.Base(cleanPath)) != "":
		return analyzeMetadataPath(cleanPath)
	case pacmanFilesName(path.Base(cleanPath)) != "", pacmanDBSignatureName(path.Base(cleanPath)) != "", pacmanFilesSignatureName(path.Base(cleanPath)) != "":
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}
	case strings.Contains(cleanPath, ".pkg.tar.") && strings.HasSuffix(cleanPath, ".sig"):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceAuxiliary, Role: filerepo.DiscoveryIgnore}
	case strings.Contains(cleanPath, ".pkg.tar."):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceArtifact, Role: filerepo.DiscoveryIgnore}
	case strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceAuxiliary, Role: filerepo.DiscoveryIgnore}
	default:
		return filerepo.DiscoveryResult{Class: filerepo.ResourceUnknown, Role: filerepo.DiscoveryIgnore}
	}
}

func analyzeMetadataPath(cleanPath string) filerepo.DiscoveryResult {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	fileName := path.Base(trimmed)
	dbName := pacmanDBName(fileName)
	if dbName == "" {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}
	}
	rootPath := strings.Trim(strings.TrimSpace(path.Dir(trimmed)), "/")
	if rootPath == "." {
		rootPath = ""
	}
	displayName := dbName
	if rootPath != "" {
		displayName = path.Join(rootPath, dbName)
	}
	return filerepo.DiscoveryResult{
		Class: filerepo.ResourceMetadata,
		Role:  filerepo.DiscoveryCreateRoot,
		Root: filerepo.RepositoryRoot{
			ID:              filerepo.RepositoryID(filerepo.LayoutPacman, rootPath),
			Path:            rootPath,
			DisplayName:     displayName,
			Layout:          filerepo.LayoutPacman,
			PrimaryMetadata: []string{path.Join(rootPath, fileName)},
			Targets: []filerepo.MetadataTarget{{
				URL:  path.Join(rootPath, fileName),
				Repo: dbName,
			}},
			Repo: dbName,
		},
	}
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifactCount := 0
	for _, target := range session.Targets() {
		count, found, err := buildDatabaseTarget(ctx, session, snapshot, target, paths)
		if err != nil {
			return nil, err
		}
		artifactCount += count
		if !found {
			snapshot.ArtifactCount = artifactCount
			return snapshot, nil
		}
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func buildDatabaseTarget(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	target filerepo.MetadataTarget,
	paths *filerepo.PathIndexBuilder,
) (int, bool, error) {
	blob, err := session.Fetch(ctx, target)
	if err != nil {
		return 0, false, err
	}
	defer session.Release(target)

	snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: true}
	if err := addDatabaseCompanions(ctx, session, snapshot, blob.Path); err != nil {
		return 0, false, err
	}

	blobReader, err := blob.Open()
	if err != nil {
		return 0, false, err
	}
	reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
	if err != nil {
		_ = blobReader.Close()
		return 0, false, err
	}
	defer reader.Close()

	tarReader := tar.NewReader(reader)
	var count int
	found := false
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return count, found, nil
		}
		if err != nil {
			return count, false, err
		}
		if path.Base(header.Name) != "desc" {
			continue
		}
		found = true
		filename, err := parseDesc(tarReader)
		if err != nil {
			return count, false, err
		}
		if filename == "" {
			continue
		}
		artifactPath := path.Join(path.Dir(blob.Path), filename)
		paths.Add(artifactPath)
		paths.AddAuxiliary(artifactPath)
		count++
	}
}

func addDatabaseCompanions(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	blobPath string,
) error {
	dbPath := pacmanDBBase(blobPath)
	for _, suffix := range []string{".files", ".files.sig"} {
		if err := addDerivedMetadata(ctx, session, snapshot, dbPath+suffix); err != nil {
			return err
		}
	}
	if archiveExt := pacmanDBArchiveExt(blobPath); archiveExt != "" {
		for _, suffix := range []string{".files" + archiveExt, ".files" + archiveExt + ".sig"} {
			if err := addDerivedMetadata(ctx, session, snapshot, dbPath+suffix); err != nil {
				return err
			}
		}
	}
	for _, companionPath := range filerepo.DeduceCompanions(blobPath) {
		if err := addDerivedMetadata(ctx, session, snapshot, companionPath); err != nil {
			return err
		}
	}
	return nil
}

func addDerivedMetadata(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	metadataPath string,
) error {
	companion, err := session.FetchDerived(ctx, metadataPath)
	if err != nil {
		return err
	}
	if companion.Path != "" {
		snapshot.Metadata[companion.Path] = companion
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

func pacmanDBName(fileName string) string {
	for _, suffix := range pacmanDBSuffixes {
		if strings.HasSuffix(fileName, suffix) {
			return strings.TrimSuffix(fileName, suffix)
		}
	}
	return ""
}

func pacmanDBBase(cleanPath string) string {
	fileName := path.Base(cleanPath)
	name := pacmanDBName(fileName)
	if name == "" {
		return strings.TrimSuffix(cleanPath, ".db")
	}
	dir := path.Dir(cleanPath)
	if dir == "." {
		return name
	}
	return path.Join(dir, name)
}

func pacmanDBArchiveExt(cleanPath string) string {
	fileName := path.Base(cleanPath)
	for _, suffix := range pacmanDBSuffixes[1:] {
		if strings.HasSuffix(fileName, suffix) {
			return strings.TrimPrefix(suffix, ".db")
		}
	}
	return ""
}

func pacmanFilesName(fileName string) string {
	for _, suffix := range pacmanFilesSuffixes {
		if strings.HasSuffix(fileName, suffix) {
			return strings.TrimSuffix(fileName, suffix)
		}
	}
	return ""
}

func pacmanDBSignatureName(fileName string) string {
	if !strings.HasSuffix(fileName, ".sig") {
		return ""
	}
	return pacmanDBName(strings.TrimSuffix(fileName, ".sig"))
}

func pacmanFilesSignatureName(fileName string) string {
	if !strings.HasSuffix(fileName, ".sig") {
		return ""
	}
	return pacmanFilesName(strings.TrimSuffix(fileName, ".sig"))
}
