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

type inspector struct{}

func (inspector) FinalizeRoot(root filerepo.RepositoryRoot) filerepo.RepositoryRoot {
	repoPath := root.Path
	if repoPath == "" {
		repoPath = "/"
	}
	root.Attributes = []filerepo.RepositoryAttribute{{LabelKey: "repo_path", Value: repoPath}}
	return root
}

func (inspector) InspectPath(cleanPath string) filerepo.DiscoveryResult {
	switch {
	case strings.HasSuffix(cleanPath, "/APKINDEX.tar.gz"), cleanPath == "APKINDEX.tar.gz":
		return analyzeMetadataPath(cleanPath)
	case strings.HasSuffix(cleanPath, "/APKINDEX.tar.gz.sig"), cleanPath == "APKINDEX.tar.gz.sig", strings.HasSuffix(cleanPath, ".apk.sig"), strings.HasSuffix(cleanPath, ".apk.asc"), strings.HasSuffix(cleanPath, ".apk.sha256"), strings.HasSuffix(cleanPath, ".apk.sha512"):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceAuxiliary, Role: filerepo.DiscoveryIgnore}
	case strings.HasSuffix(cleanPath, ".apk"):
		return filerepo.DiscoveryResult{Class: filerepo.ResourceArtifact, Role: filerepo.DiscoveryIgnore}
	default:
		return filerepo.DiscoveryResult{Class: filerepo.ResourceUnknown, Role: filerepo.DiscoveryIgnore}
	}
}

func analyzeMetadataPath(cleanPath string) filerepo.DiscoveryResult {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	if path.Base(trimmed) != "APKINDEX.tar.gz" {
		return filerepo.DiscoveryResult{Class: filerepo.ResourceMetadata, Role: filerepo.DiscoveryIgnore}
	}
	rootPath := strings.Trim(strings.TrimSpace(path.Dir(trimmed)), "/")
	if rootPath == "." {
		rootPath = ""
	}
	displayName := rootPath
	if displayName == "" {
		displayName = "/"
	}
	return filerepo.DiscoveryResult{
		Class: filerepo.ResourceMetadata,
		Role:  filerepo.DiscoveryCreateRoot,
		Root: filerepo.RepositoryRoot{
			ID:              filerepo.RepositoryID(filerepo.LayoutAPK, rootPath),
			Path:            rootPath,
			DisplayName:     displayName,
			Layout:          filerepo.LayoutAPK,
			PrimaryMetadata: []string{path.Join(rootPath, "APKINDEX.tar.gz")},
			Targets:         []filerepo.MetadataTarget{{URL: path.Join(rootPath, "APKINDEX.tar.gz")}},
		},
	}
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifacts := 0
	for _, target := range session.Targets() {
		blob, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: true}
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
			if path.Base(header.Name) != "APKINDEX" {
				continue
			}
			found = true
			n, err := parseIndex(path.Dir(blob.Path), tarReader, paths)
			if err != nil {
				_ = reader.Close()
				session.Release(target)
				return nil, err
			}
			artifacts += n
			break
		}
		_ = reader.Close()
		session.Release(target)
		if !found {
			return nil, fmt.Errorf("%s: APKINDEX entry not found", blob.Path)
		}
	}
	snapshot.ArtifactCount = artifacts
	return snapshot, nil
}

func parseIndex(basePath string, input io.Reader, paths *filerepo.PathIndexBuilder) (int, error) {
	var name string
	var version string
	count := 0
	flush := func() {
		if name == "" || version == "" {
			return
		}
		artifactPath := path.Join(basePath, name+"-"+version+".apk")
		paths.Add(artifactPath)
		paths.AddAuxiliary(artifactPath)
		count++
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(nil, 10<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			name, version = "", ""
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
		}
	}
	flush()
	return count, scanner.Err()
}
