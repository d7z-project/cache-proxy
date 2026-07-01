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

type rootSpec struct {
	Branch string
	Repo   string
	Arch   string
}

func (s *rootSpec) Key() string {
	return strings.Join([]string{s.Branch, s.Repo, s.Arch}, "|")
}

func (s *rootSpec) SubPath() string {
	return s.Branch + "/" + s.Repo + "/" + s.Arch
}

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	return []filerepo.MetadataTarget{{URL: path.Join(s.Branch, s.Repo, s.Arch, "APKINDEX.tar.gz")}}
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	return false
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 || parts[3] != "APKINDEX.tar.gz" {
		return nil, false
	}
	for _, part := range parts[:3] {
		if part == "" {
			return nil, false
		}
	}
	return &rootSpec{Branch: parts[0], Repo: parts[1], Arch: parts[2]}, true
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
			if path.Base(header.Name) != "APKINDEX" {
				continue
			}
			_, err = parseIndex(path.Dir(blob.Path), tarReader, paths)
			_ = reader.Close()
			blob.Close()
			if err != nil {
				return err
			}
			break
		}
		_ = reader.Close()
		blob.Close()
	}
	return nil
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
