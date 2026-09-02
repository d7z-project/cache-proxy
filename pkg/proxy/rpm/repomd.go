package rpm

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
)

const maxRepomdSize = 16 << 20
const maxRepomdItems = 4096
const maxRepomdTokens = 1 << 20
const maxRepomdDepth = 64
const maxRepomdFieldSize = 4096

type repomdItem struct {
	Type         string `json:"type"`
	Location     string `json:"location"`
	Checksum     string `json:"checksum"`
	SumType      string `json:"checksum_type"`
	Size         int64  `json:"size"`
	OpenChecksum string `json:"open_checksum,omitempty"`
	OpenSumType  string `json:"open_checksum_type,omitempty"`
	OpenSize     int64  `json:"open_size,omitempty"`
}

func parseRepomdReader(ctx context.Context, reader io.Reader) ([]repomdItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	decoder := xml.NewDecoder(io.LimitReader(reader, maxRepomdSize+1))
	var items []repomdItem
	var node *struct {
		item                         repomdItem
		checksum, openChecksum, size strings.Builder
		openSize                     strings.Builder
	}
	var text *strings.Builder
	textElement := ""
	textDepth := 0
	dataDepth := 0
	depth := 0
	tokens := 0
	rootSeen := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			switch {
			case decoder.InputOffset() > maxRepomdSize:
				return nil, fmt.Errorf("repomd exceeds %d bytes", maxRepomdSize)
			case !rootSeen:
				return nil, fmt.Errorf("repomd root element is missing")
			case len(items) == 0:
				return nil, fmt.Errorf("repomd contains no data entries")
			default:
				return items, nil
			}
		}
		if err != nil {
			return nil, err
		}
		tokens++
		if tokens > maxRepomdTokens {
			return nil, fmt.Errorf("repomd contains more than %d XML tokens", maxRepomdTokens)
		}
		if tokens&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		switch value := token.(type) {
		case xml.StartElement:
			depth++
			if depth > maxRepomdDepth {
				return nil, fmt.Errorf("repomd XML nesting exceeds %d elements", maxRepomdDepth)
			}
			if !rootSeen {
				if depth != 1 || value.Name.Local != "repomd" {
					return nil, fmt.Errorf("unexpected repomd root element %q", value.Name.Local)
				}
				rootSeen = true
				continue
			}
			if text != nil && depth > textDepth {
				return nil, fmt.Errorf("repomd %s contains nested XML", textElement)
			}
			if node == nil {
				if depth != 2 || value.Name.Local != "data" {
					continue
				}
				if len(items) >= maxRepomdItems {
					return nil, fmt.Errorf("repomd contains more than %d data entries", maxRepomdItems)
				}
				node = &struct {
					item                         repomdItem
					checksum, openChecksum, size strings.Builder
					openSize                     strings.Builder
				}{}
				dataDepth = depth
				for _, attribute := range value.Attr {
					if attribute.Name.Local == "type" {
						node.item.Type = attribute.Value
					}
				}
				continue
			}
			if depth != dataDepth+1 {
				continue
			}
			text, textElement, textDepth = nil, value.Name.Local, depth
			switch value.Name.Local {
			case "checksum":
				text = &node.checksum
				for _, attribute := range value.Attr {
					if attribute.Name.Local == "type" {
						node.item.SumType = attribute.Value
					}
				}
			case "open-checksum":
				text = &node.openChecksum
				for _, attribute := range value.Attr {
					if attribute.Name.Local == "type" {
						node.item.OpenSumType = attribute.Value
					}
				}
			case "location":
				for _, attribute := range value.Attr {
					if attribute.Name.Local == "href" {
						node.item.Location = attribute.Value
					}
				}
			case "size":
				text = &node.size
			case "open-size":
				text = &node.openSize
			}
		case xml.CharData:
			if text != nil {
				if text.Len()+len(value) > maxRepomdFieldSize {
					return nil, fmt.Errorf("repomd %s exceeds %d bytes", textElement, maxRepomdFieldSize)
				}
				_, _ = text.Write(value)
			}
		case xml.EndElement:
			if text != nil && depth == textDepth && value.Name.Local == textElement {
				text, textElement, textDepth = nil, "", 0
			}
			if node != nil && depth == dataDepth && value.Name.Local == "data" {
				node.item.Checksum = strings.TrimSpace(node.checksum.String())
				node.item.OpenChecksum = strings.TrimSpace(node.openChecksum.String())
				node.item.Type = strings.TrimSpace(node.item.Type)
				node.item.Location = strings.TrimSpace(node.item.Location)
				node.item.SumType = strings.TrimSpace(node.item.SumType)
				node.item.OpenSumType = strings.TrimSpace(node.item.OpenSumType)
				node.item.Size = -1
				if field := strings.TrimSpace(node.size.String()); field != "" {
					parsed, parseErr := strconv.ParseInt(field, 10, 64)
					if parseErr != nil || parsed < 0 {
						return nil, fmt.Errorf("invalid repomd size %q", field)
					}
					node.item.Size = parsed
				}
				node.item.OpenSize = -1
				if field := strings.TrimSpace(node.openSize.String()); field != "" {
					parsed, parseErr := strconv.ParseInt(field, 10, 64)
					if parseErr != nil || parsed < 0 {
						return nil, fmt.Errorf("invalid repomd open-size %q", field)
					}
					node.item.OpenSize = parsed
				}
				if node.item.Type == "" || node.item.Location == "" || node.item.Checksum == "" {
					return nil, fmt.Errorf("repomd data entry is missing type, location, or checksum")
				}
				items = append(items, node.item)
				node = nil
				dataDepth = 0
			}
			depth--
		}
	}
}

func rpmChecksum(sumType string) (hash.Hash, error) {
	switch strings.ToLower(strings.TrimSpace(sumType)) {
	case "sha", "sha1":
		return sha1.New(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha224":
		return sha256.New224(), nil
	case "sha384":
		return sha512.New384(), nil
	case "sha512":
		return sha512.New(), nil
	case "md5":
		return md5.New(), nil
	default:
		return nil, fmt.Errorf("unsupported repomd checksum type %q", sumType)
	}
}
