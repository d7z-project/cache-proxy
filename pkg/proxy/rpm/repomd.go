package rpm

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
)

type repomdItem struct {
	Type     string
	Location string
	Checksum string
	SumType  string
	Size     int64
}

func parseRepomdReader(reader io.Reader) ([]repomdItem, error) {
	decoder := xml.NewDecoder(io.LimitReader(reader, maxRepomdSize+1))
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
			Size string `xml:"size"`
		}
		if err := decoder.DecodeElement(&node, &start); err != nil {
			return nil, err
		}
		size := int64(-1)
		if value := strings.TrimSpace(node.Size); value != "" {
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || parsed < 0 {
				return nil, fmt.Errorf("invalid repomd size %q", value)
			}
			size = parsed
		}
		items = append(items, repomdItem{
			Type: node.Type, Location: node.Location.Href, Checksum: strings.TrimSpace(node.Checksum.Value),
			SumType: strings.TrimSpace(node.Checksum.Type), Size: size,
		})
	}
}

func verifyRepomdChecksumReader(cleanPath, sumType, expected string, expectedSize int64, reader io.Reader) error {
	if expected == "" {
		return fmt.Errorf("%s: missing repomd checksum", cleanPath)
	}
	sum, err := rpmChecksum(sumType)
	if err != nil {
		return fmt.Errorf("%s: %w", cleanPath, err)
	}
	size, err := io.Copy(sum, reader)
	if err != nil {
		return err
	}
	if expectedSize >= 0 && size != expectedSize {
		return fmt.Errorf("%s: repomd size mismatch: got %d, want %d", cleanPath, size, expectedSize)
	}
	if actual := hex.EncodeToString(sum.Sum(nil)); !strings.EqualFold(expected, actual) {
		return fmt.Errorf("%s: repomd checksum mismatch", cleanPath)
	}
	return nil
}

func rpmChecksum(sumType string) (hash.Hash, error) {
	switch strings.ToLower(strings.TrimSpace(sumType)) {
	case "sha", "sha1":
		return sha1.New(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha512":
		return sha512.New(), nil
	case "md5":
		return md5.New(), nil
	default:
		return nil, fmt.Errorf("unsupported repomd checksum type %q", sumType)
	}
}
