package flatpak

import (
	"bufio"
	"compress/flate"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/foundriesio/ostreeuploader/pkg/gvariant"
)

const (
	ostreeFileHeaderFormat = "(tuuuusa(ayay))"
	maxOSTreeHeaderSize    = 16 << 20
	maxOSTreeFileSize      = int64(2 << 30)
)

func verifyOSTreeFileObject(source io.Reader, expectedDigest string) error {
	var prefix [8]byte
	if _, err := io.ReadFull(source, prefix[:]); err != nil {
		return fmt.Errorf("read ostree file header: %w", err)
	}
	headerSize := binary.BigEndian.Uint32(prefix[:4])
	if headerSize == 0 || headerSize > maxOSTreeHeaderSize {
		return fmt.Errorf("invalid ostree file header size %d", headerSize)
	}
	if prefix[4] != 0 || prefix[5] != 0 || prefix[6] != 0 || prefix[7] != 0 {
		return errors.New("invalid ostree file header padding")
	}
	headerBytes := make([]byte, headerSize)
	if _, err := io.ReadFull(source, headerBytes); err != nil {
		return fmt.Errorf("read ostree file header: %w", err)
	}
	header, err := gvariant.New(headerBytes, ostreeFileHeaderFormat)
	if err != nil {
		return fmt.Errorf("decode ostree file header: %w", err)
	}
	declaredSizeBytes := header.Child(0).Raw()
	if len(declaredSizeBytes) != 8 {
		return errors.New("invalid ostree file size field")
	}
	declaredSize := binary.BigEndian.Uint64(declaredSizeBytes)
	if declaredSize > uint64(maxOSTreeFileSize) {
		return fmt.Errorf("ostree file exceeds %d bytes", maxOSTreeFileSize)
	}
	readUint32 := func(index int) uint32 {
		value := header.Child(index).Raw()
		if len(value) != 4 {
			return 0
		}
		return binary.BigEndian.Uint32(value)
	}
	uid, gid, mode := readUint32(1), readUint32(2), readUint32(3)
	symlinkTarget := header.Child(5).Str()
	xattrsValue := header.Child(6)
	xattrs := make([]gvariant.Xattr, 0, xattrsValue.Len())
	for index := 0; index < xattrsValue.Len(); index++ {
		entry := xattrsValue.At(index)
		name := entry.Child(0).Bytes()
		if len(name) == 0 || name[len(name)-1] != 0 {
			return errors.New("invalid ostree xattr name")
		}
		xattrs = append(xattrs, gvariant.Xattr{Name: append([]byte(nil), name[:len(name)-1]...), Value: append([]byte(nil), entry.Child(1).Bytes()...)})
	}
	if err := header.Err(); err != nil {
		return fmt.Errorf("decode ostree file header: %w", err)
	}
	fileType := mode & 0o170000
	if fileType != 0o100000 && fileType != 0o120000 {
		return fmt.Errorf("unsupported ostree file mode %o", mode)
	}
	if fileType == 0o100000 && symlinkTarget != "" || fileType == 0o120000 && symlinkTarget == "" || fileType == 0o120000 && declaredSize != 0 {
		return errors.New("inconsistent ostree file header")
	}

	checksumHeader := gvariant.EncodeFileHeader(uid, gid, mode, symlinkTarget, xattrs)
	digest := sha256.New()
	var checksumPrefix [8]byte
	binary.BigEndian.PutUint32(checksumPrefix[:4], uint32(len(checksumHeader)))
	_, _ = digest.Write(checksumPrefix[:])
	_, _ = digest.Write(checksumHeader)
	if fileType == 0o100000 {
		buffered := bufio.NewReader(source)
		inflated := flate.NewReader(buffered)
		written, copyErr := io.Copy(digest, io.LimitReader(inflated, int64(declaredSize)+1))
		closeErr := inflated.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return fmt.Errorf("inflate ostree file content: %w", err)
		}
		if written != int64(declaredSize) {
			return fmt.Errorf("ostree file size mismatch: got %d, want %d", written, declaredSize)
		}
		if _, err := buffered.ReadByte(); err != io.EOF {
			if err == nil {
				err = errors.New("trailing data")
			}
			return fmt.Errorf("invalid ostree file object trailer: %w", err)
		}
	} else {
		var trailing [1]byte
		if n, err := source.Read(trailing[:]); n != 0 || err != io.EOF {
			if err == nil {
				err = errors.New("trailing data")
			}
			return fmt.Errorf("invalid ostree symlink object trailer: %w", err)
		}
	}
	if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), expectedDigest) {
		return errors.New("ostree file object digest mismatch")
	}
	return nil
}
