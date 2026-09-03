package flatpak

import (
	"bytes"
	"compress/flate"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/foundriesio/ostreeuploader/pkg/gvariant"
	"github.com/stretchr/testify/require"
)

func TestVerifyOSTreeFileObject(t *testing.T) {
	content := []byte("verified ostree content\n")
	header := gvariant.EncodeFileHeader(0, 0, 0o100644, "", nil)
	checksumInput := make([]byte, 8+len(header)+len(content))
	binary.BigEndian.PutUint32(checksumInput[:4], uint32(len(header)))
	copy(checksumInput[8:], header)
	copy(checksumInput[8+len(header):], content)
	digest := sha256.Sum256(checksumInput)

	compressedHeader := encodeTestFilezHeader(uint64(len(content)), 0, 0, 0o100644)
	var object bytes.Buffer
	require.NoError(t, binary.Write(&object, binary.BigEndian, uint32(len(compressedHeader))))
	object.Write(make([]byte, 4))
	object.Write(compressedHeader)
	compressor, err := flate.NewWriter(&object, flate.DefaultCompression)
	require.NoError(t, err)
	_, err = compressor.Write(content)
	require.NoError(t, err)
	require.NoError(t, compressor.Close())

	require.NoError(t, verifyOSTreeFileObject(bytes.NewReader(object.Bytes()), hex.EncodeToString(digest[:])))
	withTrailer := append(append([]byte(nil), object.Bytes()...), 0)
	require.ErrorContains(t, verifyOSTreeFileObject(bytes.NewReader(withTrailer), hex.EncodeToString(digest[:])), "trailer")
	corrupt := append([]byte(nil), object.Bytes()...)
	corrupt[len(corrupt)-1] ^= 0xff
	require.Error(t, verifyOSTreeFileObject(bytes.NewReader(corrupt), hex.EncodeToString(digest[:])))
}

// This is the fixed regular-file subset of OSTree's (tuuuusa(ayay)) header.
func encodeTestFilezHeader(size uint64, uid, gid, mode uint32) []byte {
	header := make([]byte, 26)
	binary.BigEndian.PutUint64(header[:8], size)
	binary.BigEndian.PutUint32(header[8:12], uid)
	binary.BigEndian.PutUint32(header[12:16], gid)
	binary.BigEndian.PutUint32(header[16:20], mode)
	// rdev occupies 20:24; the empty symlink string is byte 24. The final
	// framing offset points past that string; the xattr array is empty.
	header[25] = 25
	return header
}
