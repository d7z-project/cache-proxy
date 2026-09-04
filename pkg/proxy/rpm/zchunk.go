package rpm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
)

const (
	maxZchunkHeaderSize     = 64 << 20
	maxZchunkDictionarySize = 64 << 20
	maxZchunkChunks         = 1_000_000
)

type zchunkPart struct {
	compressedSize   int64
	uncompressedSize int64
	stream           uint64
	storedRaw        bool
}

type zchunkHeader struct {
	compression            uint64
	dictionaryCompressed   uint64
	dictionaryUncompressed uint64
	parts                  []zchunkPart
}

func decompressZchunk(ctx context.Context, source io.Reader, destination io.Writer, maxOutput int64) (int64, error) {
	if maxOutput < 0 {
		return 0, errors.New("zchunk output limit is invalid")
	}
	header, err := readZchunkHeader(source)
	if err != nil {
		return 0, err
	}
	compression := header.compression
	dictionaryCompressed := header.dictionaryCompressed
	dictionaryUncompressed := header.dictionaryUncompressed
	parts := header.parts

	dictionaryWire := make([]byte, int(dictionaryCompressed))
	if _, err := io.ReadFull(source, dictionaryWire); err != nil {
		return 0, err
	}
	var dictionary []byte
	if dictionaryUncompressed != 0 {
		buffer := bytes.NewBuffer(make([]byte, 0, int(dictionaryUncompressed)))
		if compression == 0 {
			if dictionaryCompressed != dictionaryUncompressed {
				return 0, errors.New("invalid uncompressed zchunk dictionary size")
			}
			_, _ = buffer.Write(dictionaryWire)
		} else if err := decompressZstdPart(ctx, bytes.NewReader(dictionaryWire), buffer, int64(dictionaryUncompressed), nil); err != nil {
			return 0, fmt.Errorf("decompress zchunk dictionary: %w", err)
		}
		dictionary = buffer.Bytes()
	} else if dictionaryCompressed != 0 {
		return 0, errors.New("invalid empty zchunk dictionary")
	}

	var written int64
	for _, part := range parts {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		chunk := &io.LimitedReader{R: source, N: part.compressedSize}
		if part.stream != 1 {
			if _, err := io.Copy(io.Discard, &contextReader{ctx: ctx, reader: chunk}); err != nil {
				return 0, err
			}
			continue
		}
		if part.uncompressedSize > maxOutput-written {
			return 0, fmt.Errorf("zchunk output exceeds %d bytes", maxOutput)
		}
		var decodeErr error
		switch {
		case compression == 0 || part.storedRaw:
			if part.compressedSize != part.uncompressedSize {
				return 0, errors.New("invalid uncompressed zchunk chunk size")
			}
			_, decodeErr = io.Copy(destination, &contextReader{ctx: ctx, reader: chunk})
		default:
			decodeErr = decompressZstdPart(ctx, chunk, destination, part.uncompressedSize, dictionary)
		}
		if decodeErr != nil {
			return 0, fmt.Errorf("decompress zchunk data: %w", decodeErr)
		}
		if chunk.N != 0 {
			return 0, errors.New("zchunk chunk has trailing compressed data")
		}
		written += part.uncompressedSize
	}
	var trailing [1]byte
	if n, err := source.Read(trailing[:]); n != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing data")
		}
		return 0, fmt.Errorf("invalid zchunk body: %w", err)
	}
	return written, nil
}

func readZchunkHeader(source io.Reader) (zchunkHeader, error) {
	var magic [5]byte
	if _, err := io.ReadFull(source, magic[:]); err != nil || magic != [5]byte{0, 'Z', 'C', 'K', '1'} {
		return zchunkHeader{}, errors.New("invalid zchunk magic")
	}
	overallChecksumType, err := readZchunkInteger(source)
	if err != nil {
		return zchunkHeader{}, err
	}
	overallChecksumSize, err := zchunkChecksumSize(overallChecksumType)
	if err != nil {
		return zchunkHeader{}, err
	}
	headerSize, err := readZchunkInteger(source)
	if err != nil || headerSize > maxZchunkHeaderSize {
		return zchunkHeader{}, fmt.Errorf("invalid zchunk header size %d", headerSize)
	}
	if err := discardZchunkBytes(source, uint64(overallChecksumSize)); err != nil {
		return zchunkHeader{}, err
	}
	headerReader := &io.LimitedReader{R: source, N: int64(headerSize)}
	if err := discardZchunkBytes(headerReader, uint64(overallChecksumSize)); err != nil {
		return zchunkHeader{}, err
	}
	flags, err := readZchunkInteger(headerReader)
	if err != nil || flags > 0b111 {
		return zchunkHeader{}, fmt.Errorf("invalid zchunk flags %d", flags)
	}
	compression, err := readZchunkInteger(headerReader)
	if err != nil || compression != 0 && compression != 2 {
		return zchunkHeader{}, fmt.Errorf("unsupported zchunk compression %d", compression)
	}
	if flags&0b10 != 0 {
		optionalCount, err := readZchunkInteger(headerReader)
		if err != nil || optionalCount == 0 || optionalCount > 1024 {
			return zchunkHeader{}, errors.New("invalid zchunk optional element count")
		}
		for range optionalCount {
			if _, err := readZchunkInteger(headerReader); err != nil {
				return zchunkHeader{}, err
			}
			size, err := readZchunkInteger(headerReader)
			if err != nil || size > uint64(headerReader.N) {
				return zchunkHeader{}, errors.New("invalid zchunk optional element size")
			}
			if err := discardZchunkBytes(headerReader, size); err != nil {
				return zchunkHeader{}, err
			}
		}
	}
	indexSize, err := readZchunkInteger(headerReader)
	if err != nil || indexSize > uint64(headerReader.N) {
		return zchunkHeader{}, errors.New("invalid zchunk index size")
	}
	index := &io.LimitedReader{R: headerReader, N: int64(indexSize)}
	chunkChecksumType, err := readZchunkInteger(index)
	if err != nil {
		return zchunkHeader{}, err
	}
	chunkChecksumSize, err := zchunkChecksumSize(chunkChecksumType)
	if err != nil {
		return zchunkHeader{}, err
	}
	chunkCount, err := readZchunkInteger(index)
	if err != nil || chunkCount == 0 || chunkCount > maxZchunkChunks {
		return zchunkHeader{}, fmt.Errorf("invalid zchunk chunk count %d", chunkCount)
	}
	if flags&0b1 != 0 {
		stream, err := readZchunkInteger(index)
		if err != nil || stream != 0 {
			return zchunkHeader{}, errors.New("invalid zchunk dictionary stream")
		}
	}
	if err := discardZchunkBytes(index, uint64(chunkChecksumSize)); err != nil {
		return zchunkHeader{}, err
	}
	if flags&0b100 != 0 {
		if err := discardZchunkBytes(index, uint64(chunkChecksumSize)); err != nil {
			return zchunkHeader{}, err
		}
	}
	dictionaryCompressed, err := readZchunkInteger(index)
	if err != nil || dictionaryCompressed > maxZchunkDictionarySize {
		return zchunkHeader{}, errors.New("invalid zchunk dictionary size")
	}
	dictionaryUncompressed, err := readZchunkInteger(index)
	if err != nil || dictionaryUncompressed > maxZchunkDictionarySize {
		return zchunkHeader{}, errors.New("invalid zchunk uncompressed dictionary size")
	}
	parts := make([]zchunkPart, 0, chunkCount-1)
	for range chunkCount - 1 {
		part := zchunkPart{stream: 1}
		if flags&0b1 != 0 {
			part.stream, err = readZchunkInteger(index)
			if err != nil {
				return zchunkHeader{}, err
			}
		}
		checksum := make([]byte, chunkChecksumSize)
		if _, err := io.ReadFull(index, checksum); err != nil {
			return zchunkHeader{}, err
		}
		part.storedRaw = flags&0b100 != 0 && allZero(checksum)
		if flags&0b100 != 0 {
			if err := discardZchunkBytes(index, uint64(chunkChecksumSize)); err != nil {
				return zchunkHeader{}, err
			}
		}
		compressed, err := readZchunkInteger(index)
		if err != nil || compressed > math.MaxInt64 {
			return zchunkHeader{}, errors.New("invalid zchunk chunk size")
		}
		uncompressed, err := readZchunkInteger(index)
		if err != nil || uncompressed > math.MaxInt64 {
			return zchunkHeader{}, errors.New("invalid zchunk uncompressed chunk size")
		}
		part.compressedSize = int64(compressed)
		part.uncompressedSize = int64(uncompressed)
		if part.storedRaw && part.compressedSize != part.uncompressedSize {
			return zchunkHeader{}, errors.New("invalid raw zchunk chunk size")
		}
		parts = append(parts, part)
	}
	if index.N != 0 {
		return zchunkHeader{}, errors.New("zchunk index has trailing data")
	}
	signatures, err := readZchunkInteger(headerReader)
	if err != nil || signatures != 0 || headerReader.N != 0 {
		return zchunkHeader{}, errors.New("invalid zchunk header trailer")
	}
	return zchunkHeader{
		compression:            compression,
		dictionaryCompressed:   dictionaryCompressed,
		dictionaryUncompressed: dictionaryUncompressed,
		parts:                  parts,
	}, nil
}

func decompressZstdPart(ctx context.Context, source io.Reader, destination io.Writer, expectedSize int64, dictionary []byte) error {
	options := []zstd.DOption{zstd.WithDecoderMaxMemory(uint64(max(expectedSize, 64<<20))), zstd.WithDecoderMaxWindow(256 << 20)}
	if len(dictionary) != 0 {
		options = append(options, zstd.WithDecoderDictRaw(0, dictionary))
	}
	decoder, err := zstd.NewReader(source, options...)
	if err != nil {
		return err
	}
	defer decoder.Close()
	written, err := io.CopyN(destination, &contextReader{ctx: ctx, reader: decoder}, expectedSize)
	if err != nil {
		return fmt.Errorf("decoded %d of %d bytes: %w", written, expectedSize, err)
	}
	var extra [1]byte
	n, err := decoder.Read(extra[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("decoded data exceeds declared size")
		}
		return err
	}
	return nil
}

func readZchunkInteger(reader io.Reader) (uint64, error) {
	var value uint64
	for index := 0; index < 10; index++ {
		var encoded [1]byte
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return 0, err
		}
		part := uint64(encoded[0] & 0x7f)
		if index == 9 && part > 1 {
			return 0, errors.New("zchunk integer overflows uint64")
		}
		value |= part << (index * 7)
		if encoded[0]&0x80 != 0 {
			if index > 0 && part == 0 {
				return 0, errors.New("zchunk integer is not canonical")
			}
			return value, nil
		}
	}
	return 0, errors.New("zchunk integer is too long")
}

func zchunkChecksumSize(checksumType uint64) (int, error) {
	switch checksumType {
	case 0:
		return 20, nil
	case 1:
		return 32, nil
	case 2:
		return 64, nil
	case 3:
		return 16, nil
	default:
		return 0, fmt.Errorf("unsupported zchunk checksum type %d", checksumType)
	}
}

func discardZchunkBytes(reader io.Reader, size uint64) error {
	if size > math.MaxInt64 {
		return errors.New("zchunk field is too large")
	}
	_, err := io.CopyN(io.Discard, reader, int64(size))
	return err
}

func allZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
