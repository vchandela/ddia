package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"lsm/encoder"

	"github.com/golang/snappy"
)

const (
	footerSizeInBytes = 8 // no.of offsets (4B) + len of index block (4B)
)

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)

type statReaderAtCloser interface {
	Stat() (fs.FileInfo, error)
	io.ReaderAt // loading data blocks in memory using their indexed offsets & loading index blocks
	io.Closer
}

type Reader struct {
	file     statReaderAtCloser
	br       *bufio.Reader
	buf      []byte
	encoder  *encoder.Encoder
	fileSize int64 //.sst file size

	compressionBuf []byte //read compressed data block into this buffer
}

func NewReader(file io.Reader) (*Reader, error) {
	r := &Reader{}
	r.file, _ = file.(statReaderAtCloser)
	r.br = bufio.NewReader(file)
	r.buf = make([]byte, 0, maxBlockSize)

	// retrieve file size immediately
	err := r.initFileSize()
	if err != nil {
		return nil, err
	}
	return r, err
}

func (r *Reader) sequentialSearch(searchKey []byte) (*encoder.EncodedValue, error) {
	for {
		keyLen, err := binary.ReadUvarint(r.br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		valLen, err := binary.ReadUvarint(r.br)
		if err != nil {
			return nil, err
		}
		needed := int(keyLen + valLen)

		if cap(r.buf) < needed {
			r.buf = make([]byte, needed)
		}
		buf := r.buf[:needed]
		_, err = io.ReadFull(r.br, buf)
		if err != nil {
			return nil, err
		}
		key := buf[:keyLen]
		val := buf[keyLen:]

		if bytes.Equal(searchKey, key) {
			return r.encoder.Parse(val), nil
		}
	}
	return nil, ErrKeyNotFound
}

// Retrieve the size of the loaded *.sst file.
func (r *Reader) initFileSize() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	r.fileSize = info.Size()

	return nil
}

// Read the *.sst footer into the supplied buffer -- this takes one disk IO.
func (r *Reader) readFooter() ([]byte, error) {
	buf := r.buf[:footerSizeInBytes]
	footerOffset := r.fileSize - footerSizeInBytes
	_, err := r.file.ReadAt(buf, footerOffset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// initialize it with {#offsets in index block, total length of index block} from the footer.
// helps with loading entire index block in memory.
func (r *Reader) prepareBlockReader(buf, footer []byte) *blockReader {
	numOffsets := int(binary.LittleEndian.Uint32(footer[:4]))
	indexLength := int(binary.LittleEndian.Uint32(footer[4:]))
	buf = buf[:indexLength]
	return &blockReader{
		buf:        buf,
		offsets:    buf[indexLength-(numOffsets+2)*4:],
		numOffsets: numOffsets,
	}
}

// load entire index block into memory.
func (r *Reader) readIndexBlock(footer []byte) (*blockReader, error) {
	b := r.prepareBlockReader(r.buf, footer)
	indexOffset := r.fileSize - int64(len(b.buf))
	_, err := r.file.ReadAt(b.buf, indexOffset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (r *Reader) sequentialSearchChunk(chunk []byte, searchKey []byte) (*encoder.EncodedValue, error) {
	var prefixKey []byte
	var offset int
	for {
		var keyLen, valLen uint64
		sharedLen, n := binary.Uvarint(chunk[offset:])
		if n <= 0 {
			break // EOF
		}
		offset += n
		keyLen, n = binary.Uvarint(chunk[offset:])
		offset += n
		valLen, n = binary.Uvarint(chunk[offset:])
		offset += n

		key := r.buf[:sharedLen+keyLen]
		if sharedLen == 0 {
			prefixKey = key
		}
		copy(key[:sharedLen], prefixKey[:sharedLen])
		copy(key[sharedLen:sharedLen+keyLen], chunk[offset:offset+int(keyLen)])
		val := chunk[offset+int(keyLen) : offset+int(keyLen)+int(valLen)]

		cmp := bytes.Compare(searchKey, key)
		if cmp == 0 {
			return r.encoder.Parse(val), nil
		}
		if cmp < 0 {
			break // Key is not present in this data block.
		}
		offset += int(keyLen) + int(valLen)
	}
	return nil, ErrKeyNotFound
}

// load data block into memory.
func (r *Reader) readDataBlock(indexEntry []byte) (*blockReader, error) {
	var err error
	val := r.encoder.Parse(indexEntry).Value()
	offset := binary.LittleEndian.Uint32(val[:4]) // data block offset in *.sst file
	length := binary.LittleEndian.Uint32(val[4:]) // data block length
	buf := r.buf[:length]
	_, err = r.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	buf, err = snappy.Decode(r.compressionBuf, buf)
	if err != nil {
		return nil, err
	}
	b := r.prepareBlockReader(buf, buf[len(buf)-footerSizeInBytes:])
	return b, nil
}

func (r *Reader) binarySearch(searchKey []byte) (*encoder.EncodedValue, error) {
	footer, err := r.readFooter()
	if err != nil {
		return nil, err
	}

	// Search index block for data block.
	index, err := r.readIndexBlock(footer)
	if err != nil {
		return nil, err
	}
	pos := index.search(searchKey, moveUpWhenKeyGT)
	if pos >= index.numOffsets {
		// searchKey is greater than the largest key in the current *.sst
		return nil, ErrKeyNotFound
	}
	indexEntry := index.readValAt(pos)

	// Search data block for data chunk.
	data, err := r.readDataBlock(indexEntry)
	if err != nil {
		return nil, err
	}
	offset := data.search(searchKey, moveUpWhenKeyGTE)
	if offset <= 0 {
		return nil, ErrKeyNotFound
	}
	chunkStart := data.readOffsetAt(offset - 1)
	chunkEnd := data.readOffsetAt(offset)
	chunk := data.buf[chunkStart:chunkEnd]

	// Search data chunk for key.
	return r.sequentialSearchChunk(chunk, searchKey)
}

func (r *Reader) Get(searchKey []byte) (*encoder.EncodedValue, error) {
	return r.binarySearch(searchKey)
}

func (r *Reader) Close() error {
	err := r.file.Close()
	if err != nil {
		return err
	}
	r.file = nil
	r.br = nil
	return nil
}
