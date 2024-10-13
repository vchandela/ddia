package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"lsm/encoder"
)

const (
	footerSizeInBytes = 8 // no.of offsets (4B) + len of index block (4B)
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
	fileSize int64
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
	return nil, fmt.Errorf("key not found")
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

// Read the *.sst footer into the supplied buffer.
func (r *Reader) readFooter() ([]byte, error) {
	buf := r.buf[:footerSizeInBytes]
	footerOffset := r.fileSize - footerSizeInBytes
	_, err := r.file.ReadAt(buf, footerOffset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

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

func (r *Reader) readIndexBlock(footer []byte) (*blockReader, error) {
	b := r.prepareBlockReader(r.buf, footer)
	indexOffset := r.fileSize - int64(len(b.buf))
	_, err := r.file.ReadAt(b.buf, indexOffset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (r *Reader) sequentialSearchBuf(buf []byte, searchKey []byte) (*encoder.EncodedValue, error) {
	var offset int
	for {
		var keyLen, valLen uint64
		var n int
		keyLen, n = binary.Uvarint(buf[offset:])
		if n <= 0 {
			break // EOF
		}
		offset += n
		valLen, n = binary.Uvarint(buf[offset:])
		offset += n
		key := r.buf[:keyLen]
		copy(key[:], buf[offset:offset+int(keyLen)])
		offset += int(keyLen)
		val := buf[offset : offset+int(valLen)]
		offset += int(valLen)
		cmp := bytes.Compare(searchKey, key)
		if cmp == 0 {
			return r.encoder.Parse(val), nil
		}
		if cmp < 0 {
			break // Key is not present in this data block.
		}
	}
	return nil, fmt.Errorf("key not found")
}

func (r *Reader) readDataBlock(indexEntry []byte) ([]byte, error) {
	var err error
	val := r.encoder.Parse(indexEntry).Value()
	offset := binary.LittleEndian.Uint32(val[:4]) // data block offset in *.sst file
	length := binary.LittleEndian.Uint32(val[4:]) // data block length
	buf := r.buf[:length]
	_, err = r.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *Reader) binarySearch(searchKey []byte) (*encoder.EncodedValue, error) {
	// Load footer in memory.
	footer, err := r.readFooter()
	if err != nil {
		return nil, err
	}

	// Load index block in memory.
	index, err := r.readIndexBlock(footer)
	if err != nil {
		return nil, err
	}
	// Search index block for data block.
	pos := index.search(searchKey)
	indexEntry := index.readValAt(pos)

	// Load data block in memory.
	data, err := r.readDataBlock(indexEntry)
	if err != nil {
		return nil, err
	}

	return r.sequentialSearchBuf(data, searchKey)
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
