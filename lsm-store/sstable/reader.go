package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"lsm/memtable"
)

const (
	footerSizeInBytes = 4
)

type statCloser interface {
	Stat() (fs.FileInfo, error)
	io.Closer
}

type Reader struct {
	file    statCloser
	br      *bufio.Reader
	buf     []byte
	encoder *memtable.Encoder
}

func NewReader(file io.Reader) *Reader {
	r := &Reader{}
	r.file, _ = file.(statCloser)
	r.br = bufio.NewReader(file)
	r.buf = make([]byte, 0, 1024)
	return r
}

func (r *Reader) sequentialSearch(searchKey []byte) (*memtable.EncodedValue, error) {
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

func (r *Reader) binarySearch(searchKey []byte) (*memtable.EncodedValue, error) {
	// Determine total size of *.sst file.
	info, err := r.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	// Load entire *.sst file into memory.
	buf := r.buf[:fileSize]
	_, err = io.ReadFull(r.br, buf)
	if err != nil {
		return nil, err
	}

	// Parse the footer and load the index block.
	footerOffset := int(fileSize - footerSizeInBytes)
	numOffsets := int(binary.LittleEndian.Uint32(buf[footerOffset:]))
	indexLength := numOffsets * 4
	indexStartOffset := footerOffset - indexLength
	indexBuf := buf[indexStartOffset : indexStartOffset+indexLength]

	// Search the data blocks using the index.
	low, high := 0, numOffsets
	var mid int
	for low < high {
		mid = (low + high) / 2
		currOffset := int(binary.LittleEndian.Uint32(indexBuf[mid*4 : mid*4+4]))
		keyLen, n := binary.Uvarint(buf[currOffset:])
		currOffset += n
		valLen, n := binary.Uvarint(buf[currOffset:])
		currOffset += n
		key := buf[currOffset : currOffset+int(keyLen)]
		currOffset += int(keyLen)
		val := buf[currOffset : currOffset+int(valLen)]
		cmp := bytes.Compare(searchKey, key)
		switch {
		case cmp > 0:
			low = mid + 1
		case cmp < 0:
			high = mid
		case cmp == 0:
			return r.encoder.Parse(val), nil
		}
	}
	return nil, fmt.Errorf("key not found")
}

func (r *Reader) Get(searchKey []byte) (*memtable.EncodedValue, error) {
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
