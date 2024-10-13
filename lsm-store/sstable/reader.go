package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"lsm/memtable"
)

type Reader struct {
	file    io.Closer
	br      *bufio.Reader
	buf     []byte
	encoder *memtable.Encoder
}

func NewReader(file io.Reader) *Reader {
	r := &Reader{}
	r.file, _ = file.(io.Closer)
	r.br = bufio.NewReader(file)
	r.buf = make([]byte, 0, 1024)
	return r
}

func (r *Reader) Get(searchKey []byte) (*memtable.EncodedValue, error) {
	for {
		buf := r.buf[:4]
		_, err := io.ReadFull(r.br, buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		keyLen := binary.LittleEndian.Uint16(buf[:2])
		valLen := binary.LittleEndian.Uint16(buf[2:])
		bytesNeeded := keyLen + valLen

		buf = r.buf[:bytesNeeded]
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

func (r *Reader) Close() error {
	err := r.file.Close()
	if err != nil {
		return err
	}
	r.file = nil
	r.br = nil
	return nil
}
