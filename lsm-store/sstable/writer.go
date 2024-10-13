package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"lsm/memtable"
)

// 2 methods -- `Close() error` and `Sync() error`
type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file syncCloser
	bw   *bufio.Writer
	buf  *bytes.Buffer
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw
	w.buf = bytes.NewBuffer(make([]byte, 0, 1024))
	return w
}

// write each kv-pair in memtable to `.sst` file by
// 1. copying each pair to writer.buf
// 2. writing the writer.buf to writer.file
// Naive data block: keyLen (2B)|valLen (2B)|key (keyLen bytes)|opKind (1B)|val (valLen bytes)
// Optimised data block: write keys and values of any length while only using the minimal amount
// of space for keylen and valLen.
// Instead of 4B + keylen + valLen bytes, we need n + keylen + valLen bytes.
func (w *Writer) writeDataBlock(key, encodedVal []byte) error {
	keyLen, valLen := len(key), len(encodedVal)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen // 20 + keyLen + valLen
	available := w.buf.Available()
	// if the buffer is not big enough, grow it to handle larger data blobs
	if needed > available {
		w.buf.Grow(needed)
	}

	buf := w.buf.AvailableBuffer()
	buf = buf[:needed]
	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], encodedVal)
	used := n + keyLen + valLen
	_, err := w.buf.Write(buf[:used])
	if err != nil {
		return err
	}
	_, err = w.bw.ReadFrom(w.buf)
	if err != nil {
		return err
	}
	return nil
}

// iterate over level 1 of the memtable and write each kv-pair to .sst file
func (w *Writer) Convert(m *memtable.Memtable) error {
	iter := m.Iterator()
	for iter.HasNext() {
		key, val := iter.Next()
		err := w.writeDataBlock(key, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Close() error {
	// Flush any remaining data from the buffer.
	err := w.bw.Flush()
	if err != nil {
		return err
	}

	// Force OS to flush its I/O buffers and write data to disk.
	err = w.file.Sync()
	if err != nil {
		return err
	}

	// Close the file.
	err = w.file.Close()
	if err != nil {
		return err
	}

	w.bw = nil
	w.file = nil
	return err
}
