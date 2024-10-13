package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
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
	buf  []byte
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw
	w.buf = make([]byte, 0, 1024)
	return w
}

// write each kv-pair in memtable to `.sst` file by
// 1. copying each pair to writer.buf
// 2. writing the writer.buf to writer.file
// data block: keyLen (2B)|valLen (2B)|key (keyLen bytes)|opKind (1B)|val (valLen bytes)
func (w *Writer) writeDataBlock(key, encodedVal []byte) error {
	keyLen, valLen := len(key), len(encodedVal)
	bytesNeeded := 4 + keyLen + valLen
	buf := w.buf[:bytesNeeded]
	binary.LittleEndian.PutUint16(buf[:], uint16(keyLen))
	binary.LittleEndian.PutUint16(buf[2:], uint16(valLen))
	copy(buf[4:], key)
	copy(buf[4+keyLen:], encodedVal)

	bytesWritten, err := w.bw.Write(buf)
	if err != nil {
		log.Fatal(err, bytesWritten)
		return err
	}
	return nil
}

// iterate over level 1 of the memtable
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
