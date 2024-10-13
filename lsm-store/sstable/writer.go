package sstable

import (
	"bufio"
	"bytes"
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
	file       syncCloser
	bw         *bufio.Writer
	buf        *bytes.Buffer
	offsets    []uint32 // offsets of each data block in the .sst file
	nextOffset uint32   // offset at the end of most recently added data block
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw
	w.buf = bytes.NewBuffer(make([]byte, 0, 1024))
	return w
}

// use byte slice as an in-mem staging area for creating data blocks
func (w *Writer) scratchBuf(needed int) []byte {
	available := w.buf.Available()
	if needed > available {
		w.buf.Grow(needed)
	}
	buf := w.buf.AvailableBuffer()
	return buf[:needed]
}

// write each kv-pair in memtable to `.sst` file by
// 1. copying each pair to writer.buf
// 2. writing the writer.buf to writer.file
// Naive data block: keyLen (2B)|valLen (2B)|key (keyLen bytes)|opKind (1B)|val (valLen bytes)
// Optimised data block: write keys and values of any length while only using the minimal amount
// of space for keylen and valLen.
// Instead of 4B + keylen + valLen bytes, we need n + keylen + valLen bytes.
func (w *Writer) writeDataBlock(key, encodedVal []byte) (int, error) {
	keyLen, valLen := len(key), len(encodedVal)
	needed := 2*binary.MaxVarintLen64 + keyLen + valLen // 20 + keyLen + valLen
	buf := w.scratchBuf(needed)

	n := binary.PutUvarint(buf, uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], encodedVal)

	used := n + keyLen + valLen
	n, err := w.buf.Write(buf[:used])
	if err != nil {
		return 0, err
	}

	m, err := w.bw.ReadFrom(w.buf)
	if err != nil {
		// return partial progress on error
		return int(m), err
	}
	return int(m), nil
}

func (w *Writer) writeIndexBlock() error {
	numOffsets := len(w.offsets)
	needed := (numOffsets + 1) * 4 // 4 bytes per offset and extra 4 bytes for total kv-pairs
	buf := w.scratchBuf(needed)
	for i, offset := range w.offsets {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], offset)
	}
	binary.LittleEndian.PutUint32(buf[needed-4:needed], uint32(numOffsets))
	_, err := w.bw.Write(buf[:])
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// track starting offsets of each data block
func (w *Writer) addIndexEntry(n int) {
	w.offsets = append(w.offsets, w.nextOffset)
	w.nextOffset += uint32(n)
}

// iterate over level 1 of the memtable and write each kv-pair to .sst file
func (w *Writer) ConvertMemtableToSST(m *memtable.Memtable) error {
	iter := m.Iterator()
	for iter.HasNext() {
		key, val := iter.Next()
		n, err := w.writeDataBlock(key, val)
		if err != nil {
			return err
		}
		w.addIndexEntry(n)
	}
	err := w.writeIndexBlock()
	if err != nil {
		return err
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
