package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"lsm/encoder"
	"lsm/memtable"
	"math"

	"github.com/golang/snappy"
)

// If we exceed 90% of the maximum acceptable data block size after adding a new data entry,
// we consider the data block to be full and suitable for flushing.
var blockFlushThreshold = int(math.Floor(maxBlockSize * 0.9))

// 2 methods -- `Close() error` and `Sync() error`
type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file       syncCloser
	bw         *bufio.Writer
	buf        []byte
	dataBlock  *blockWriter
	indexBlock *blockWriter
	encoder    *encoder.Encoder

	offset       int    // offset of current data block.
	bytesWritten int    // bytesWritten to current data block.
	lastKey      []byte // lastKey (largest) in current data block

	compressionBuf []byte // stores compressed data block
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.buf = make([]byte, 0, 8)
	w.file, w.bw = file.(syncCloser), bw
	w.dataBlock, w.indexBlock = newBlockWriter(), newBlockWriter()
	w.indexBlock.trackOffsets = true
	return w
}

// add largest key -> {offset, length} of data block to indexBlock.
func (w *Writer) addIndexEntry() error {
	buf := w.buf[:8]
	binary.LittleEndian.PutUint32(buf[:4], uint32(w.offset))              // data block offset
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(w.compressionBuf))) // data block length
	_, err := w.indexBlock.add(w.lastKey, w.encoder.Encode(encoder.OpKindSet, buf))
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) flushDataBlock() error {
	if w.bytesWritten <= 0 {
		return nil // nothing to flush
	}

	// write dataBlock buffer to underlying *.sst file
	w.compressionBuf = snappy.Encode(w.compressionBuf, w.dataBlock.buf.Bytes())
	w.dataBlock.buf.Reset()
	_, err := w.bw.Write(w.compressionBuf)
	if err != nil {
		return err
	}

	// add a corresponding data entry into the indexBlock buffer
	err = w.addIndexEntry()
	if err != nil {
		return err
	}

	// updates the w.offset and w.bytesWritten for subsequent data blocks
	w.offset += len(w.compressionBuf)
	w.bytesWritten = 0
	return nil
}

// iterate over level 1 of the memtable and write each kv-pair to .sst file
func (w *Writer) ConvertMemtableToSST(m *memtable.Memtable) error {
	iter := m.Iterator()
	for iter.HasNext() {
		key, val := iter.Next()
		n, err := w.dataBlock.add(key, val)
		if err != nil {
			return err
		}
		w.bytesWritten += n
		w.lastKey = key

		if w.bytesWritten > blockFlushThreshold {
			err = w.flushDataBlock()
			if err != nil {
				return err
			}
		}
	}
	// flush any pending data
	err := w.flushDataBlock()
	if err != nil {
		return err
	}

	// update index block
	err = w.indexBlock.finish()
	if err != nil {
		return err
	}

	// write indexBlock buffer to underlying *.sst file
	_, err = w.bw.ReadFrom(w.indexBlock.buf)
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
