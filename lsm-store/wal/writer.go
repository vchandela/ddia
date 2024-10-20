package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"lsm/encoder"
)

const headerSize = 3

const (
	chunkTypeFull   = 1
	chunkTypeFirst  = 2
	chunkTypeMiddle = 3
	chunkTypeLast   = 4
)

const blockSize = 4 << 10 // 4 KiB

type block struct {
	buf    [blockSize]byte // used as a scratch space for writing records in memory
	offset int             // current position within the block that data should be written to or read from
	len    int             // total size of the data block (can be <blockSize for last block)
}

type syncWriteCloser interface {
	io.WriteCloser
	Sync() error
}

// assembles data blocks in memory before writing them to WAL file
type Writer struct {
	block   *block
	file    syncWriteCloser
	encoder *encoder.Encoder
	buf     *bytes.Buffer // staging area for splitting the full payload into chunks that fit into the fixed-size block buffer
}

func NewWriter(logFile syncWriteCloser) *Writer {
	w := &Writer{
		block:   &block{},
		file:    logFile,
		encoder: encoder.NewEncoder(),
		buf:     &bytes.Buffer{},
	}
	return w
}

// handle the dynamic resizing of the bytes.Buffer based on the length of the incoming payload
func (w *Writer) scratchBuf(needed int) []byte {
	available := w.buf.Available()
	if needed > available {
		w.buf.Grow(needed)
	}
	buf := w.buf.AvailableBuffer()
	return buf[:needed]
}

// writeAndSync writes to the underlying WAL file and forces a sync of its contents to stable storage
func (w *Writer) writeAndSync(p []byte) (err error) {
	if _, err = w.file.Write(p); err != nil {
		return err
	}
	// data is immediately written to disk rather than stuck in the Linux page cache.
	if err = w.file.Sync(); err != nil {
		return err
	}
	return nil
}

// sealBlock applies zero padding to the current block and calls writeAndSync to persist it to stable storage
func (w *Writer) sealBlock() error {
	b := w.block
	clear(b.buf[b.offset:])
	if err := w.writeAndSync(b.buf[b.offset:]); err != nil {
		return err
	}
	// prepare data block for new iteration.
	// The buffer itself remains dirty (holding the contents of the previous data block), but that's
	// okay as only newly modified portions of the buffer are going to be synced to disk during
	// subsequent write operations.
	b.offset = 0
	return nil
}

func (w *Writer) record(key, val []byte) error {
	// determine the maximum possible payload length
	keyLen, valLen := len(key), len(val)
	maxLen := 2*binary.MaxVarintLen64 + keyLen + valLen
	// initialize a scratch buffer capable of fitting the entire payload
	scratch := w.scratchBuf(maxLen)
	// place the entire payload into the scratch buffer
	n := binary.PutUvarint(scratch[:], uint64(keyLen))
	n += binary.PutUvarint(scratch[n:], uint64(valLen))
	copy(scratch[n:], key)
	copy(scratch[n+keyLen:], val)
	// calculate the actual scratch buffer length being used
	dataLen := n + keyLen + valLen
	// discard the unused portion
	scratch = scratch[:dataLen]

	// start splitting the payload into chunks
	for chunk := 0; len(scratch) > 0; chunk++ {
		// reference the current data block
		b := w.block
		// seal the block if it doesn't have enough room to accommodate this chunk
		if b.offset+headerSize >= blockSize {
			if err := w.sealBlock(); err != nil {
				return err
			}
		}
		// fill the data block with as much of the available payload as possible
		buf := b.buf[b.offset:]
		dataLen = copy(buf[headerSize:], scratch)
		// write the payload length to the chunk header
		binary.LittleEndian.PutUint16(buf, uint16(dataLen))
		// advance the scratch buffer and data block offsets
		scratch = scratch[dataLen:]
		b.offset += dataLen + headerSize

		// determine the chunk type and write it to the chunk header
		if b.offset < blockSize {
			if chunk == 0 {
				buf[2] = chunkTypeFull
			} else {
				buf[2] = chunkTypeLast
			}
		} else {
			if chunk == 0 {
				buf[2] = chunkTypeFirst
			} else {
				buf[2] = chunkTypeMiddle
			}
		}

		// flush updated data block portion to disk
		if err := w.writeAndSync(buf[:dataLen+headerSize]); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) RecordInsertion(key, val []byte) error {
	val = w.encoder.Encode(encoder.OpKindSet, val)
	return w.record(key, val)
}

func (w *Writer) RecordDeletion(key []byte) error {
	val := w.encoder.Encode(encoder.OpKindDelete, nil)
	return w.record(key, val)
}

func (w *Writer) Close() (err error) {
	// seal remaining portion of data block's buffer in memory
	if err = w.sealBlock(); err != nil {
		return err
	}
	err = w.file.Close()
	w.file = nil
	if err != nil {
		return err
	}
	return nil
}
