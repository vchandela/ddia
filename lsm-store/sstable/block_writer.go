package sstable

import (
	"bytes"
	"encoding/binary"
)

// maxBlockSize is the maximum size of a data block and index block.
const (
	maxBlockSize = 4096
)

// encapsulate operations that are common to preparing both index blocks
// and data blocks for writing to disk
type blockWriter struct {
	buf        *bytes.Buffer //bytes.Buffer makes it easier to read/write/grow the buffer than []byte
	offsets    []uint32
	currOffset uint32 // starting offset of the current data chunk
	nextOffset uint32

	chunkSize  int    // desired numEntries in each data chunk
	numEntries int    // numEntries in the current data chunk
	prefixKey  []byte // prefixKey of the current data chunk
}

func newBlockWriter(chunkSize int) *blockWriter {
	bw := &blockWriter{}
	bw.buf = bytes.NewBuffer(make([]byte, 0, maxBlockSize))
	bw.chunkSize = chunkSize
	return bw
}

// use byte slice as an in-mem staging area for creating data blocks
func (b *blockWriter) scratchBuf(needed int) []byte {
	available := b.buf.Available()
	if needed > available {
		b.buf.Grow(needed)
	}
	buf := b.buf.AvailableBuffer()
	return buf[:needed]
}

func (b *blockWriter) trackOffset(n uint32) {
	b.nextOffset += n
	if b.numEntries == b.chunkSize {
		b.offsets = append(b.offsets, b.currOffset)
		b.currOffset = b.nextOffset
		b.numEntries = 0
		b.prefixKey = nil
	}
}

func (b *blockWriter) calculateSharedLength(key []byte) int {
	sharedLen := 0
	if b.prefixKey == nil {
		b.prefixKey = key
		return sharedLen
	}

	for i := 0; i < min(len(key), len(b.prefixKey)); i++ {
		if key[i] != b.prefixKey[i] {
			break
		}
		sharedLen++
	}

	return sharedLen
}

// index block and data block share similar logic for writing kv-pairs (data entry)
// data entry = keyLen|valLen|key|val
func (b *blockWriter) add(key, val []byte) (int, error) {
	sharedLen := b.calculateSharedLength(key)
	keyLen, valLen := len(key), len(val)
	needed := 3*binary.MaxVarintLen64 + (keyLen - sharedLen) + valLen
	buf := b.scratchBuf(needed)
	n := binary.PutUvarint(buf, uint64(sharedLen))
	n += binary.PutUvarint(buf[n:], uint64(keyLen-sharedLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key[sharedLen:])
	copy(buf[n+keyLen-sharedLen:], val)
	used := n + (keyLen - sharedLen) + valLen

	n, err := b.buf.Write(buf[:used])
	if err != nil {
		return n, err
	}

	b.numEntries++
	b.trackOffset(uint32(n))
	return n, nil
}

func (b *blockWriter) reset() {
	b.nextOffset = 0
	b.currOffset = 0
	b.offsets = b.offsets[:0]
	b.numEntries = 0
	b.prefixKey = nil
}

// This method is only for index block.
// Write all of the collected offsets into the final index block.
// Along with that it also records the total length of the index block, and the total number of offsets that were recorded
// So, our footer size is 8 bytes.
func (b *blockWriter) finish() error {
	if b.prefixKey != nil {
		// Force flush of last prefix key offset.
		b.offsets = append(b.offsets, b.currOffset)
	}
	numOffsets := len(b.offsets)
	needed := (numOffsets + 2) * 4
	buf := b.scratchBuf(needed)
	for i, offset := range b.offsets {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], offset)
	}

	binary.LittleEndian.PutUint32(buf[needed-8:needed-4], uint32(numOffsets))
	// total len of index block = size of buffer (contains largest key -> [startOffset;size of data block])
	// + size of offsets + size of footer
	binary.LittleEndian.PutUint32(buf[needed-4:needed], uint32(b.buf.Len()+needed))
	_, err := b.buf.Write(buf)
	if err != nil {
		return err
	}
	// ensure that every data block starts with a clean offset state
	b.reset()
	return nil
}
