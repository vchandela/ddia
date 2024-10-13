package sstable

import (
	"bytes"
	"encoding/binary"
)

type blockReader struct {
	buf        []byte
	offsets    []byte //part of index block containing all the offsets (4B each)
	numOffsets int
}

func (b *blockReader) fetchDataFor(pos int) (kvOffset int, key, val []byte) {
	var keyLen, valLen uint64
	var n int
	kvOffset = int(binary.LittleEndian.Uint32(b.offsets[pos*4 : pos*4+4]))
	offset := kvOffset
	keyLen, n = binary.Uvarint(b.buf[offset:])
	offset += n
	valLen, n = binary.Uvarint(b.buf[offset:])
	offset += n
	key = b.buf[offset : offset+int(keyLen)]
	offset += int(keyLen)
	val = b.buf[offset : offset+int(valLen)]
	return kvOffset, key, val
}

func (b *blockReader) readOffsetAt(pos int) int {
	offset, _, _ := b.fetchDataFor(pos)
	return offset
}

func (b *blockReader) readKeyAt(pos int) []byte {
	_, key, _ := b.fetchDataFor(pos)
	return key
}

func (b *blockReader) readValAt(pos int) []byte {
	_, _, val := b.fetchDataFor(pos)
	return val
}

func (b *blockReader) search(searchKey []byte) int {
	low, high := 0, b.numOffsets
	var mid int
	for low < high {
		mid = (low + high) / 2
		key := b.readKeyAt(mid)
		cmp := bytes.Compare(searchKey, key)
		if cmp > 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
