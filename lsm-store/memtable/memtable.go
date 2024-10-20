package memtable

import (
	"lsm/encoder"
	"lsm/skiplist"
	"lsm/storage"
)

type Memtable struct {
	sl        *skiplist.SkipList
	sizeUsed  int // The approximate amount of space used by the Memtable so far (in bytes).
	sizeLimit int // The maximum allowed size of the Memtable (in bytes).
	encoder   *encoder.Encoder
	logMeta   *storage.FileMetadata
}

func NewMemtable(sizeLimit int, logMeta *storage.FileMetadata) *Memtable {
	m := &Memtable{
		sl:        skiplist.NewSkipList(),
		sizeLimit: sizeLimit,
		encoder:   encoder.NewEncoder(),
		logMeta:   logMeta,
	}
	return m
}

// check if memtable has room for new kv-pair
func (m *Memtable) HasRoomForWrite(key, val []byte) bool {
	sizeAvailable := m.sizeLimit - m.sizeUsed
	// +1 for OpKind
	return (len(key) + len(val) + 1) <= sizeAvailable
}

func (m *Memtable) Insert(key, val []byte) {
	encodedVal := m.encoder.Encode(encoder.OpKindSet, val)
	m.sl.Insert(key, encodedVal)
	// +1 for OpKind
	m.sizeUsed += (len(key) + len(val) + 1)
}

func (m *Memtable) InsertTombstone(key []byte) {
	encodedVal := m.encoder.Encode(encoder.OpKindDelete, nil)
	m.sl.Insert(key, encodedVal)
	m.sizeUsed += 1
}

func (m *Memtable) Get(key []byte) (*encoder.EncodedValue, bool) {
	encodedVal, found := m.sl.Get(key)
	if !found {
		return nil, false
	}
	return m.encoder.Parse(encodedVal), true
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}

func (m *Memtable) Iterator() *skiplist.Iterator {
	return m.sl.Iterator()
}

func (m *Memtable) LogFile() *storage.FileMetadata {
	return m.logMeta
}
