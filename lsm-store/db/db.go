package db

import (
	"fmt"
	"log"
	"lsm/memtable"
)

const (
	memtableSizeLimit = 4 << 10 // 4 KiB
)

type MemTables struct {
	mutable *memtable.Memtable   // current mutable (read-write) memtable
	queue   []*memtable.Memtable // queue of immutable (read-only) memtables, not flushed to disk yet
}

type DB struct {
	memtables MemTables
}

func Open() *DB {
	db := &DB{}
	db.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)
	return db
}

func (d *DB) rotateMemtables() *memtable.Memtable {
	d.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	d.memtables.queue = append(d.memtables.queue, d.memtables.mutable)
	return d.memtables.mutable
}

func (d *DB) prepMemtableForKV(key, val []byte) *memtable.Memtable {
	m := d.memtables.mutable
	if !m.HasRoomForWrite(key, val) {
		m = d.rotateMemtables()
	}
	return m
}

func (d *DB) Set(key, val []byte) {
	m := d.prepMemtableForKV(key, val)
	m.Insert(key, val)
}

func (d *DB) Get(key []byte) ([]byte, error) {
	// scan memtables from newest to oldest
	for i := len(d.memtables.queue) - 1; i >= 0; i-- {
		m := d.memtables.queue[i]
		if encodedVal, ok := m.Get(key); ok {
			if encodedVal.IsTombstone() {
				log.Printf(`Found key "%s" marked as deleted in memtable "%d".\n`, key, i)
				return nil, fmt.Errorf("key not found")
			} else {
				log.Printf(`Found key "%s" in memtable "%d" with value "%s"`, key, i, encodedVal.Value())
				return encodedVal.Value(), nil
			}
		}

	}
	return nil, fmt.Errorf("key not found")
}

func (d *DB) Delete(key []byte) {
	m := d.prepMemtableForKV(key, nil)
	m.InsertTombstone(key)
}
