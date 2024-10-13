package db

import (
	"fmt"
	"log"
	"lsm/memtable"
	"lsm/sstable"
)

const (
	memtableSizeLimit      = 4 << 10 // 4 KiB
	memtableFlushThreshold = 8 << 10 // 8 KiB
)

type MemTables struct {
	mutable *memtable.Memtable   // current mutable (read-write) memtable
	queue   []*memtable.Memtable // queue of immutable (read-only) memtables, not flushed to disk yet
}

type DB struct {
	memtables   MemTables
	dataStorage *sstable.Provider
}

func Open(dirname string) (*DB, error) {
	dataStorage, err := sstable.NewProvider(dirname)
	if err != nil {
		return nil, err
	}
	db := &DB{dataStorage: dataStorage}
	db.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)
	return db, nil
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

func (d *DB) flushMemtables() error {
	n := len(d.memtables.queue) - 1
	flushable := d.memtables.queue[:n]
	// update the queue to discard flushed memtables
	d.memtables.queue = d.memtables.queue[n:]

	for i:=0; i<len(flushable); i++ {
		meta := d.dataStorage.PrepareNewFile()
		f, err := d.dataStorage.OpenFileForWriting(meta)
		if err != nil {
			return err
		}

		w := sstable.NewWriter(f)
		err = w.Convert(flushable[i])
		if err != nil {
			return err
		}

		err = w.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) maybeScheduleFlush() {
	var totalSize int
	for i := 0; i < len(d.memtables.queue); i++ {
		totalSize += d.memtables.queue[i].Size()
	}
	fmt.Printf("Total size of memtables: %d\n", totalSize)
	if totalSize > memtableFlushThreshold {
		err := d.flushMemtables()
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
}

func (d *DB) Set(key, val []byte) {
	m := d.prepMemtableForKV(key, val)
	m.Insert(key, val)
	d.maybeScheduleFlush()
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
	d.maybeScheduleFlush()
}
