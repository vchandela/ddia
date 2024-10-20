package db

import (
	"errors"
	"fmt"
	"io"
	"log"
	"lsm/encoder"
	"lsm/memtable"
	"lsm/sstable"
	"lsm/storage"
	"lsm/wal"
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
	dataStorage *storage.Provider
	// DB interacts with currently active WAL file's writer
	wal struct {
		w  *wal.Writer
		fm *storage.FileMetadata
	}
	sstables []*storage.FileMetadata
	logs     []*storage.FileMetadata
}

// After restarting our database storage engine, data previously stored on
// disk becomes inaccessible. To prevent this, we need to load all SSTables & WAL on DB restarts.
func (d *DB) loadFiles() error {
	meta, err := d.dataStorage.ListFiles()
	if err != nil {
		return err
	}
	for _, f := range meta {
		switch {
		case f.IsSSTable():
			d.sstables = append(d.sstables, f)
		case f.IsWAL():
			d.logs = append(d.logs, f)
		default:
			continue
		}
	}
	return nil
}

func Open(dirname string) (*DB, error) {
	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		return nil, err
	}
	db := &DB{dataStorage: dataStorage}

	if err = db.loadFiles(); err != nil {
		return nil, err
	}

	// replay WAL(s) right after DB loads the WAL file metadata, but before creating
	// a write-ahead log file for the mutable memtable
	if err = db.replayWALs(); err != nil {
		return nil, err
	}

	// always called right before rotateMemtables, so d.wal.fm is guaranteed to
	// contain metadata referencing the correct log file.
	if err = db.createNewWAL(); err != nil {
		return nil, err
	}

	db.rotateMemtables()
	return db, nil
}

func (d *DB) rotateMemtables() *memtable.Memtable {
	d.memtables.mutable = memtable.NewMemtable(memtableSizeLimit, d.wal.fm)
	d.memtables.queue = append(d.memtables.queue, d.memtables.mutable)
	return d.memtables.mutable
}

func (d *DB) prepMemtableForKV(key, val []byte) (*memtable.Memtable, error) {
	m := d.memtables.mutable
	if !m.HasRoomForWrite(key, val) {
		if err := d.rotateWAL(); err != nil {
			return nil, err
		}
		m = d.rotateMemtables()
	}
	return m, nil
}

func (d *DB) flushMemtables() error {
	n := len(d.memtables.queue) - 1
	flushable := d.memtables.queue[:n]
	// update the queue to discard flushed memtables
	d.memtables.queue = d.memtables.queue[n:]

	for i := 0; i < len(flushable); i++ {
		meta := d.dataStorage.PrepareNewSSTFile()
		f, err := d.dataStorage.OpenFileForWriting(meta)
		if err != nil {
			return err
		}

		w := sstable.NewWriter(f)
		err = w.ConvertMemtableToSST(flushable[i])
		if err != nil {
			return err
		}

		err = w.Close()
		if err != nil {
			return err
		}

		// add the new sstable to the list of sstables
		d.sstables = append(d.sstables, meta)
		err = d.dataStorage.DeleteFile(flushable[i].LogFile())
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

func (d *DB) Set(key, val []byte) error {
	if err := d.wal.w.RecordInsertion(key, val); err != nil {
		return err
	}
	m, err := d.prepMemtableForKV(key, val)
	if err != nil {
		return err
	}
	m.Insert(key, val)
	d.maybeScheduleFlush()
	return nil
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

	// scan sstables from newest to oldest
	for j := len(d.sstables) - 1; j >= 0; j-- {
		meta := d.sstables[j]
		f, err := d.dataStorage.OpenFileForReading(meta)
		if err != nil {
			return nil, err
		}
		r, err := sstable.NewReader(f)
		if err != nil {
			log.Fatalf("unable to initialize reader")
		}
		defer r.Close()

		var encodedValue *encoder.EncodedValue
		encodedValue, err = r.Get(key)
		if err != nil {
			if errors.Is(err, fmt.Errorf("key not found")) {
				continue
			}
			log.Fatal(err)
		}
		if encodedValue.IsTombstone() {
			log.Printf(`Found key "%s" marked as deleted in sstable "%d".`, key, meta.FileNum())
			return nil, errors.New("key not found")
		}
		log.Printf(`Found key "%s" in sstable "%d" with value "%s"`, key, meta.FileNum(), encodedValue.Value())
		return encodedValue.Value(), nil
	}

	return nil, fmt.Errorf("key not found")
}

func (d *DB) Delete(key []byte) error {
	if err := d.wal.w.RecordDeletion(key); err != nil {
		return err
	}
	m, err := d.prepMemtableForKV(key, nil)
	if err != nil {
		return err
	}
	m.InsertTombstone(key)
	d.maybeScheduleFlush()
	return nil
}

func (d *DB) createNewWAL() error {
	ds := d.dataStorage
	fm := ds.PrepareNewWALFile()
	logFile, err := ds.OpenFileForWriting(fm)
	if err != nil {
		return err
	}
	d.wal.w = wal.NewWriter(logFile)
	d.wal.fm = fm
	return nil
}

func (d *DB) rotateWAL() (err error) {
	if err = d.wal.w.Close(); err != nil {
		return err
	}
	if err = d.createNewWAL(); err != nil {
		return err
	}
	return nil
}

func (d *DB) replayWALs() error {
	for _, fm := range d.logs {
		if err := d.replayWAL(fm); err != nil {
			return err
		}
	}
	d.logs = nil
	return nil
}

func (d *DB) replayWAL(fm *storage.FileMetadata) error {
	// open WAL file for reading
	f, err := d.dataStorage.OpenFileForReading(fm)
	if err != nil {
		return err
	}
	// create a new reader for iterating the WAL file
	r := wal.NewReader(f)
	// prepare a new memtable to apply records to
	d.wal.fm = fm
	m := d.rotateMemtables()
	// start processing records
	for {
		// fetch next record from WAL file
		key, val, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// rotate memtable if it's full.
		// In certain edge cases, you may end up having multiple memtables pointing to the same WAL 
		// file. However, this is generally okay, as it's only likely to occur during a 
		// replay operation, and memtables used during the replay process are only briefly 
		// kept in memory.
		if !m.HasRoomForWrite(key, val.Value()) {
			d.rotateMemtables()
		}
		// apply WAL record to memtable
		if val.IsTombstone() {
			m.InsertTombstone(key)
		} else {
			m.Insert(key, val.Value())
		}
	}
	// hacky way to create a new mutable memtable and make others replayable
	d.rotateMemtables()
	// flush all memtables to disk
	if err = d.flushMemtables(); err != nil {
		return err
	}
	d.memtables.queue, d.memtables.mutable = nil, nil
	// close WAL file
	if err = f.Close(); err != nil {
		return err
	}
	return nil
}
