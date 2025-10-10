package irisdb

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/alimx07/IrisDB/page"
)

type WAL struct {
	page *page.Page
}

// NewWal creates a new Write-Ahead Log
func NewWal(name string, pageSize uint16, fsync bool, syncInterval time.Duration) (*WAL, error) {
	pg, err := page.InitPage(
		name,
		os.O_CREATE|os.O_RDWR,
		0644,
		pageSize,
		fsync,
		syncInterval,
	)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		page: pg,
	}
	return w, nil
}

// Singe Wal Entry
type LogEntry struct {
	Op    byte // Operation type
	Key   []byte
	Value []byte
}

func (w *WAL) Write(entry *LogEntry) (uint32, error) {

	data := w.serializeEntry(entry)

	pageNum, err := w.page.Write(data)
	if err != nil {
		return 0, err
	}

	return pageNum, nil
}

func (w *WAL) Read(pageNum uint16) (*LogEntry, error) {
	data, _, err := w.page.Read(pageNum)
	if err != nil {
		return nil, err
	}

	return w.deserializeEntry(data), nil
}

func (w *WAL) serializeEntry(entry *LogEntry) []byte {

	/*
	   ENTRY LAYOUT
	   ------------------------------------------------------
	   | Op(1) | KeyLen(2) | ValueLen(4) | Key | Value     |
	   ------------------------------------------------------
	*/

	keyLen := len(entry.Key)
	valueLen := len(entry.Value)

	data := make([]byte, 7+keyLen+valueLen)

	data[0] = entry.Op
	binary.BigEndian.PutUint32(data[1:3], uint32(keyLen))
	binary.BigEndian.PutUint32(data[3:7], uint32(valueLen))
	copy(data[7:], entry.Key)
	copy(data[7+keyLen:], entry.Value)

	return data
}

func (w *WAL) deserializeEntry(data []byte) *LogEntry {
	op := data[0]
	keyLen := binary.BigEndian.Uint32(data[1:5])
	valueLen := binary.BigEndian.Uint32(data[5:9])

	key := make([]byte, keyLen)
	value := make([]byte, valueLen)

	copy(key, data[9:9+keyLen])
	copy(value, data[9+keyLen:9+keyLen+valueLen])

	return &LogEntry{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

// Replay replays all WAL entries using an iterator
func (w *WAL) Replay(fn func(*LogEntry) error) error {
	it := page.Newiterator(w.page)

	for it.Valid() {
		pgNum := it.Next()
		data, err := it.Get(uint16(pgNum))
		if err != nil {
			return err
		}

		entry := w.deserializeEntry(data)
		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	return w.page.Close()
}
