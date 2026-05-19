package irisdb

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alimx07/IrisDB/db"
	"github.com/alimx07/IrisDB/filter"
	"github.com/alimx07/IrisDB/page"
	"github.com/alimx07/IrisDB/skiplist"
	"github.com/klauspost/compress/snappy"
)

type Entry struct {
	Key   []byte // non-nil only in Scan results
	Value []byte
	Time  time.Time
}

// Give me Hint. Where should I start
// to find place to add data in memtable
// NOTE: one goroutine per Hint. Never share
type Hint struct{ inner *skiplist.Hint }

func NewHint() *Hint { return &Hint{inner: &skiplist.Hint{}} }

var ErrKeyNotFound = errors.New("irisdb: key not found")

type SSTABLE struct {
	name      string
	keys      *page.Page
	vals      *page.Page
	filter    *filter.Bloomfilter
	index     *IndexBlock
	dataPages uint32
	size      uint64
}

type MemTableState struct {
	m   *skiplist.SkipList
	wal *WAL
}

type IrisDB struct {
	mu        sync.RWMutex
	sstables  [][]*SSTABLE
	currMem   *MemTableState
	frozenMem *MemTableState
	muMem     sync.RWMutex
	dbPath    string
	done      chan struct{}
}

func newMemtableState() *MemTableState {
	return &MemTableState{
		m: skiplist.NewSkipList(uint32(MemTableSize)),
	}
}

func OpenDB(dbPath string) (*IrisDB, error) {
	if err := os.MkdirAll(dbPath, os.FileMode(Permission)); err != nil {
		return nil, err
	}

	idb := &IrisDB{
		dbPath:   dbPath,
		sstables: make([][]*SSTABLE, MaxLevels),
		done:     make(chan struct{}),
	}

	// idb.currMem = idb.newMemtableState(false)
	err := filepath.WalkDir(dbPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		if ext != WalExtension && ext != KeyExtension {
			return nil
		}

		if ext == WalExtension {
			curr, initErr := page.InitPage(path, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
			if initErr != nil {
				return initErr
			}
			idb.currMem = idb.newMemtableState(false)
			idb.currMem.wal = &WAL{page: curr}

			// mem := skiplist.NewSkipList(uint32(MemTableSize))

			if replayErr := idb.currMem.wal.Replay(func(log *LogEntry) error {
				idb.currMem.m.Insert(log.Key, db.NewValue(log.Value))
				return nil
			}); replayErr != nil {
				return replayErr
			}
			// idb.memtables = append(idb.memtables, mem)
			// idb.wal = append(idb.wal, wal)
		}

		if ext == KeyExtension {
			base := path[:len(path)-len(KeyExtension)]
			lv := extractLevel(base)

			// malfored path
			if lv < 0 || int(lv) >= MaxLevels {
				return nil
			}

			curr, initErr := page.InitPage(path, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
			if initErr != nil {
				return initErr
			}

			val, valErr := page.InitPage(base+ValExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
			if valErr != nil {
				curr.Close()
				return valErr
			}

			totalPages := curr.GetLastPage()

			// Min is 2
			if totalPages < 2 {
				curr.Close()
				val.Close()
				return errors.New("sstable too small (missing index/magic): " + path)
			}

			magicPageNum := uint16(totalPages - 1)
			indexPageNum := uint16(totalPages - 2)
			dataPages := uint32(totalPages - 2)

			mgData, _, mgErr := curr.Read(magicPageNum)
			if mgErr != nil {
				return mgErr
			}
			if len(mgData) < 4 || binary.BigEndian.Uint32(mgData[:4]) != uint32(MagicNumber) {
				curr.Close()
				val.Close()
				return errors.New("sstable corrupted (bad magic): " + path)
			}

			indexData, _, idxErr := curr.Read(indexPageNum)
			if idxErr != nil {
				return idxErr
			}
			index, idxErr := DeserializeIndex(bytes.NewBuffer(indexData))
			if idxErr != nil {
				return idxErr
			}

			bfData, bfErr := os.ReadFile(base + BloomExtension)
			if bfErr != nil {
				return bfErr
			}
			bf, bfErr := filter.Deserialize(bytes.NewBuffer(bfData))
			if bfErr != nil {
				return bfErr
			}

			sst := &SSTABLE{
				name:      base,
				keys:      curr,
				vals:      val,
				filter:    bf,
				index:     index,
				dataPages: dataPages,
				size:      uint64(SstableSize) * uint64(SizeMultiple*(int(lv)+1)),
			}
			idb.sstables[lv] = append(idb.sstables[lv], sst)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// no mem found
	if idb.currMem == nil {
		idb.currMem = idb.newMemtableState(true)
	}

	go idb.compact(idb.done)
	return idb, nil
}

func (idb *IrisDB) Close() error {
	close(idb.done)
	idb.mu.Lock()
	defer idb.mu.Unlock()

	idb.currMem.wal.Close()

	for _, level := range idb.sstables {
		for _, sst := range level {
			if sst != nil {
				sst.keys.Close()
				sst.vals.Close()
			}
		}
	}
	return nil
}

/*

	Even SkipList & Page are threadSafe
	Some operation on DB take too long lock Path
	TODO: Try to optimize to hold lock shorter

*/

func (idb *IrisDB) Put(key, value []byte) error {

	return idb.put(key, value, nil)
}

// PutSeq writes key → value using a Hint
// the caller must not share a Hint across goroutines.
func (idb *IrisDB) PutSeq(key, value []byte, hint *Hint) error {
	return idb.put(key, value, hint)
}

// Delete Key (tombstone).
func (idb *IrisDB) Delete(key []byte) error {
	return idb.put(key, TOMPOSTONE, nil)
}

// helper function
func (idb *IrisDB) put(key, value []byte, hint *Hint) error {

	// TODO: Try to reduce Lock path

	idb.mu.Lock()
	defer idb.mu.Unlock()

	tsKey := db.NewKey(key)

	if _, err := idb.currMem.wal.Write(&LogEntry{Op: OpPut, Key: tsKey, Value: value}); err != nil {
		return err
	}

	var err error
	// no hint given
	if hint == nil {
		err = idb.currMem.m.Insert(tsKey, db.NewValue(value))
	} else {
		err = idb.currMem.m.InsertWithHints(tsKey, db.NewValue(value), hint.inner)
	}

	if err != nil {
		return err
	}

	return idb.maybeFlush()
}

func (idb *IrisDB) maybeFlush() error {
	if idb.currMem.m.GetSize() >= uint32(MemTableSize)/10 {
		return nil
	}
	if idb.frozenMem != nil {
		return nil // previous flush still running
	}
	next := idb.newMemtableState(true)
	if next == nil {
		return errors.New("failed to create memtable")
	}
	// Swap happens here under mu.Lock (held by put), so new writes go to
	// the fresh arena immediately without racing with the flush goroutine.
	frozen := idb.currMem
	idb.frozenMem = frozen
	idb.currMem = next
	go idb.flushFrozen(frozen)
	return nil
}

func (idb *IrisDB) flushFrozen(frozen *MemTableState) {
	sst, err := memToSST(frozen.m, 0, idb.dbPath)
	if err == nil {
		idb.mu.Lock()
		idb.sstables[0] = append(idb.sstables[0], sst)
		idb.mu.Unlock()
	}

	idb.muMem.Lock()
	if idb.frozenMem == frozen {
		idb.frozenMem = nil
	}
	idb.muMem.Unlock()

	walPath := frozen.wal.Path()
	frozen.wal.Close()
	os.Remove(walPath)
	frozen.m.Close()
}

func (idb *IrisDB) newMemtableState(createWal bool) *MemTableState {

	var w *WAL
	var err error
	if createWal {
		walName := fmt.Sprintf("%s/%s-%d%s", idb.dbPath, DBName, time.Now().UnixNano(), WalExtension)
		w, err = NewWal(walName, uint16(PageSize), Fsync, SyncInterval)
		if err != nil {
			return nil
		}
	}

	memState := &MemTableState{
		wal: w,
		m:   skiplist.NewSkipList(uint32(MemTableSize)),
	}
	return memState
}

func (idb *IrisDB) Get(key []byte) ([]byte, error) {
	searchKey := db.NewKeyAt(key, math.MaxUint64)

	idb.mu.RLock()
	defer idb.mu.RUnlock()

	it := skiplist.Newiterator(idb.currMem.m, math.MaxUint64)
	val, found := it.Get(searchKey)
	it.Close()
	if found {
		v := val.GetValue()
		if bytes.Equal(v, TOMPOSTONE) {
			return nil, nil
		}
		return v, nil
	}

	idb.muMem.RLock()
	defer idb.muMem.RUnlock()

	if idb.frozenMem != nil {
		it = skiplist.Newiterator(idb.frozenMem.m, math.MaxUint64)
		val, found = it.Get(searchKey)
		it.Close()
		if found {
			v := val.GetValue()
			if bytes.Equal(v, TOMPOSTONE) {
				return nil, nil
			}
			return v, nil
		}
	}

	for _, sstLevel := range idb.sstables {
		for _, sst := range sstLevel {
			if sst == nil {
				continue
			}
			data, found, err := sst.find(key)
			if err != nil {
				return nil, err
			}
			if found {
				if bytes.Equal(data, TOMPOSTONE) {
					return nil, nil
				}
				return data, nil
			}
		}
	}
	return nil, nil
}

// TODO: Optimze (GetBefore + GetRange)
// Try to jump to exact memtable or sstable directly
// // Maybe store some metadata about Ts Range for everyone

// GetBefore returns newest Entry <= ts
// nil if not found
func (idb *IrisDB) GetBefore(key []byte, ts time.Time) (*Entry, error) {

	tsNano := uint64(ts.UnixNano())
	searchKey := db.NewKeyAt(key, math.MaxUint64)

	idb.mu.RLock()
	defer idb.mu.RUnlock()

	it := skiplist.Newiterator(idb.currMem.m, tsNano)
	it.Get(searchKey)
	if it.Valid() && bytes.Equal(it.RawKey(), key) && it.Timestamp() <= tsNano {
		v := it.GetVal()
		writtenAt := time.Unix(0, int64(it.Timestamp()))
		it.Close()
		if bytes.Equal(v, TOMPOSTONE) {
			return nil, nil
		}
		cp := make([]byte, len(v))
		copy(cp, v)
		return &Entry{Value: cp, Time: writtenAt}, nil
	}
	it.Close()

	idb.muMem.RLock()
	defer idb.muMem.RUnlock()

	if idb.frozenMem != nil {
		it = skiplist.Newiterator(idb.frozenMem.m, tsNano)
		it.Get(searchKey)
		if it.Valid() && bytes.Equal(it.RawKey(), key) && it.Timestamp() <= tsNano {
			v := it.GetVal()
			writtenAt := time.Unix(0, int64(it.Timestamp()))
			it.Close()
			if bytes.Equal(v, TOMPOSTONE) {
				return nil, nil
			}
			cp := make([]byte, len(v))
			copy(cp, v)
			return &Entry{Value: cp, Time: writtenAt}, nil
		}
		it.Close()
	}

	for _, sstLevel := range idb.sstables {
		for _, sst := range sstLevel {
			if sst == nil {
				continue
			}
			data, entryTs, found, err := sst.findBeforeWithTs(key, tsNano)
			if err != nil {
				return nil, err
			}
			if found {
				if bytes.Equal(data, TOMPOSTONE) {
					return nil, nil
				}
				return &Entry{Value: data, Time: time.Unix(0, int64(entryTs))}, nil
			}
		}
	}
	return nil, nil
}

// GetRange returns all Entries in [ts1, ts2]
func (idb *IrisDB) GetRange(key []byte, ts1, ts2 time.Time) ([]*Entry, error) {
	ts1Nano := uint64(ts1.UnixNano())
	ts2Nano := uint64(ts2.UnixNano())
	searchKey := db.NewKeyAt(key, math.MaxUint64)

	// READ LOCK
	// Prevent corrputed read values
	idb.mu.RLock()
	defer idb.mu.RUnlock()

	var results []*Entry

	// Loop from recent
	it := skiplist.Newiterator(idb.currMem.m, math.MaxUint64)
	it.SeekToKey(searchKey)
	for it.Valid() {
		rk := it.RawKey()
		if !bytes.Equal(rk, key) {
			break
		}
		kTs := it.Timestamp()
		if kTs < ts1Nano {
			// If we reach kTs < ts1 in N memtable
			// All [:N-1] Memtables + SSTables already out of Range
			break
		}
		if kTs >= ts1Nano && kTs <= ts2Nano {
			v := it.GetVal()
			if !bytes.Equal(v, TOMPOSTONE) {
				cp := make([]byte, len(v))
				copy(cp, v)
				results = append(results, &Entry{Value: cp, Time: time.Unix(0, int64(kTs))})
			}
		}
		it.Next()
	}
	it.Close()

	idb.muMem.RLock()

	if idb.frozenMem != nil {
		it = skiplist.Newiterator(idb.frozenMem.m, math.MaxUint64)
		it.SeekToKey(searchKey)
		for it.Valid() {
			rk := it.RawKey()
			if !bytes.Equal(rk, key) {
				break
			}
			kTs := it.Timestamp()
			if kTs < ts1Nano {
				// If we reach kTs < ts1 in N memtable
				// All [:N-1] Memtables + SSTables already out of Range
				break
			}
			if kTs >= ts1Nano && kTs <= ts2Nano {
				v := it.GetVal()
				if !bytes.Equal(v, TOMPOSTONE) {
					cp := make([]byte, len(v))
					copy(cp, v)
					results = append(results, &Entry{Value: cp, Time: time.Unix(0, int64(kTs))})
				}
			}
			it.Next()
		}
		it.Close()
	}

	idb.muMem.RUnlock()

	// Here it is okay. we go from lvl 0 to Max
	for _, sstLevel := range idb.sstables {
		for _, sst := range sstLevel {
			if sst == nil {
				continue
			}
			entries, err := sst.findRangeEntries(key, ts1Nano, ts2Nano)
			if err != nil {
				return nil, err
			}
			results = append(results, entries...)
		}
	}

	// Results already sorted.
	// It is a granutee by design
	return results, nil
}

// Scan returns all Entries [start, end]
// GetRange but for keys
func (idb *IrisDB) Scan(start, end []byte) ([]*Entry, error) {
	idb.mu.RLock()
	defer idb.mu.RUnlock()

	type candidate struct {
		val []byte
		ts  uint64
	}
	seen := make(map[string]*candidate)

	scanMem := func(m *skiplist.SkipList) {
		it := skiplist.Newiterator(m, math.MaxUint64)
		if start != nil {
			it.SeekToKey(db.NewKeyAt(start, math.MaxUint64))
		} else {
			it.SeekToStart()
		}
		for it.Valid() {
			rk := it.RawKey()
			if end != nil && bytes.Compare(rk, end) > 0 {
				break
			}
			k := string(rk)
			ts := it.Timestamp()
			if c, exists := seen[k]; !exists || ts > c.ts {
				v := it.GetVal()
				cp := make([]byte, len(v))
				copy(cp, v)
				seen[k] = &candidate{val: cp, ts: ts}
			}
			it.Next()
		}
		it.Close()
	}

	scanMem(idb.currMem.m)

	idb.muMem.RLock()
	if idb.frozenMem != nil {
		scanMem(idb.frozenMem.m)
	}
	idb.muMem.RUnlock()

	for _, sstLevel := range idb.sstables {
		for _, sst := range sstLevel {
			if sst == nil {
				continue
			}
			keys, err := sst.allKeys()
			if err != nil {
				return nil, err
			}
			for _, ke := range keys {
				// ke : [valPgNum(2) | rawKey | ts(8)]
				if len(ke) < 11 { // 2 + min 1 raw byte + 8
					continue
				}
				internal := ke[2:]            // rawKey + ts(8)
				rawKey := db.RawKey(internal) // strips ts
				ts := db.GetTsAsUint64(internal)
				if start != nil && bytes.Compare(rawKey, start) < 0 {
					continue
				}
				if end != nil && bytes.Compare(rawKey, end) > 0 {
					continue
				}
				k := string(rawKey)
				if c, exists := seen[k]; exists && c.ts >= ts {
					continue
				}
				valPgNum := binary.BigEndian.Uint16(ke[:2])
				val, _, err := sst.vals.Read(valPgNum)
				if err != nil {
					return nil, err
				}
				cp := make([]byte, len(val))
				copy(cp, val)
				seen[k] = &candidate{val: cp, ts: ts}
			}
		}
	}

	sortedKeys := make([]string, 0, len(seen))
	for k := range seen {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	results := make([]*Entry, 0, len(sortedKeys))
	for _, k := range sortedKeys {
		c := seen[k]
		if bytes.Equal(c.val, TOMPOSTONE) {
			continue
		}
		results = append(results, &Entry{
			Key:   []byte(k),
			Value: c.val,
			Time:  time.Unix(0, int64(c.ts)),
		})
	}
	return results, nil
}

// Helper Functions
func (sst *SSTABLE) find(rawKey []byte) ([]byte, bool, error) {
	if sst.filter == nil || sst.index == nil || len(rawKey) == 0 {
		return nil, false, nil
	}
	if !sst.filter.Contains(rawKey) {
		return nil, false, nil
	}
	return sst.readBlock(rawKey, func(block *Block) (uint16, bool) {
		return block.findBefore(rawKey, math.MaxUint64)
	})
}

func (sst *SSTABLE) findRangeEntries(rawKey []byte, ts1, ts2 uint64) ([]*Entry, error) {
	if sst.index == nil || len(rawKey) == 0 {
		return nil, nil
	}
	pgNum, found := sst.index.find(rawKey[0])
	if !found {
		return nil, nil
	}
	dx, _, err := sst.keys.Read(uint16(pgNum))
	if err != nil {
		return nil, err
	}
	if Compression {
		dx = decompress(dx)
	}
	block, err := DeserializeBlock(bytes.NewBuffer(dx))
	if err != nil {
		return nil, err
	}
	pgNums, timestamps := block.findRangeWithTs(rawKey, ts1, ts2)
	var entries []*Entry
	for i, pg := range pgNums {
		v, _, err := sst.vals.Read(pg)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(v, TOMPOSTONE) {
			cp := make([]byte, len(v))
			copy(cp, v)
			entries = append(entries, &Entry{Value: cp, Time: time.Unix(0, int64(timestamps[i]))})
		}
	}
	return entries, nil
}

func (sst *SSTABLE) readBlock(rawKey []byte, fn func(*Block) (uint16, bool)) ([]byte, bool, error) {
	pgNum, found := sst.index.find(rawKey[0])
	if !found {
		return nil, false, nil
	}
	dx, _, err := sst.keys.Read(uint16(pgNum))
	if err != nil {
		return nil, false, err
	}
	if Compression {
		dx = decompress(dx)
	}
	block, err := DeserializeBlock(bytes.NewBuffer(dx))
	if err != nil {
		return nil, false, err
	}
	valPg, ok := fn(block)
	if !ok {
		return nil, false, nil
	}
	val, _, err := sst.vals.Read(valPg)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (sst *SSTABLE) readBlockWithTs(rawKey []byte, fn func(*Block) (uint16, uint64, bool)) ([]byte, uint64, bool, error) {
	pgNum, found := sst.index.find(rawKey[0])
	if !found {
		return nil, 0, false, nil
	}
	dx, _, err := sst.keys.Read(uint16(pgNum))
	if err != nil {
		return nil, 0, false, err
	}
	if Compression {
		dx = decompress(dx)
	}
	block, err := DeserializeBlock(bytes.NewBuffer(dx))
	if err != nil {
		return nil, 0, false, err
	}
	valPg, ts, ok := fn(block)
	if !ok {
		return nil, 0, false, nil
	}
	val, _, err := sst.vals.Read(valPg)
	if err != nil {
		return nil, 0, false, err
	}
	return val, ts, true, nil
}

func (sst *SSTABLE) findBeforeWithTs(rawKey []byte, ts uint64) ([]byte, uint64, bool, error) {
	if sst.index == nil || len(rawKey) == 0 {
		return nil, 0, false, nil
	}
	return sst.readBlockWithTs(rawKey, func(block *Block) (uint16, uint64, bool) {
		return block.findBeforeWithTs(rawKey, ts)
	})
}

func (sst *SSTABLE) allKeys() ([][]byte, error) {
	var keys [][]byte
	if sst.dataPages == 0 {
		return nil, nil
	}
	it := page.Newiterator(sst.keys, 2) // skip last 2 pages: IndexBlock + Magic
	for it.Valid() {
		pgNum := it.Next()
		data, err := it.Get(uint16(pgNum))
		if err != nil {
			break
		}
		if Compression {
			data = decompress(data)
		}
		block, err := DeserializeBlock(bytes.NewBuffer(data))
		if err != nil {
			break
		}
		keys = append(keys, block.Keys...)
	}
	return keys, nil
}

func NewSSTABLE(level int, dbPath string) (*SSTABLE, error) {
	name := fmt.Sprintf("%s/%s-%02d-%d", dbPath, DBName, level, time.Now().UnixNano())
	keys, err := page.InitPage(name+KeyExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
	if err != nil {
		return nil, err
	}
	vals, err := page.InitPage(name+ValExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
	if err != nil {
		keys.Close()
		return nil, err
	}

	n := uint32(SstableSize) * uint32(SizeMultiple*(level+1)) / uint32(AvgKeySize)
	bf, err := filter.NewBloomFilter(n, FalsePostiveProb)
	if err != nil {
		keys.Close()
		vals.Close()
		return nil, err
	}

	return &SSTABLE{
		name:   name,
		keys:   keys,
		vals:   vals,
		filter: bf,
		index:  &IndexBlock{},
		size:   uint64(SstableSize) * uint64(SizeMultiple*(level+1)),
	}, nil
}

func writeBlock(sst *SSTABLE, block *Block, firstByte byte) error {
	data, err := block.SerializeBlock()
	if err != nil {
		return err
	}
	if Compression {
		data = compress(data)
	}
	pgNum, err := sst.keys.Write(data)
	if err != nil {
		return err
	}
	sst.index.Entries = append(sst.index.Entries, IndexEntry{Key: firstByte, Off: pgNum})
	sst.dataPages++
	return nil
}

func finalizeSSTABLE(sst *SSTABLE) (*SSTABLE, error) {
	indexBytes, err := sst.index.SerializeIndex()
	if err != nil {
		return nil, err
	}
	if _, err = sst.keys.Write(indexBytes); err != nil {
		return nil, err
	}

	bfBytes, err := sst.filter.Serialize()
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(sst.name+BloomExtension, bfBytes, os.FileMode(Permission)); err != nil {
		return nil, err
	}

	magicBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(magicBuf, uint32(MagicNumber))
	if _, err = sst.keys.Write(magicBuf); err != nil {
		return nil, err
	}
	return sst, nil
}

// memToSST -->  frozen memtable To SST
func memToSST(mem *skiplist.SkipList, level int, dbPath string) (*SSTABLE, error) {
	sst, err := NewSSTABLE(level, dbPath)
	if err != nil {
		return nil, err
	}

	it := skiplist.Newiterator(mem, math.MaxUint64)
	it.SeekToStart()

	var currentBlock *Block
	var currentFirstByte byte
	isFirst := true

	for it.Valid() {
		key := it.GetKey()   // rawKey + ts(8)
		value := it.GetVal() // user value bytes

		rawKey := db.RawKey(key)
		sst.filter.Add(rawKey)

		valPg, err := sst.vals.Write(value)
		if err != nil {
			it.Close()
			return nil, err
		}

		storedKey := make([]byte, 2+len(key))
		binary.BigEndian.PutUint16(storedKey[:2], uint16(valPg))
		copy(storedKey[2:], key)

		firstByte := rawKey[0]
		if isFirst || firstByte != currentFirstByte {
			if !isFirst && currentBlock != nil {
				if err := writeBlock(sst, currentBlock, currentFirstByte); err != nil {
					it.Close()
					return nil, err
				}
			}
			currentBlock = &Block{}
			currentFirstByte = firstByte
			isFirst = false
		}
		currentBlock.Keys = append(currentBlock.Keys, storedKey)

		it.Next()
	}
	it.Close()

	if !isFirst && currentBlock != nil {
		if err := writeBlock(sst, currentBlock, currentFirstByte); err != nil {
			return nil, err
		}
	}

	return finalizeSSTABLE(sst)
}

func (idb *IrisDB) compact(done <-chan struct{}) {
	ticker := time.NewTicker(CompactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			for level := 0; level < MaxLevels-1; level++ {
				if idb.levelFull(level) {
					_ = idb.compactLevel(level)
				}
			}
		}
	}
}

func (idb *IrisDB) levelFull(level int) bool {
	idb.mu.RLock()
	defer idb.mu.RUnlock()
	var total uint64
	for _, sst := range idb.sstables[level] {
		if sst == nil {
			continue
		}
		total += uint64(sst.keys.Size()) + uint64(sst.vals.Size())
	}
	return total > uint64(SstableSize)*uint64(SizeMultiple*(level+1))
}

func (idb *IrisDB) compactLevel(level int) error {
	idb.mu.RLock()
	src := make([]*SSTABLE, 0, len(idb.sstables[level])+len(idb.sstables[level+1]))
	for _, s := range idb.sstables[level] {
		if s != nil {
			src = append(src, s)
		}
	}
	for _, s := range idb.sstables[level+1] {
		if s != nil {
			src = append(src, s)
		}
	}
	idb.mu.RUnlock()

	if len(src) == 0 {
		return nil
	}

	iter := NewSSTMergeIterator(src, level+1, idb.dbPath)
	newSST, err := iter.CreateSST()
	if err != nil {
		return err
	}

	idb.mu.Lock()
	for _, sst := range src {
		sst.keys.Close()
		sst.vals.Close()
		os.Remove(sst.name + KeyExtension)
		os.Remove(sst.name + ValExtension)
		os.Remove(sst.name + BloomExtension)
	}
	idb.sstables[level] = nil
	idb.sstables[level+1] = []*SSTABLE{newSST}
	idb.mu.Unlock()

	return nil
}

// Merge iterator

type SSTMergeIterator struct {
	heap   *MinHeap
	level  int
	dbPath string
}

type HeapItem struct {
	key  []byte
	vals *page.Page
	id   int
}

type MinHeap []*HeapItem

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool {

	ki := h[i].key[2:]
	kj := h[j].key[2:]
	cmp := db.CompareKeys(ki, kj)
	if cmp < 0 {
		return true
	}
	if cmp == 0 {
		return h[i].id < h[j].id
	}
	return false
}

func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x any) { *h = append(*h, x.(*HeapItem)) }

func (h *MinHeap) Pop() any {
	n := len(*h)
	item := (*h)[n-1]
	(*h)[n-1] = nil
	*h = (*h)[:n-1]
	return item
}

func NewSSTMergeIterator(sstables []*SSTABLE, level int, dbPath string) *SSTMergeIterator {
	h := &MinHeap{}
	heap.Init(h)

	for id, sst := range sstables {
		if sst == nil {
			continue
		}
		keys, err := sst.allKeys()
		if err != nil {
			continue
		}
		for _, key := range keys {
			heap.Push(h, &HeapItem{key: key, vals: sst.vals, id: id})
		}
	}
	return &SSTMergeIterator{heap: h, level: level, dbPath: dbPath}
}

func (smi *SSTMergeIterator) Next() ([]byte, *page.Page, error) {
	if smi.heap.Len() == 0 {
		return nil, nil, errors.New("empty heap")
	}
	v := heap.Pop(smi.heap).(*HeapItem)

	// Skip exact duplicates
	for smi.heap.Len() > 0 {
		top := (*smi.heap)[0]
		if bytes.Equal(top.key[2:], v.key[2:]) {
			heap.Pop(smi.heap)
			continue
		}
		break
	}

	return v.key, v.vals, nil
}

func (smi *SSTMergeIterator) CreateSST() (*SSTABLE, error) {
	sst, err := NewSSTABLE(smi.level, smi.dbPath)
	if err != nil {
		return nil, err
	}

	var currentBlock *Block
	var currentFirstByte byte
	isFirst := true

	for smi.heap.Len() > 0 {
		key, srcVals, err := smi.Next()
		if err != nil {
			break
		}

		valPgNum := binary.BigEndian.Uint16(key[:2])
		val, _, err := srcVals.Read(valPgNum)
		if err != nil {
			return nil, err
		}

		// Drop tombstones at deepeset level. GoodBye
		if bytes.Equal(val, TOMPOSTONE) && smi.level == MaxLevels-1 {
			continue
		}

		rawKey := db.RawKey(key[2:])
		sst.filter.Add(rawKey)

		newValPg, err := sst.vals.Write(val)
		if err != nil {
			return nil, err
		}

		storedKey := make([]byte, len(key))
		copy(storedKey, key)
		binary.BigEndian.PutUint16(storedKey[:2], uint16(newValPg))

		firstByte := rawKey[0]
		if isFirst || firstByte != currentFirstByte {
			if !isFirst && currentBlock != nil {
				if err := writeBlock(sst, currentBlock, currentFirstByte); err != nil {
					return nil, err
				}
			}
			currentBlock = &Block{}
			currentFirstByte = firstByte
			isFirst = false
		}
		currentBlock.Keys = append(currentBlock.Keys, storedKey)
	}

	if !isFirst && currentBlock != nil {
		if err := writeBlock(sst, currentBlock, currentFirstByte); err != nil {
			return nil, err
		}
	}

	return finalizeSSTABLE(sst)
}

func extractLevel(path string) int64 {
	base := filepath.Base(path)
	splits := strings.Split(base, "-")
	if len(splits) != 3 {
		return -1
	}
	if v, err := strconv.ParseInt(splits[1], 10, 64); err == nil {
		return v
	}
	return -1
}

func compress(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func decompress(compressed []byte) []byte {
	data, err := snappy.Decode(nil, compressed)
	if err != nil {
		return compressed
	}
	return data
}
