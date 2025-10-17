package irisdb

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alimx07/IrisDB/db"
	"github.com/alimx07/IrisDB/filter"
	"github.com/alimx07/IrisDB/page"
	"github.com/alimx07/IrisDB/skiplist"
	"github.com/klauspost/compress/snappy"
)

type SSTABLE struct {
	keys   *page.Page
	vals   *page.Page
	filter *filter.Bloomfilter
	index  *IndexBlock
	size   uint64
}

type IrisDB struct {
	sstables  [][]*SSTABLE
	memtables []*skiplist.SkipList
	wal       []*WAL
}

func OpenDB(dbPath string) (*IrisDB, error) {

	// db not found
	if _, err := os.Open(dbPath); err != nil {
		return nil, err
	}
	DB := &IrisDB{}

	var curr, key, val *page.Page

	sstables := make([][]*SSTABLE, MaxLevels)

	// Read Files in the DB
	err := filepath.WalkDir(dbPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		curr, err = page.InitPage(path, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
		if err != nil {
			return err
		}
		if ext == WalExtension {
			wal := &WAL{page: curr}
			mem := skiplist.NewSkipList(uint32(MemTableSize))
			wal.Replay(func(log *LogEntry) error {

				// TODO : Handle error
				mem.Insert(log.Key, db.NewValue(log.Value))
				return nil
			})
		}
		if ext == SSTABLEExtesnion {
			lv := extractLevel(path)
			if lv == -1 {
				return err
			}
			key, err = page.InitPage(path+KeyExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
			if err != nil {
				return err
			}
			val, err = page.InitPage(path+ValExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
			if err != nil {
				return err
			}

			lastPg := key.GetLastPage()
			mgNum, _, err := key.Read(uint16(lastPg))
			if err != nil {
				return err
			}
			magicNum := binary.BigEndian.Uint64(mgNum[:8])
			if magicNum != uint64(MagicNumber) {
				return errors.New("sstable data is corrupted")
			}

			// Load index and bf into memory
			indexAsBytes, _, err := key.Read(uint16(lastPg - 1))
			if err != nil {
				return err
			}
			bfAsBytes, _, err := key.Read(uint16(lastPg - 2))

			if err != nil {
				return err
			}
			bf := filter.Desrialize(*bytes.NewBuffer(bfAsBytes))
			index := DeserializeIndex(*bytes.NewBuffer(indexAsBytes))

			sst := &SSTABLE{keys: key, vals: val, filter: bf, index: index, size: uint64(SstableSize) * uint64(SizeMultiple*(int(lv)+1))}
			sstables[lv] = append(sstables[lv], sst)
		}
		DB.sstables = sstables
		return nil
	})
	if err != nil {
		return nil, err
	}
	go DB.compact()
	return DB, err
}

func (DB *IrisDB) compact() {

}

func NewSSTABLE(level int) (*SSTABLE, error) {

	/*
		SSTABLE STRUCTURE
		-----------------------------------------------

		KEYS
		------------------------------------------------------------------------------
		| Block | Block | .... | Bloom filter | Index (e.g: firstLetter --> (offset) |
		------------------------------------------------------------------------------

		VALS
		------------------
		| Val | Val |....|
		------------------

		NOTE : KEYS OR VALS CAN BE COMPRESSED
	*/
	name := fmt.Sprintf("%s-%02d-%d", DBName, level, time.Now().Second())
	keys, err := page.InitPage(name+KeyExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
	if err != nil {
		return nil, err
	}
	vals, err := page.InitPage(name+ValExtension, Flag, os.FileMode(Permission), uint16(PageSize), Fsync, SyncInterval)
	if err != nil {
		return nil, err
	}
	return &SSTABLE{keys: keys, vals: vals, size: uint64(SstableSize) * uint64(SizeMultiple*(level+1))}, nil
}

func (sst *SSTABLE) find(key []byte) ([]byte, bool, error) {
	found := sst.filter.Contains(key)
	if !found {
		return nil, false, nil
	}
	pgNum, found := sst.index.find(key[0])
	if !found {
		return nil, false, nil
	}
	dx, _, err := sst.keys.Read(uint16(pgNum))
	if err != nil {
		return nil, false, err
	}
	var block *Block
	if Compression {
		dx = decompress(dx)
	}
	block = DeserializeBlock(*bytes.NewBuffer(dx))
	valPgNum, found := block.find(key)
	if !found {
		return nil, false, nil
	}
	val, _, err := sst.vals.Read(valPgNum)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (sst *SSTABLE) fullSize() bool {
	return sst.size <= uint64(sst.keys.Size())+uint64(sst.vals.Size())
}

// Merge N sstables
type SSTMergeIterator struct {
	heap  *MinHeap
	vals  map[int]*page.Page
	level int
}

type HeapItem struct {
	it  *page.Iterator
	key []byte
	id  int // sstable ID

}

// Implement Heap Interface

type MinHeap []*HeapItem

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp < 0 {
		return true
	}
	if cmp == 0 {
		return h[i].id < h[j].id
	}
	return false
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(*HeapItem))
}

func (h *MinHeap) Pop() any {
	n := len(*h)
	item := (*h)[n-1]
	(*h)[n-1] = nil
	*h = (*h)[:n-1]
	return item
}

func NewSSTMergeIterator(sstables []*SSTABLE, level int) *SSTMergeIterator {

	h := &MinHeap{}
	heap.Init(h)
	vals := make(map[int]*page.Page)

	for id, sst := range sstables {
		it := page.Newiterator(sst.keys)
		if !it.Valid() {
			continue
		}
		key, _ := it.Get(uint16(it.Next()))
		heapItem := &HeapItem{
			key: key,
			id:  id,
			it:  it,
		}
		heap.Push(h, heapItem)
		vals[id] = sst.vals
	}
	return &SSTMergeIterator{heap: h, vals: vals, level: level}
}

func (smi *SSTMergeIterator) Next() ([]byte, int, error) {
	if smi.heap.Len() <= 0 {
		return nil, 0, errors.New("emtpy heap")
	}
	v := heap.Pop(smi.heap).(*HeapItem)
	for smi.heap.Len() > 0 {
		nxPop := heap.Pop(smi.heap).(*HeapItem)
		if bytes.Equal(nxPop.key, v.key) {
			if nxPop.it.Valid() {
				nx := nxPop.it.Next()
				nxItem, _ := nxPop.it.Get(uint16(nx))
				smi.heap.Push(&HeapItem{
					key: nxItem, id: nxPop.id, it: nxPop.it,
				})
			}
			continue
		}
		heap.Push(smi.heap, nxPop)
		break
	}
	if v.it.Valid() {
		nx := v.it.Next()
		nxItem, _ := v.it.Get(uint16(nx))
		smi.heap.Push(&HeapItem{
			key: nxItem, id: v.id, it: v.it,
		})
	}
	return v.key, v.id, nil
}

func (smi *SSTMergeIterator) CreateSST() ([]*SSTABLE, error) {

	var sstables []*SSTABLE
	sst, err := NewSSTABLE(smi.level)
	if err != nil {
		return nil, err
	}
	mp := make(map[byte]bool)
	for smi.heap.Len() > 0 {
		key, id, err := smi.Next()
		if err != nil {
			return nil, err
		}
		pgNum := binary.BigEndian.Uint16(key[:2])
		val, _, err := smi.vals[id].Read(pgNum)
		if err != nil {
			return nil, err
		}
		sst.filter.Add(key[2:])
		newPg, err := sst.vals.Write(val)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint16(key[:2], uint16(newPg))
		keyPg, err := sst.keys.Write(key)
		if !mp[key[2]] {
			sst.index.entries = append(sst.index.entries, IndexEntry{
				key: key[2],
				off: keyPg,
			})
			mp[key[2]] = true
		}

		if err != nil {
			return nil, err
		}
		if sst.fullSize() {

			// IndexBlock maxSize := 256 * 8
			// intiallize buf of size fitler once
			// it will handle both
			buf := make([]byte, sst.size)

			sst.index.SerializeIndex(*bytes.NewBuffer(buf))
			_, err = sst.keys.Write(buf)
			if err != nil {
				return nil, err
			}
			sst.filter.Serialize(*bytes.NewBuffer(buf))
			_, err = sst.keys.Write(buf)
			if err != nil {
				return nil, err
			}
			binary.BigEndian.PutUint32(buf[:8], MagicNumber)
			_, err = sst.keys.Write(buf[:8])
			if err != nil {
				return nil, err
			}
			sstables = append(sstables, sst)
			sst, _ = NewSSTABLE(smi.level)
		}
	}
	return sstables, nil
}

func extractLevel(path string) int64 {
	splits := strings.Split(path, "-")
	if len(splits) != 3 {
		return -1 // err
	}
	if v, err := strconv.ParseInt(splits[1], 10, 64); err == nil {
		return v
	}
	return -1
}

// TODO:
// avoid suddenly flush when read
func (db *IrisDB) Read(key []byte) ([]byte, error) {
	for _, mem := range db.memtables {

		val, found := mem.Get(key)

		if !found {
			continue
		}
		v := val.GetValue()
		if !bytes.Equal(v, TOMPOSTONE) {
			return v, nil
		}
		return nil, nil
	}
	for _, sstLevel := range db.sstables {
		for _, sst := range sstLevel {
			data, found, _ := sst.find(key)
			if found {
				return data, nil
			}
		}
	}
	return nil, nil
}

func compress(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func decompress(compressed []byte) []byte {
	data, err := snappy.Decode(nil, compressed)
	if err != nil {

		// return same data in case of error
		return compressed
	}
	return data
}
