package irisdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alimx07/IrisDB/filter"
	"github.com/alimx07/IrisDB/page"
	"github.com/alimx07/IrisDB/skiplist"
)

type SSTABLE struct {
	keys   *page.Page
	vals   *page.Page
	filter *filter.Bloomfilter
}

type IrisDB struct {
	sstables  [][]*SSTABLE
	memtables []*skiplist.SkipList
	wal       []*WAL
}

func OpenDB(dbPath string) error {

	db := &IrisDB{}

	var curr, key, val *page.Page

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
			db.wal = append(db.wal, &WAL{page: curr})
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
			bfData, _, err := key.Read(0)
			if err != nil {
				return err
			}

			// THink again
			bLen := binary.BigEndian.Uint16(bfData[0:2])
			buf := make([]byte, bLen)
			bf := filter.BytesTOSTruct(*bytes.NewBuffer(buf[2:]))

			sst := &SSTABLE{keys: key, vals: val, filter: bf}
			db.sstables[lv] = append(db.sstables[lv], sst)
		}
		return nil
	})

	return err
}

func NewSSTABLE(n uint32, level int) (*SSTABLE, error) {

	/*
		SSTABLE STRUCTURE
		-----------------------------------------------

		KEYS
		---------------------------------------------------------
		| Len(2) | BLoom Filter | KeyLen | Key | KeyLen | Key...|
		---------------------------------------------------------

		VALS
		---------------------------------
		| ValLen | Val | ValLen | Val... |
		---------------------------------

		NOTE : KEYS OR VALS CAN BE COMPRESSED (ROW COMPRESSION)
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
	f, err := filter.NewBloomFilter(n, FalsePostiveProb)
	if err != nil {
		return nil, err
	}

	return &SSTABLE{keys: keys, vals: vals, filter: f}, nil
}

func extractLevel(path string) int64 {
	splits := strings.Split(path, "-")
	if len(splits) != 3 {
		return -1 // err
	}
	if v, err := strconv.ParseInt(splits[1], 10, 64); err != nil {
		return v
	}
	return -1
}
