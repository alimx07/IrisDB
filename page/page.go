package page

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// TODO:
// 1- Add faster way for write (may use worker pools or go routines)
// 2- Find a way to decrease read func allocations

type Page struct {
	file     *os.File // Underline OS file
	close    chan struct{}
	wg       *sync.WaitGroup // Used to ensure all concurrent operations ended
	IsClosed atomic.Bool     // Make sure Page Closed from one Thread
	pageNum  atomic.Uint32   // Number of pages (filesize/pagesize)
	pageSize uint16          // Size of Page (up to 64KB)
	fsync    bool            // fsync or not
}

// Open/Create page for specific data
func InitPage(name string, flag int, perm os.FileMode, pageSize uint16, fsync bool, syncInterval time.Duration) (*Page, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	pg := &Page{
		file:     file,
		pageSize: pageSize,
		close:    make(chan struct{}),
	}
	pg.wg = &sync.WaitGroup{}
	if fsync {
		pg.fsync = true
		go pg.syncProcess(syncInterval)
	}
	return pg, nil
}

func (pg *Page) syncProcess(syncInterval time.Duration) {
	pg.wg.Add(1)
	defer pg.wg.Done()
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = pg.file.Sync()
		case <-pg.close:
			_ = pg.file.Sync()
			close(pg.close)
			return
		}
	}
}

// Write data into file and return pgNum
// (thread safe)
func (pg *Page) Write(data []byte) (uint32, error) {

	/*
		PAGE LAYOUT

		----------------------------------
		| DataSize | overflow | Data     |
		| 0 - 30   |  31 	  | 32 - end |
		----------------------------------
	*/

	/*
		WriteAt and ReadAt using Pwrite and Pread under the hood
		Using Pwrite and Pread is better than Seek + Read :
		1 - Lock Free (one atomic operation of seek and read)
		2 - ONE syscall for the kernel instead of TWO
	*/

	var err error
	var newP uint32

	pg.wg.Add(1)
	defer pg.wg.Done()

	// max allocation per function will be pageSize
	// despite of size of data size
	buf := make([]byte, pg.pageSize)
	// Data fits in One Page
	if len(data)+4 <= int(pg.pageSize) {

		newP = pg.newPage(1) - 1
		off := int64(newP * uint32(pg.pageSize))
		header := len(data) << 1

		binary.BigEndian.PutUint32(buf[0:], uint32(header))
		copy(buf[4:], data)
		pg.file.WriteAt(buf, off)

	} else {

		// split Data
		curr := 0
		n := int(math.Ceil(float64(len(data)) / float64(pg.pageSize-4)))
		newP = pg.newPage(uint32(n)) - uint32(n)

		// start filling the pages
		for i := range n {
			pgNum := newP + uint32(i)
			off := int64(pgNum * uint32(pg.pageSize))

			x := min(int(pg.pageSize-4), len(data)-curr)

			header := x<<1 | 1
			if i == n-1 {
				header = header & ^1
			}
			binary.BigEndian.PutUint32(buf[0:], uint32(header))
			copy(buf[4:], data[curr:curr+x])
			curr += x
			_, err = pg.file.WriteAt(buf, off)
			if err != nil {
				return 0, err
			}
		}
	}

	return newP, nil
}

// Read the data started from this pageNum
// (Thread Safe)
func (pg *Page) Read(pageNum uint16) ([]byte, uint16, error) {

	var data []byte
	header := make([]byte, 4)
	var err error
	for {

		off := int64(pageNum) * int64(pg.pageSize)
		_, err = pg.file.ReadAt(header, off)
		if err != nil {
			return nil, 0, err
		}

		h := binary.BigEndian.Uint32(header)

		curr := make([]byte, h>>1)

		_, err = pg.file.ReadAt(curr, off+4)
		if err != nil && err != io.EOF {
			return nil, 0, err
		}
		data = append(data, curr...)
		if (h & 1) == 0 {
			break
		}
		pageNum++
	}
	return data, pageNum, nil

}

func (pg *Page) Close() error {

	if !pg.IsClosed.CompareAndSwap(false, true) {

		// already closed
		return nil
	}

	// sync if sycn is ON
	if pg.fsync {
		pg.close <- struct{}{}
	}

	// wait everything to be Done
	pg.wg.Wait()

	err := pg.file.Close()
	if err != nil {
		return err
	}
	pg = nil
	return nil
}

// Return New PageNum after allocating pages atomically
func (pg *Page) newPage(delta uint32) uint32 {

	/*
	  Instead of allocating one page at the time
	  allocate all needed pages sequentially
	  this brings better data locality when reading some
	  data sits in more than one page
	*/
	return pg.pageNum.Add(delta)
}

func (pg *Page) GetLastPage() uint32 {
	return pg.pageNum.Load()
}

func (pg *Page) Size() uint32 {

	// estimation of Page curr size
	return pg.pageNum.Load() * uint32(pg.pageSize)
}

/*
Iterator is used to iterate Page Struct concurrently

Note : Iterator does not take a snapshot on Page at creation time so new data stored will be reflected in iterator
*/
type Iterator struct {
	pg      *Page
	currNum atomic.Uint32
}

func Newiterator(pg *Page) *Iterator {
	it := &Iterator{
		pg: pg,
	}
	return it
}

// Next() points Iterator to next Value in file
// and return pageNum
func (it *Iterator) Next() uint32 {
	return it.currNum.Load()
}

func (it *Iterator) Valid() bool {
	// current pageNum in page struct
	return it.currNum.Load() <= it.pg.pageNum.Load()
}

// return Curr value
func (it *Iterator) Get(pgNum uint16) ([]byte, error) {
	data, newPgNum, err := it.pg.Read(pgNum)
	if err != nil {
		return nil, err
	}
	it.currNum.Store(uint32(newPgNum))
	return data, nil
}
