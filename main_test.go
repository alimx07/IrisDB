package irisdb

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func openTestDB(t *testing.T) (*IrisDB, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "irisdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return db, dir
}

func cleanup(db *IrisDB, dir string) {
	db.Close()
	os.RemoveAll(dir)
}

func TestPutGet(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	if err := db.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("foo"))
	if err != nil || !bytes.Equal(val, []byte("bar")) {
		t.Fatalf("got %s, err %v", val, err)
	}
}

func TestGetMissing(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	val, err := db.Get([]byte("ghost"))
	if err != nil || val != nil {
		t.Fatalf("expected nil, got %s err %v", val, err)
	}
}

func TestOverwrite(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))

	val, _ := db.Get([]byte("k"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected v2, got %s", val)
	}
}

func TestDelete(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	db.Put([]byte("del"), []byte("data"))
	db.Delete([]byte("del"))

	val, err := db.Get([]byte("del"))
	if err != nil || val != nil {
		t.Fatalf("expected nil after delete, got %s err %v", val, err)
	}
}

func TestPutSeqHint(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	h := NewHint()
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("seq-%04d", i))
		if err := db.PutSeq(k, []byte("val"), h); err != nil {
			t.Fatal(err)
		}
	}
	val, _ := db.Get([]byte("seq-0050"))
	if !bytes.Equal(val, []byte("val")) {
		t.Fatalf("expected val, got %s", val)
	}
}

func TestGetBefore(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	t0 := time.Now()
	db.Put([]byte("ts"), []byte("first"))
	t1 := time.Now()
	time.Sleep(time.Millisecond)
	db.Put([]byte("ts"), []byte("second"))

	e, err := db.GetBefore([]byte("ts"), t1)
	if err != nil {
		t.Fatal(err)
	}
	if e == nil || !bytes.Equal(e.Value, []byte("first")) {
		t.Fatalf("expected first, got %v", e)
	}

	_ = t0
}

func TestGetBeforeMissing(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	before := time.Now().Add(-time.Second)
	db.Put([]byte("future"), []byte("v"))

	e, err := db.GetBefore([]byte("future"), before)
	if err != nil || e != nil {
		t.Fatalf("expected nil entry, got %v err %v", e, err)
	}
}

func TestGetRange(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	t0 := time.Now()
	db.Put([]byte("r"), []byte("a"))
	time.Sleep(time.Millisecond)
	t1 := time.Now()
	db.Put([]byte("r"), []byte("b"))
	time.Sleep(time.Millisecond)
	t2 := time.Now()
	db.Put([]byte("r"), []byte("c"))
	t3 := time.Now()

	entries, err := db.GetRange([]byte("r"), t1, t2)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !bytes.Equal(entries[0].Value, []byte("b")) {
		t.Fatalf("expected [b], got %v", entries)
	}

	all, _ := db.GetRange([]byte("r"), t0, t3)
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}
}

func TestScan(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		db.Put([]byte(k), []byte("v-"+k))
	}

	entries, err := db.Scan([]byte("banana"), []byte("date"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries [banana,cherry,date], got %d", len(entries))
	}
	if !bytes.Equal(entries[0].Key, []byte("banana")) || !bytes.Equal(entries[2].Key, []byte("date")) {
		t.Fatalf("unexpected scan order: %v", entries)
	}
}

func TestScanAll(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	for i := 0; i < 10; i++ {
		db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}
	db.Delete([]byte("k05"))

	entries, err := db.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 9 {
		t.Fatalf("expected 9 (delete excluded), got %d", len(entries))
	}
}

func TestWALReplay(t *testing.T) {
	dir, _ := os.MkdirTemp("", "irisdb-wal-*")
	defer os.RemoveAll(dir)

	db, err := OpenDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("persist"), []byte("yes"))
	db.Put([]byte("also"), []byte("me"))
	db.Close()

	db2, err := OpenDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, err := db2.Get([]byte("persist"))
	if err != nil || !bytes.Equal(val, []byte("yes")) {
		t.Fatalf("WAL replay failed: got %s err %v", val, err)
	}
	val2, _ := db2.Get([]byte("also"))
	if !bytes.Equal(val2, []byte("me")) {
		t.Fatalf("WAL replay second key failed: got %s", val2)
	}
}

func TestFlushToSST(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	// Write enough to trigger a flush (MemTableSize = 64KB)
	val := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("flush-%04d", i))
		if err := db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond) // let flush goroutine run

	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("flush-%04d", i))
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("key %s: %v", k, err)
		}
		if !bytes.Equal(v, val) {
			t.Fatalf("key %s: value mismatch", k)
		}
	}
}

func TestConcurrentPutGet(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	var wg sync.WaitGroup
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			k := []byte(fmt.Sprintf("c-%04d", i))
			db.Put(k, k)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("c-%04d", i))
		v, err := db.Get(k)
		if err != nil || !bytes.Equal(v, k) {
			t.Errorf("key %s: got %s err %v", k, v, err)
		}
	}
}

func TestDeleteThenPut(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	db.Put([]byte("x"), []byte("old"))
	db.Delete([]byte("x"))
	db.Put([]byte("x"), []byte("new"))

	val, _ := db.Get([]byte("x"))
	if !bytes.Equal(val, []byte("new")) {
		t.Fatalf("expected new, got %s", val)
	}
}

func TestMultipleKeys(t *testing.T) {
	db, dir := openTestDB(t)
	defer cleanup(db, dir)

	data := map[string]string{
		"alpha": "1", "beta": "2", "gamma": "3",
		"delta": "4", "epsilon": "5",
	}
	for k, v := range data {
		db.Put([]byte(k), []byte(v))
	}
	for k, v := range data {
		got, err := db.Get([]byte(k))
		if err != nil || !bytes.Equal(got, []byte(v)) {
			t.Errorf("%s: expected %s got %s err %v", k, v, got, err)
		}
	}
}
