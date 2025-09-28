package skiplist

// THIS IS INTIAL TESTS

// TODO:
// ADD More Edge cases tests

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alimx07/IrisDB/db"
)

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return b
}

type entry struct {
	k []byte
	v db.Value
}

func makeEntries(num, valSize int, rng *rand.Rand) []entry {
	entries := make([]entry, num)
	for i := 0; i < num; i++ {
		k := randomBytes(rng, 16)
		v := randomBytes(rng, valSize)
		entries[i] = entry{k: db.NewKey(k), v: db.NewValue(v)}
	}
	return entries
}

func makeEntriesSeq(num, valSize int, rng *rand.Rand) []entry {
	entries := make([]entry, num)
	for i := 0; i < num; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		v := randomBytes(rng, valSize)
		entries[i] = entry{k: db.NewKey(k), v: db.NewValue(v)}
	}
	return entries
}

func TestInsertGet(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	tests := makeEntries(1000, 128, rng)
	sl := NewSkipList(3 << 30)
	for _, entry := range tests {
		sl.Insert(entry.k, entry.v)
	}
	for _, entry := range tests {
		val := sl.Get(entry.k)
		if !bytes.Equal(val.GetValue(), entry.v.GetValue()) {
			t.Error(fmt.Printf("Wrong value for some Key. Expected %s, got %s", entry.v.GetValue(), val.GetValue()))
		}
	}
}

func TestMutlipleInsertSameKey(t *testing.T) {
	entries := make([]entry, 20)
	key := []byte("key")
	for i := range entries {
		entries[i].k = db.NewKey(key)
		entries[i].v = db.NewValue([]byte(fmt.Sprintf("Val-%d", i)))
	}
	sl := NewSkipList(1 << 20)
	for i := range entries {
		sl.Insert(entries[i].k, entries[i].v)
	}
	v := sl.Get(db.NewKey(key))
	if !bytes.Equal(v.GetValue(), entries[19].v.GetValue()) {
		t.Error(fmt.Printf("Wrong Value. Expected %s, got %s", entries[19].v.GetValue(), v.GetValue()))
	}
}

func TestGetNonExistent(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	tests := makeEntries(100, 128, rng)
	sl := NewSkipList(3 << 20)
	for _, entry := range tests {
		sl.Insert(entry.k, entry.v)
	}

	nonExistentKey := db.NewKey([]byte("NOT-FOUND"))
	val := sl.Get(nonExistentKey)
	if val.GetValue() != nil {
		t.Error("Expected nil for a non-existent key, but got a value")
	}
}

func TestInsertSequentially(t *testing.T) {
	entries := make([]entry, 10)
	for i := range entries {
		entries[i].k = db.NewKey([]byte(fmt.Sprintf("key-%d", i)))
		entries[i].v = db.NewValue([]byte("val"))
	}
	sl := NewSkipList(1 << 20)
	h := &Hint{}
	for _, entry := range entries {
		sl.InsertWithHints(entry.k, entry.v, h)
	}
	for _, entry := range entries {
		val := sl.Get(entry.k)
		if !bytes.Equal(val.GetValue(), entry.v.GetValue()) {
			t.Error(fmt.Printf("Wrong value for some Key. Expected %s, got %s", entry.v.GetValue(), val.GetValue()))
		}
	}
}

func TestIteratorSeek(t *testing.T) {
	sl := NewSkipList(1 << 20)
	entries := make([]entry, 100)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		v := []byte(fmt.Sprintf("val-%d", i))
		entries[i] = entry{k: db.NewKey(k), v: db.NewValue(v)}
		sl.Insert(entries[i].k, entries[i].v)
	}

	it := Newiterator(sl)
	defer it.Close()

	i := 50
	seekKey := []byte(fmt.Sprintf("key-%03d", i))
	it.Seek(db.NewKey(seekKey))
	for it.Seek(db.NewKey(seekKey)); it.Valid(); it.Next() {
		val := it.Get()
		if !bytes.Equal(val, entries[i].v.GetValue()) {
			t.Error(fmt.Printf("Wrong value for some Key. Expected %s, got %s", entries[i].v.GetValue(), val))
		}
		i++
	}

	seekNonExistent := []byte("key-101")
	it.Seek(db.NewKey(seekNonExistent))
	if it.Valid() {
		t.Error("Iterator should be invalid when seeking a key greater than all existing keys")
	}
}

func TestIteratorSnapshot(t *testing.T) {
	sl := NewSkipList(1 << 20)

	key1 := []byte("key1")
	val1 := db.NewValue([]byte("value1"))
	sl.Insert(db.NewKey(key1), val1)
	key2 := []byte("key2")
	val2 := db.NewValue([]byte("value2"))
	sl.Insert(db.NewKey(key2), val2)

	it := Newiterator(sl)
	defer it.Close()

	key1UpdateVal := db.NewValue([]byte("new_value1"))
	sl.Insert(db.NewKey(key1), key1UpdateVal)

	key3 := []byte("key3")
	val3 := db.NewValue([]byte("value3"))
	sl.Insert(db.NewKey(key3), val3)

	it.SeekToStart()

	// First key should be key1 with old value
	if !bytes.Equal(it.Get(), val1.GetValue()) {
		t.Errorf("Expected value1, got %s", it.Get())
	}
	it.Next()
	it.Next()

	// Iterator should be invalid now, key3 not in the snapshot
	if it.Valid() {
		t.Error("Iterator should be invalid after iterating through all items in snapshot")
	}

	// New iterator , New snapshot
	it2 := Newiterator(sl)
	defer it2.Close()

	it2.SeekToStart()

	if !bytes.Equal(it2.Get(), key1UpdateVal.GetValue()) {
		t.Errorf("New iterator does not see new value ,Expected value1, got %s", it2.Get())
	}

	it2.Seek(db.NewKey([]byte("key3")))
	// Iterator should see key3 in the snapshot
	if !bytes.Equal(it2.Get(), val3.GetValue()) {
		t.Errorf("New iterator does not see new value ,Expected value3, got %s", it2.Get())
	}

}

// Insert-only benchmarks for different value sizes
func BenchmarkInsert(b *testing.B) {
	lengths := []int{16, 256, 512}
	rng := rand.New(rand.NewSource(1))

	for _, l := range lengths {
		b.Run(fmt.Sprintf("Value-%dB", l), func(b *testing.B) {
			tests := makeEntries(10000, l, rng)
			sl := NewSkipList(3 << 30)

			var idx uint64
			b.SetParallelism(4)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx++
					entry := tests[idx%uint64(len(tests))]
					// println(entry.k)
					sl.Insert(entry.k, entry.v)
				}
			})
		})
	}
}

// Get-Only Benchmarks for different value sizes
func BenchmarkGet(b *testing.B) {
	lengths := []int{16, 256, 512}
	rng := rand.New(rand.NewSource(1))
	for _, l := range lengths {
		b.Run(fmt.Sprintf("Value-%dB", l), func(b *testing.B) {
			tests := makeEntries(1000, l, rng)
			sl := NewSkipList(3 << 30)

			// Insert Some Values
			for _, entry := range tests {
				sl.Insert(entry.k, entry.v)
			}

			var i uint64
			b.SetParallelism(4)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := atomic.AddUint64(&i, 1)
					entry := tests[idx%uint64(len(tests))]
					sl.Get(entry.k)
				}
			})
		})
	}

}

func BenchmarkInsertSequentially(b *testing.B) {
	lengths := []int{16, 256, 512}
	rng := rand.New(rand.NewSource(1))

	for _, l := range lengths {
		b.Run(fmt.Sprintf("Value-%dB", l), func(b *testing.B) {
			tests := makeEntriesSeq(1000000, l, rng)
			sl := NewSkipList(3 << 30)

			var idx uint64
			b.SetParallelism(4)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				h := &Hint{}
				for pb.Next() {
					atomic.AddUint64(&idx, 1)
					entry := tests[idx%uint64(len(tests))]
					// println(entry.k)
					sl.InsertWithHints(entry.k, entry.v, h)
				}
			})
		})
	}
}

// Mixed Benchmark with different Read/Write ratios
func BenchmarkMixed(b *testing.B) {
	value := db.NewValue([]byte("Test_Value"))
	rng := rand.New(rand.NewSource(1))
	tests := makeEntries(10000, 0, rng)
	b.Run("Write-Heavy", func(bx *testing.B) {
		sl := NewSkipList(3 << 30)
		ratio := float32(0.75)
		var i uint64
		bx.SetParallelism(4)
		bx.ResetTimer()
		bx.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			for pb.Next() {
				atomic.AddUint64(&i, 1)
				if rng.Float32() < ratio {
					sl.Insert(tests[i%uint64(len(tests))].k, value)
				} else {
					_ = sl.Get(tests[i%uint64(len(tests))].k)
				}
			}
		})
	})
	b.Run("Read-Heavy", func(bx *testing.B) {
		sl := NewSkipList(3 << 30)
		ratio := float32(0.25)
		var i uint64
		bx.SetParallelism(4)
		bx.ResetTimer()
		bx.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			for pb.Next() {
				atomic.AddUint64(&i, 1)
				if rng.Float32() < ratio {
					sl.Insert(tests[i%uint64(len(tests))].k, value)
				} else {
					_ = sl.Get(tests[i%uint64(len(tests))].k)
				}
			}
		})
	})

	b.Run("Write-Read", func(bx *testing.B) {
		sl := NewSkipList(3 << 30)
		ratio := float32(0.5)
		var i uint64
		bx.SetParallelism(4)
		bx.ResetTimer()
		bx.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			for pb.Next() {
				atomic.AddUint64(&i, 1)
				if rng.Float32() < ratio {
					sl.Insert(tests[i%uint64(len(tests))].k, value)
				} else {
					_ = sl.Get(tests[i%uint64(len(tests))].k)
				}
			}
		})
	})

}
