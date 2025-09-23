package skiplist

// TODO:
// ADD More Edge cases tests

import (
	"bytes"
	"fmt"
	"irisdb/db"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
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
	entries := make([]entry, 10)
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
	if !bytes.Equal(v.GetValue(), entries[9].v.GetValue()) {
		t.Error(fmt.Printf("Wrong Value. Expected %s, got %s", entries[9].v.GetValue(), v.GetValue()))
	}
}

// Insert-only benchmarks for different value sizes
func BenchmarkInsert(b *testing.B) {
	lengths := []int{16, 128, 512}
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
	lengths := []int{16, 128, 512}
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
