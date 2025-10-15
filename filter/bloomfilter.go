package filter

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math"

	"github.com/cespare/xxhash/v2"
)

type Bloomfilter struct {
	bitSet []uint64
	hashes []uint64
	Size   uint32
}

func NewBloomFilter(n uint32, fp float64) (*Bloomfilter, error) {
	if n <= 0 {
		return nil, errors.New("expected number of items must be positive")
	}
	if fp <= 0 || fp >= 1 {
		return nil, errors.New("probability of false positive must be in range (0, 1)")
	}

	// optimal size
	sz := uint32(math.Ceil(-(float64(n) * math.Log(fp)) / math.Pow(math.Log(2), 2)))

	// optimal number of hash functions
	k := uint32(math.Ceil((float64(sz) / float64(n)) * math.Log(2)))

	sz = (sz + 63) / 64

	return &Bloomfilter{
		Size:   sz,
		bitSet: make([]uint64, sz),
		hashes: make([]uint64, k),
	}, nil
}

func (bf *Bloomfilter) getHashes(data []byte) {
	h1 := xxhash.Sum64(data)

	// Mock Way to generate another hash
	h2 := h1 >> 32

	// Double hashing technique: h_i(x) = h1(x) + i*h2(x)
	for i := 0; i < len(bf.hashes); i++ {
		hx := h1 + uint64(i)*h2
		bf.hashes[i] = hx % uint64(bf.Size)
	}
}

func (bf *Bloomfilter) Add(data []byte) {
	bf.getHashes(data)
	for _, pos := range bf.hashes {
		arrayPos := pos / 64
		bitPos := pos % 64
		bf.bitSet[arrayPos] |= 1 << bitPos
	}
}

func (bf *Bloomfilter) Contains(data []byte) bool {
	bf.getHashes(data)
	for _, pos := range bf.hashes {
		arrayPos := pos / 64
		bitPos := pos % 64
		if (bf.bitSet[arrayPos] & (1 << bitPos)) == 0 {
			return false
		}
	}
	return true // maybe
}

func BytesTOSTruct(buf bytes.Buffer) *Bloomfilter {
	dec := gob.NewDecoder(&buf)

	var bf *Bloomfilter
	dec.Decode(bf)
	return bf
}

func (bf *Bloomfilter) StructToBytes(buf bytes.Buffer) error {
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(&bf)
	return err
}
