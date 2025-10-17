package irisdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
)

type Block struct {
	keys [][]byte
}

type IndexBlock struct {
	entries []IndexEntry
}

type IndexEntry struct {
	off uint32
	key byte
}

func (i *IndexBlock) find(x byte) (uint32, bool) {

	// every index will contains at max 256 items
	// so linear seach here is acceptable

	for _, entry := range i.entries {
		if entry.key == x {
			return entry.off, true
		}
	}
	return 0, false
}

func (b *Block) find(key []byte) (uint16, bool) {
	// here i will apply binary search on block

	l, h := 0, len(b.keys)-1
	mid := (l + h) / 2
	for l <= h {
		cmp := bytes.Compare(key, b.keys[mid][2:])
		if cmp == 0 {
			return binary.BigEndian.Uint16(b.keys[mid][:2]), true
		}
		if cmp < 0 {
			h = mid - 1
		}
		l = mid + 1
	}
	return 0, false
}

func DeserializeBlock(buf bytes.Buffer) *Block {
	dec := gob.NewDecoder(&buf)

	var b *Block
	dec.Decode(b)
	return b
}

func (b *Block) SerializeBlock(buf bytes.Buffer) {
	enc := gob.NewEncoder(&buf)
	enc.Encode(b)
}

func DeserializeIndex(buf bytes.Buffer) *IndexBlock {
	dec := gob.NewDecoder(&buf)

	var i *IndexBlock
	dec.Decode(i)
	return i
}

func (i *IndexBlock) SerializeIndex(buf bytes.Buffer) {
	enc := gob.NewEncoder(&buf)
	enc.Encode(i)
}
