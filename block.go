package irisdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
)

type Block struct {
	Keys [][]byte
}

type IndexBlock struct {
	Entries []IndexEntry
}

type IndexEntry struct {
	Off uint32
	Key byte
}

func (i *IndexBlock) find(x byte) (uint32, bool) {

	// every index will contains at max 256 items
	// so linear seach here is acceptable

	for _, entry := range i.Entries {
		if entry.Key == x {
			return entry.Off, true
		}
	}
	return 0, false
}

// func (b *Block) find(key []byte) (uint16, bool) {
// 	// here i will apply binary search on block

// 	l, h := 0, len(b.Keys)-1
// 	for l <= h {
// 		mid := (l + h) / 2
// 		cmp := bytes.Compare(key, b.Keys[mid][2:]) // remove valPgNum
// 		if cmp == 0 {
// 			return binary.BigEndian.Uint16(b.Keys[mid][:2]), true
// 		}
// 		if cmp < 0 {
// 			h = mid - 1
// 		} else {
// 			l = mid + 1
// 		}
// 	}
// 	return 0, false
// }

// findBefore returns valPgNum of newest ver. of rawKey (ts <= ts).
func (b *Block) findBefore(rawKey []byte, ts uint64) (uint16, bool) {
	pg, _, ok := b.findBeforeWithTs(rawKey, ts)
	return pg, ok
}

// findBeforeWithTs is like findBefore but also returns the matched timestamp.
func (b *Block) findBeforeWithTs(rawKey []byte, ts uint64) (uint16, uint64, bool) {
	for _, k := range b.Keys {
		stored := k[2:] // rawKey + ts(8)
		if len(stored) < 8 {
			continue
		}
		rawLen := len(stored) - 8
		kRaw := stored[:rawLen]
		kTs := binary.BigEndian.Uint64(stored[rawLen:])
		if bytes.Equal(kRaw, rawKey) && kTs <= ts {
			return binary.BigEndian.Uint16(k[:2]), kTs, true
		}
	}
	return 0, 0, false
}

// findRangeWithTs returns all valPgNums rawKey [ts1, ts2].
func (b *Block) findRangeWithTs(rawKey []byte, ts1, ts2 uint64) ([]uint16, []uint64) {
	var pgs []uint16
	var timestamps []uint64
	for _, k := range b.Keys {
		stored := k[2:]
		if len(stored) < 8 {
			continue
		}
		rawLen := len(stored) - 8
		kRaw := stored[:rawLen]
		kTs := binary.BigEndian.Uint64(stored[rawLen:])
		if bytes.Equal(kRaw, rawKey) && kTs >= ts1 && kTs <= ts2 {
			pgs = append(pgs, binary.BigEndian.Uint16(k[:2]))
			timestamps = append(timestamps, kTs)
		}
	}
	return pgs, timestamps
}

func DeserializeBlock(buf *bytes.Buffer) (*Block, error) {
	dec := gob.NewDecoder(buf)
	var b Block
	if err := dec.Decode(&b); err != nil {
		return nil, err
	}
	return &b, nil
}

func (b *Block) SerializeBlock() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeIndex(buf *bytes.Buffer) (*IndexBlock, error) {
	dec := gob.NewDecoder(buf)
	var i IndexBlock
	if err := dec.Decode(&i); err != nil {
		return nil, err
	}
	return &i, nil
}

func (i *IndexBlock) SerializeIndex() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(i); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
