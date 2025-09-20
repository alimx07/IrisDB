package db

import (
	"bytes"
	"encoding/binary"
	"time"
)

type Key struct {
	key []byte
}

const (

	// len TimeStamp in bytes
	lenTs = 8
)

// key operations

func NewKey(k []byte) Key {
	t := time.Now().UnixMicro()
	return Key{
		key: binary.BigEndian.AppendUint64(k, uint64(t)),
	}
}

func NewPrevKey(k []byte) Key {
	return Key{key: k}
}
func (k Key) GetSize() uint32 {
	return uint32(len(k.key))
}

func (k Key) GetKey() []byte {
	return k.key
}

func (k Key) GetRawKey() []byte {
	// println(len(k.key), len(k.GetKey()), len(k.GetKey())-lenTs)
	return k.key[:k.GetSize()-lenTs]
}
func (k Key) GetTsUint64() uint64 {
	return binary.BigEndian.Uint64(k.key[k.GetSize()-lenTs:])
}

func (k Key) GetTsBytes() []byte {
	return k.key[k.GetSize()-lenTs:]
}

func CompareRawKeys(k1, k2 Key) int {
	return bytes.Compare(k1.GetRawKey(), k2.GetRawKey())
}

func CompareKeysTs(k1, k2 Key) int {
	return -bytes.Compare(k1.GetTsBytes(), k2.GetTsBytes())
}

func CompareKeys(k1, k2 Key) int {
	cmp := CompareRawKeys(k1, k2)
	if cmp == 0 {
		return CompareKeysTs(k1, k2)
	}
	return cmp
}
