package db

import (
	"bytes"
	"encoding/binary"
	"time"
)

const (
	// len TimeStamp in bytes
	lenTs = 8
)

func NewKey(k []byte) []byte {
	t := time.Now().UnixNano()
	result := make([]byte, len(k)+lenTs)
	copy(result, k)
	binary.BigEndian.PutUint64(result[len(k):], uint64(t))
	return result
}

func NewKeyAt(k []byte, ts uint64) []byte {
	result := make([]byte, len(k)+lenTs)
	copy(result, k)
	binary.BigEndian.PutUint64(result[len(k):], ts)
	return result
}

func RawKey(internal []byte) []byte {
	if len(internal) < lenTs {
		return internal
	}
	return internal[:len(internal)-lenTs]
}

func CompareRawKeys(k1, k2 []byte) int {
	if len(k1) < lenTs {
		return -1
	}
	raw1 := k1[:len(k1)-lenTs]
	// k2 may be an internal key (len >= lenTs) or a pure raw key (len < lenTs).
	var raw2 []byte
	if len(k2) >= lenTs {
		raw2 = k2[:len(k2)-lenTs]
	} else {
		raw2 = k2
	}
	return bytes.Compare(raw1, raw2)
}

func CompareKeysTs(k1, k2 []byte) int {
	// negative to sort by Ts desc
	// which means newer entries come first
	return -(bytes.Compare(k1[len(k1)-lenTs:], k2[len(k2)-lenTs:]))
}

func CompareKeys(k1, k2 []byte) int {
	cmp := CompareRawKeys(k1, k2)
	if cmp == 0 {
		return CompareKeysTs(k1, k2)
	}
	return cmp
}

func GetTsAsUint64(k []byte) uint64 {
	ts := binary.BigEndian.Uint64(k[len(k)-lenTs:])
	return ts
}
