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

// key operations
// New functions will be added

func NewKey(k []byte) []byte {
	t := time.Now().UnixMicro()
	return binary.BigEndian.AppendUint64(k, uint64(t))

}

func CompareRawKeys(k1, k2 []byte) int {

	// handle case of nil pointer of head
	if len(k1) < lenTs {
		return -1
	}
	return bytes.Compare(k1[:len(k1)-lenTs], k2[:len(k2)-lenTs])
}

func CompareKeysTs(k1, k2 []byte) int {
	// negative to sort by Ts desc
	// which means getNewKeys entires frist will search
	return -bytes.Compare(k1[len(k1)-lenTs:], k2[len(k2)-lenTs:])
}

func CompareKeys(k1, k2 []byte) int {
	cmp := CompareRawKeys(k1, k2)
	if cmp == 0 {
		return CompareKeysTs(k1, k2)
	}
	return cmp
}
