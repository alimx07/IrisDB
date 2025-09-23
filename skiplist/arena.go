package skiplist

import (
	"irisdb/db"
	"sync/atomic"
	"unsafe"
)

// Arena is bump allocator
type Arena struct {
	// the actual buffer
	buf []byte
	// loc represent curr position inside buf
	// uint32 can support up to 2^32 = 4GB
	loc atomic.Uint32
}

const (
	nodeLevelSize = uint32(unsafe.Sizeof(atomic.Uint32{}))
	MaxSize       = uint32(unsafe.Sizeof(Node{}))
	align         = uint32(unsafe.Alignof(atomic.Uint32{})) - 1
)

func NewArena(n uint32) *Arena {

	arena := &Arena{
		buf: make([]byte, n),
	}
	return arena
}

// alloc space for node and return the starts offset as nodeLoc , KeyLoc , ValueLoc
func (arena *Arena) allocNode(h uint32, k []byte, v db.Value) (uint32, uint32, uint32) {

	// size of Node , key , value
	// -1 as our h starts from 0
	sz := MaxSize - ((MaxHeight - h - 1) * nodeLevelSize)
	ks := uint32(len(k))
	vs := v.GetSize()

	// ensure that arena is 4 byte alignment
	// as we will load and store in next atomically
	// size of all our data
	deltaLoc := sz + ks + vs + align
	newLoc := arena.loc.Add(deltaLoc)

	// start locations
	startLoc := (newLoc - (sz + ks + vs)) & ^align
	keyLoc := startLoc + sz
	valLoc := keyLoc + ks

	// copy key & value into buf
	copy(arena.buf[keyLoc:keyLoc+ks], k)
	copy(arena.buf[valLoc:valLoc+vs], v.GetValue())

	return startLoc, keyLoc, valLoc
}

func (arena *Arena) getNodePointer(nodeLoc uint32) unsafe.Pointer {
	return unsafe.Pointer(&arena.buf[nodeLoc])
}

func (arena *Arena) getItem(loc, sz uint32) []byte {
	return arena.buf[loc : loc+sz]
}

func (arena *Arena) getNodeOffset(node *Node) uint32 {
	if node == nil {
		return 0 // nil offset
	}
	nodeOff := uintptr(unsafe.Pointer(node)) - uintptr(arena.getNodePointer(0))
	// println(uint32(nodeOff))
	return uint32(nodeOff)
}
