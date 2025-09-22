package skiplist

import (
	"irisdb/db"
	"sync/atomic"
	"unsafe"
)

// Arena is bump allocator
type Arena struct {

	// loc represent curr position inside buf
	// unint32 can support up to 2^32 = 4GB
	loc atomic.Uint32

	// the actual buffer
	buf []byte
}

const (
	nodeLevelSize = uint32(unsafe.Sizeof(atomic.Uint32{}))
	MaxSize       = uint32(unsafe.Sizeof(Node{}))
	// align         = uint32(unsafe.Alignof(atomic.Uint32{})) - 1
)

func NewArena(n uint32) *Arena {

	arena := &Arena{
		buf: make([]byte, n),
	}
	return arena
}

// alloc space for node and return the start offset
func (arena *Arena) allocNode(h uint32) uint32 {

	// size of Node
	// -1 as our h starts from 0
	sz := MaxSize - ((MaxHeight - h - 1) * nodeLevelSize)

	newLoc := arena.loc.Add(sz)
	startLoc := newLoc - sz
	return startLoc
}

// return the start offset of key loc
func (arena *Arena) setNodeKey(k db.Key) uint32 {
	sz := uint32(k.GetSize())
	newLoc := arena.loc.Add(sz)
	startLoc := newLoc - sz
	copy(arena.buf[startLoc:startLoc+sz], k.GetKey())
	return startLoc
}

// return the start offset of val loc
func (arena *Arena) setNodeVal(v db.Value) uint32 {
	sz := v.GetSize()
	newLoc := arena.loc.Add(sz)
	startLoc := newLoc - sz
	copy(arena.buf[startLoc:startLoc+sz], v.GetValue())
	return startLoc
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
