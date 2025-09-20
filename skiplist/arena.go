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
)

type offsetType = uint32

func NewArena(n uint32) *Arena {

	arena := &Arena{
		buf: make([]byte, n),
	}
	return arena
}

// alloc space for node and return the start offset
func (arena *Arena) allocNode(h uint32) offsetType {

	// size of Node
	sz := MaxSize - (nodeLevelSize * (MaxHeight - h))
	newLoc := arena.loc.Add(sz)
	startLoc := newLoc - sz
	return startLoc
}

// return the start offset of key loc
func (arena *Arena) setNodeKey(k db.Key) offsetType {
	sz := k.GetSize()
	newLoc := arena.loc.Add(sz)
	startLoc := newLoc - sz
	// println("Store", string(k.GetKey()))
	copy(arena.buf[startLoc:startLoc+sz], k.GetKey())
	return startLoc
}

// return the start offset of val loc
func (arena *Arena) setNodeVal(v db.Value) offsetType {
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

func (arena *Arena) getNodeOffset(node *Node) offsetType {
	if node == nil {
		return 0 // nil offset
	}
	nodeOff := uintptr(unsafe.Pointer(node)) - uintptr(arena.getNodePointer(0))
	// println(uint32(nodeOff))
	return uint32(nodeOff)
}
