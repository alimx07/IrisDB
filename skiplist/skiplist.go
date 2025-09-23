/*

This implmentation inspired from RocksDB inline skiplist

*/

// TODO:
// ADD Tests for arena (V.I)
// Add Splice(Hints) for better sequential inserts
// what about versioning ?
// --> for now i am using Timestamp as a seq number !
// should i overwrite values and keep newest one only ?

package skiplist

import (
	"irisdb/db"
	"math"
	"sync/atomic"
	_ "unsafe"
)

const (
	MaxHeight = 25
	// Fulls     = uint32(4096)
)

// P(H Increase) = 1/3
var LogP float64 = math.Log(1 - (1.0 / math.E))

type Node struct {
	// key size 16B --> 64KB (avg <256B)
	// val size 32B --> 4GB (avg 1-4KB)
	// key&val off 32B  --> (Arena MaxSize)
	keysize uint16
	keyoff  uint32
	valsize uint32
	valoff  uint32

	topLevel uint32

	next [MaxHeight]atomic.Uint32
}

// func node(h int) Node {
// 	t := make([]atomic.Uint32, h)
// 	return Node{
// 		next: t,
// 	}
// }

func newNode(arena *Arena, level uint32, k []byte, v db.Value) *Node {
	startLoc := arena.allocNode(level)
	node := (*Node)(arena.getNodePointer(startLoc))
	node.keyoff = arena.setNodeKey(k)
	node.keysize = uint16(len(k))
	node.valoff = arena.setNodeVal(v)
	node.valsize = v.GetSize()
	node.topLevel = level
	return node
}

type SkipList struct {
	arena  *Arena
	head   *Node
	height atomic.Int32
	// prob   [MaxHeight]float32
}

func NewSkipList(sz uint32) *SkipList {
	arena := NewArena(sz)
	node := newNode(arena, MaxHeight-1, nil, db.Value{})
	sl := &SkipList{
		head:  node,
		arena: arena,
	}
	return sl
}

func (sl *SkipList) Get(k []byte) db.Value {

	level := sl.height.Load()
	curr := sl.head
	for {
		nextNode := curr.getNextNode(int(level), sl.arena)
		nextKey := nextNode.getKey(sl.arena)
		// println(string(nextKey))
		if len(nextKey) == 0 {
			if level == 0 {
				currKey := curr.getKey(sl.arena)
				if len(currKey) != 0 && db.CompareRawKeys(currKey, k) == 0 {
					return db.NewValue(curr.getVal(sl.arena))
				} else {
					break
				}
			}
			level--
			continue
		}
		cmp := db.CompareRawKeys(nextKey, k)
		if cmp >= 0 {
			if level > 0 {
				level--
				continue
			}
			if cmp == 0 {
				// println("val next", string(nextNode.getVal(sl.arena)))
				return db.NewValue(nextNode.getVal(sl.arena))
			}
			return db.Value{}
		}
		curr = nextNode
	}
	return db.Value{}
}

func (sl *SkipList) Insert(k []byte, v db.Value) {

	toplevel := sl.randomLevel()
	node := newNode(sl.arena, uint32(toplevel), k, v)
	var prev, succ [MaxHeight]*Node
	// println(unsafe.Pointer(node))
	sl.checkHeight(int32(toplevel))
	sl.findALLBounds(k, &prev, &succ)
	// start link
	for level := 0; level <= toplevel; level++ {
		for {
			succOff := sl.arena.getNodeOffset(succ[level])
			node.next[level].Store(succOff)
			ok := prev[level].next[level].CompareAndSwap(succOff, sl.arena.getNodeOffset(node))
			if ok {
				break
			}
			// the switch of links failed in this case (new X):
			// prev          succ
			//     \       /
			//      X    curr
			// So prev do not point to succ anymore
			// Do search again just from prev to find new prev and succ for curr for this level
			// it is like a search finger
			prev[level], succ[level] = sl.findBoundsForLevel(k, uint32(level), prev[level])
		}
	}
}

// func (sl *SkipList) findFromFinger(k db.Key , level , )

// search for prev and succ of key for specific level
func (sl *SkipList) findBoundsForLevel(k []byte, level uint32, prev *Node) (*Node, *Node) {
	for {
		succOff := prev.next[level].Load()

		// we reach end of the level
		if succOff == 0 {
			return prev, nil
		}
		succ := (*Node)(sl.arena.getNodePointer(succOff))
		loc, sz := succ.keyoff, succ.keysize

		currKey := sl.arena.getItem(loc, uint32(sz))
		cmp := db.CompareKeys(k, currKey)

		// next node is higher/equal curr
		// we found our bounds
		if cmp <= 0 {
			return prev, succ
		}

		prev = succ
	}
}

// search for prev and succ of key for all levels
func (sl *SkipList) findALLBounds(k []byte, prev, succ *[MaxHeight]*Node) {
	h := uint32(sl.height.Load())

	// top most level
	prev[h], succ[h] = sl.findBoundsForLevel(k, h, sl.head)
	// println("ff", h)
	for level := int(h) - 1; level >= 0; level-- {
		// use last level prev to start search
		// O(log n) property of skiplist
		// println(level)
		prev[level], succ[level] = sl.findBoundsForLevel(k, uint32(level), prev[level+1])
		// println(prev[level] == nil)
	}
}

func (sl *SkipList) randomLevel() int {

	u := float64(fastRandomNum()+1) / (1 << 32)
	h := 1 + int(math.Floor(math.Log(u)/LogP))
	h = min(h, MaxHeight-1)
	return h
}

func (sl *SkipList) checkHeight(level int32) {
	currHeight := sl.height.Load()
	for level > currHeight {
		ok := sl.height.CompareAndSwap(currHeight, level)
		if ok {
			break
		}
		currHeight = sl.height.Load()
	}
}

func (node *Node) getNextNode(level int, arena *Arena) *Node {
	off := node.next[level].Load()
	nodePtr := (*Node)(arena.getNodePointer(off))
	return nodePtr
}

func (node *Node) getKey(arena *Arena) []byte {
	loc := node.keyoff
	sz := node.keysize
	return arena.getItem(loc, uint32(sz))
}

func (node *Node) getVal(arena *Arena) []byte {
	loc := node.valoff
	sz := node.valsize
	return arena.getItem(loc, sz)
}

// func newProbTable(arena *Arena) {
// 	loc := arena.allocTable()
// 	for i := range MaxHeight {
// 		p := math.Pow(P , float64(i))

// 	}
// }

//go:linkname fastRandomNum runtime.fastrand
func fastRandomNum() uint32
