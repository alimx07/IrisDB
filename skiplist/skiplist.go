package skiplist

import (
	"irisdb/db"
	"math/rand"
	"sync/atomic"
)

const (
	Mod = 2

	MaxHeight = 16
)

type Node struct {

	// offset(0:31) - size(32:63)
	// sameKey--> multiple values now
	//TODO: overwrite value (atomic)
	// Can overwrite key also ??
	key      uint64
	val      uint64
	topLevel uint32

	next [MaxHeight]atomic.Uint32
}

// func node(h int) Node {
// 	t := make([]atomic.Uint32, h)
// 	return Node{
// 		next: t,
// 	}
// }

func newNode(arena *Arena, level uint32, k db.Key, v db.Value) *Node {
	startLoc := arena.allocNode(level)
	node := (*Node)(arena.getNodePointer(startLoc))
	// c := EncodeItem(k.GetSize(), arena.setNodeKey(k))
	// println("TTT", c)
	node.key = EncodeItem(k.GetSize(), arena.setNodeKey(k))
	node.val = EncodeItem(v.GetSize(), arena.setNodeVal(v))
	node.topLevel = level
	// println(unsafe.Pointer(node))
	return node
}

type SkipList struct {
	arena  *Arena
	head   *Node
	height atomic.Int32
}

func NewSkipList(sz uint32) *SkipList {
	arena := NewArena(sz)
	node := newNode(arena, MaxHeight, db.Key{}, db.Value{})
	sl := &SkipList{
		head:  node,
		arena: arena,
	}
	return sl
}

func (sl *SkipList) Get(k db.Key) db.Value {
	// println(string(k.GetKey()))

	level := sl.height.Load()
	curr := sl.head
	for {
		nextNode := curr.getNextNode(int(level), sl.arena)
		nextKey := nextNode.getKey(sl.arena)
		// println(string(nextKey))
		if len(nextKey) == 0 {
			if level == 0 {
				currKey := curr.getKey(sl.arena)
				// println("curr", string(currKey))
				if len(currKey) != 0 && db.CompareRawKeys(db.NewPrevKey(currKey), k) == 0 {
					// println("val curr", string(curr.getVal(sl.arena)))
					return db.NewValue(curr.getVal(sl.arena))
				} else {
					break
				}
			}
			level--
			continue
		}
		cmp := db.CompareRawKeys(db.NewPrevKey(nextKey), k)
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

func (sl *SkipList) Insert(k db.Key, v db.Value) {

	toplevel := randomLevel()
	node := newNode(sl.arena, uint32(toplevel), k, v)
	// println(unsafe.Pointer(node))
	sl.checkHeight(int32(toplevel))

	var prev, succ [MaxHeight]*Node
	// start link
	for level := 0; level <= toplevel; level++ {
		for {
			if prev[level] == nil {
				prev[level], succ[level] = sl.find(k, uint32(level), sl.head)
			}
			succOff := sl.arena.getNodeOffset(succ[level])
			node.next[level].Store(succOff)
			ok := prev[level].next[level].CompareAndSwap(succOff, sl.arena.getNodeOffset(node))
			if ok {
				break
			}
			prev[level], succ[level] = sl.find(k, uint32(level), sl.head)
		}
	}
}

// search for prev and succ of key on specific level
// I will search by level as optimization:
// --> Node can be found in higher level
// --> avoid going down with no benefit
func (sl *SkipList) find(k db.Key, level uint32, startNode *Node) (*Node, *Node) {
	if startNode == nil {
		startNode = sl.head
	}
	prev := startNode
	for {
		// println("Try")
		// get offset of next in arena
		succOff := prev.next[level].Load()
		if succOff == 0 {
			return prev, nil
		}
		succ := (*Node)(sl.arena.getNodePointer(succOff))
		// println("OFF", succOff)
		// println("VAL", succ.val)
		// find key
		keyMetaData := succ.key

		// println("DATA", keyMetaData)
		loc, sz := DecondeItem(keyMetaData)
		currKey := sl.arena.getItem(loc, sz)
		// println(currKey)
		cmp := db.CompareKeys(k, db.NewPrevKey(currKey))
		// println(cmp)
		if cmp == 0 {
			return succ, succ
		}
		if cmp < 0 {
			// println("IN")
			return prev, succ
		}
		prev = succ
	}
}
func EncodeItem(size, loc uint32) uint64 {
	// println("ENCODE", loc, "  ", size)/
	return uint64(size)<<32 | uint64(loc)
}

func DecondeItem(encodedkey uint64) (uint32, uint32) {
	loc := uint32(encodedkey)
	sz := uint32(encodedkey >> 32)
	return loc, sz
}

func randomLevel() int {
	h := 0
	for {
		if h == MaxHeight-1 || rand.Int()%Mod == 0 {
			break
		}
		h++
	}
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
	key := node.key
	return arena.getItem(DecondeItem(key))
}

func (node *Node) getVal(arena *Arena) []byte {
	val := node.val
	return arena.getItem(DecondeItem(val))
}
