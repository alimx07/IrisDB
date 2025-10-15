// TODO:
// ADD Tests for arena (V.I)

package skiplist

import (
	"bytes"
	"errors"
	"math"
	"sync/atomic"
	"time"
	_ "unsafe"

	"github.com/alimx07/IrisDB/db"
)

const (
	MaxHeight = 25
)

// P(H Increase) = 1/e
// this is the best P of Two worlds (speed & Size) as found by pugh experiments
// https://dl.acm.org/doi/pdf/10.1145/78973.78977

var LogP float64 = math.Log(1 - (1.0 / math.E))

var (
	ErrNilHint         = errors.New("nil hint. Make sure to intiallize the hint befor passing")
	ErrSizeFull        = errors.New("no enough size for the insertion")
	ErrStillReferenced = errors.New("skiplist still referenced by one or more goroutines")
)

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

func newNode(arena *Arena, level uint32, k []byte, v db.Value) (*Node, error) {
	startLoc, keyLoc, valLoc, err := arena.allocNode(level, k, v)
	if err != nil {
		return nil, err
	}
	node := (*Node)(arena.getNodePointer(startLoc))
	node.keyoff = keyLoc
	node.keysize = uint16(len(k))
	node.valoff = valLoc
	node.valsize = v.GetSize()
	node.topLevel = level
	return node, nil
}

type SkipList struct {
	arena  *Arena       // Arena to store our Nodes
	head   *Node        // head of the skiplist
	height atomic.Int32 // curr Height of skiplist
	ref    atomic.Int32 // Number of Items operate on Skiplist now
}

func NewSkipList(sz uint32) *SkipList {
	arena := NewArena(sz)
	node, _ := newNode(arena, MaxHeight-1, nil, db.Value{})
	sl := &SkipList{
		head:  node,
		arena: arena,
	}
	return sl
}

// return the value of K
func (sl *SkipList) Get(k []byte) db.Value {

	node, found := sl.seek(k, math.MaxUint64)
	if found {
		return db.NewValue(node.getVal(sl.arena))
	}
	return db.Value{}
}

func (sl *SkipList) insert(k []byte, v db.Value, topLevel int, prev, succ *[MaxHeight]*Node) error {

	node, err := newNode(sl.arena, uint32(topLevel), k, v)
	if err != nil {
		return err
	}

	for level := 0; level <= topLevel; level++ {
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
			// So prev does not point to succ anymore
			// Do search again just from prev to find new prev and succ for this level
			// Old prev is like a search finger/hint
			prev[level], succ[level] = sl.findBoundsForLevel(k, uint32(level), prev[level])
		}
	}

	return nil
}

// Insert New Key-Value Pair in the skiplist
func (sl *SkipList) Insert(k []byte, v db.Value) {
	toplevel := sl.randomLevel()

	h := int(sl.checkHeight(int32(toplevel)))

	var prev, succ [MaxHeight]*Node

	sl.findAllBounds(k, h, &prev, &succ)

	sl.insert(k, v, toplevel, &prev, &succ)
}

// Insert Key-Value pair in the skiplist using hint (near previous node)
// NOTE:
//   - In concurrent environments, the caller must ensure that the hint is unique per goroutine or
//     operation to avoid race conditions and ensure good use of hints for faster performance.
// Example: If two goroutines use the same hint and insert into different locations,
// the hint will go back and forth between the two insertion points, leading to uselessness of hint optimization.

func (sl *SkipList) InsertWithHints(k []byte, v db.Value, hint *Hint) error {

	/*
		    NOTE : This optimization is inspired from Facebook (ROCKSDB) inlineSkipList

			THis Function will insert as normal but starting from Hint Node
			which makes it powerful for sequential inserts that have some order
			making insertion time from O(log N) to O(log D) where D is distance between
			hint and the inserted node.

	*/
	if hint == nil {
		return ErrNilHint
	}

	toplevel := sl.randomLevel()
	currlevel := int(sl.checkHeight(int32(toplevel)))

	var recompute_level int

	if currlevel > int(hint.Level) {
		//fallback to normal insert
		sl.findAllBounds(k, currlevel, &hint.prev, &hint.succ)
	} else {
		// check the hints to check if there are miss levels
		for level := 0; level <= currlevel; level++ {
			if db.CompareKeys(hint.prev[level].getKey(sl.arena), k) > 0 {
				recompute_level++
				continue
			}
			if hint.succ[level] != nil && db.CompareKeys(k, hint.succ[level].getKey(sl.arena)) > 0 {
				recompute_level++
				continue
			}
			nextNode := hint.prev[level].getNextNode(level, sl.arena)
			if nextNode != hint.succ[level] {
				recompute_level++
				continue
			}
			break

		}
	}

	var start *Node

	// compute for miss levels
	for level := recompute_level - 1; level >= 0; level-- {
		if level == MaxHeight-1 {
			start = sl.head
		} else {
			start = hint.prev[level+1]
		}
		hint.prev[level], hint.succ[level] = sl.findBoundsForLevel(k, uint32(level), start)

	}

	sl.insert(k, v, toplevel, &hint.prev, &hint.succ)

	hint.Level = int32(currlevel)
	return nil
}

// search for prev and succ of key for specific level
func (sl *SkipList) findBoundsForLevel(k []byte, level uint32, prev *Node) (*Node, *Node) {
	if prev == nil {
		prev = sl.head
	}
	for {
		succ := prev.nextNode(level, sl.arena)

		// no succ
		if succ == nil {
			return prev, nil
		}

		loc, sz := succ.keyoff, succ.keysize

		succKey := sl.arena.getItem(loc, uint32(sz))
		cmp := db.CompareKeys(k, succKey)

		// next node is higher/equal curr
		// we found our bounds
		if cmp <= 0 {
			return prev, succ
		}

		prev = succ
	}
}

// search for prev and succ of key for all levels
func (sl *SkipList) findAllBounds(k []byte, h int, prev, succ *[MaxHeight]*Node) {
	prev[h], succ[h] = sl.findBoundsForLevel(k, uint32(h), sl.head)
	for level := int(h) - 1; level >= 0; level-- {

		// use last level prev to start search
		// O(log n) property of skiplist
		prev[level], succ[level] = sl.findBoundsForLevel(k, uint32(level), prev[level+1])
	}
}

// Find a node with key >= k
func (sl *SkipList) seek(k []byte, ts uint64) (*Node, bool) {
	level := sl.height.Load()
	curr := sl.head
	for {
		nxNode := curr.nextNode(uint32(level), sl.arena)
		if nxNode == nil {
			if level == 0 {
				return nil, false
			}
			level--
			continue
		}
		nxKey := nxNode.getKey(sl.arena)
		cmp := db.CompareRawKeys(nxKey, k)
		if cmp >= 0 {
			if level > 0 {
				level--
				continue
			}
			if cmp == 0 && ts >= db.GetTsAsUint64(nxKey) {
				return nxNode, true
			}

			// IF we reach level 0 and did not find the node
			// in this case we already on node.key > key
			// Loop until we found node.ts <= ts
			for {
				nxNode = nxNode.nextNode(0, sl.arena)
				if nxNode == nil {
					return nil, false
				}
				nxKey = nxNode.getKey(sl.arena)
				if ts >= db.GetTsAsUint64(nxKey) {
					return nxNode, false
				}

			}
		}
		curr = nxNode
	}
}
func (sl *SkipList) randomLevel() int {

	u := float64(fastRandomNum()+1) / (1 << 32)
	h := 1 + int(math.Floor(math.Log(u)/LogP))
	h = min(h, MaxHeight-1)
	return h
}

func (sl *SkipList) checkHeight(level int32) int32 {
	currHeight := sl.height.Load()
	for level > currHeight {
		ok := sl.height.CompareAndSwap(currHeight, level)
		if ok {
			return level
		}
		currHeight = sl.height.Load()
	}
	return currHeight
}

func (sl *SkipList) GetSize() uint32 {
	return sl.arena.getSize()
}

func (sl *SkipList) Close() error {
	if sl.ref.Load() > 0 {
		return ErrStillReferenced
	}

	// remove any references for arena to make GC Free it
	sl.arena = nil
	sl.head = nil

	return nil
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

func (node *Node) nextNode(level uint32, arena *Arena) *Node {
	succOff := node.next[level].Load()

	// we reach end of the level
	if succOff == 0 {
		return nil
	}
	succ := (*Node)(arena.getNodePointer(succOff))
	return succ
}

// func newProbTable(arena *Arena) {
// 	loc := arena.allocTable()
// 	for i := range MaxHeight {
// 		p := math.Pow(P , float64(i))

// 	}
// }

type Hint struct {
	prev  [MaxHeight]*Node
	succ  [MaxHeight]*Node
	Level int32
}

type Iterator struct {
	sl     *SkipList
	cursor *Node
	ts     uint64 // timestamp of iterator creation
}

// The Iterator take a snapshot on Memtable at ts
func Newiterator(sl *SkipList) *Iterator {
	sl.ref.Add(1)
	return &Iterator{
		ts: uint64(time.Now().UnixNano()),
		sl: sl,
	}
}

// the iterator go in one direction for now

// Seek node.key >= k at the time a request happens
func (it *Iterator) Seek(k []byte) {
	it.cursor, _ = it.sl.seek(k, it.ts)
}

// Get Value of current Key
func (it *Iterator) Get() []byte {
	return it.cursor.getVal(it.sl.arena)
}

// Get curr Key
func (it *Iterator) GetKey() []byte {
	return it.cursor.getKey(it.sl.arena)
}

// GO to the next node
func (it *Iterator) Next() {
	for {
		it.cursor = it.cursor.nextNode(0, it.sl.arena)
		if it.cursor == nil {
			return
		}
		if it.ts >= db.GetTsAsUint64(it.cursor.getKey(it.sl.arena)) {
			break
		}
	}
}

// Ensure we on valid node and not the end
func (it *Iterator) Valid() bool {
	return it.cursor != nil
}

func (it *Iterator) SeekToStart() {
	it.cursor = it.sl.head
	// go to the next node after the head
	it.Next()
}

func (it *Iterator) Close() {
	it.sl.ref.Add(-1)
	it = nil
}

// Merge Two Frozen memtables
type MergeIterator struct {
	first  *Iterator
	second *Iterator
}

func NewMergeIterator(mem1, mem2 *SkipList) *MergeIterator {
	it1 := Newiterator(mem1)
	it2 := Newiterator(mem2)
	it1.SeekToStart()
	it2.SeekToStart()
	return &MergeIterator{first: it1, second: it2}
}

func (mems MergeIterator) Next() []byte {
	if !mems.first.Valid() && !mems.second.Valid() {
		return nil
	}
	// one side exhausted
	if !mems.first.Valid() {
		v := mems.second.Get()
		mems.second.Next()
		return v
	}
	if !mems.second.Valid() {
		v := mems.first.Get()
		mems.first.Next()
		return v
	}

	if bytes.Compare(mems.first.GetKey(), mems.second.GetKey()) <= 0 {
		v := mems.first.Get()
		mems.first.Next()
		return v
	}
	v := mems.second.Get()
	mems.second.Next()
	return v
}

//go:linkname fastRandomNum runtime.fastrand
func fastRandomNum() uint32
