package skiplist

import (
	"bytes"
	"lsm/fastrand"
	"math"
)

const (
	MaxHeight = 16
	p         = 0.5
)

var probabilities [MaxHeight]uint32

type node struct {
	key   []byte
	val   []byte
	tower [MaxHeight]*node
}

type SkipList struct {
	head   *node // starting head node
	height int   // current height
}

func init() {
	probability := 1.0

	for level := 0; level < MaxHeight; level++ {
		probabilities[level] = uint32(probability * float64(math.MaxUint32))
		probability *= p
	}
}

func randomHeight() int {
	//runtime.fastrand is faster than math/rand
	seed := fastrand.Uint32()

	height := 1
	for height < MaxHeight && seed <= probabilities[height] {
		height++
	}

	return height
}

func (sl *SkipList) String() string {
	v := &visualizer{sl}
	return v.visualize()
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:   &node{},
		height: 1,
	}
}

func (sl *SkipList) search(key []byte) (*node, [MaxHeight]*node) {
	var next *node
	var journey [MaxHeight]*node

	prev := sl.head
	// top to bottom level
	// ToDo: apply binary search
	for level := sl.height - 1; level >= 0; level-- {
		for next = prev.tower[level]; next != nil; next = prev.tower[level] {
			// key <= next.key
			if bytes.Compare(key, next.key) <= 0 {
				break
			}
			// key > next.key
			prev = next
		}
		journey[level] = prev
	}

	if next != nil && bytes.Equal(key, next.key) {
		return next, journey
	}
	return nil, journey
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	n, _ := sl.search(key)

	if n != nil {
		return n.val, true
	}
	return nil, false
}

func (sl *SkipList) Insert(key, val []byte) {
	n, journey := sl.search(key)

	//update value of existing key
	if n != nil {
		n.val = val
		return
	}

	height := randomHeight()
	new_node := &node{
		key: key,
		val: val,
	}

	//bottom to top level
	for level := 0; level < height; level++ {
		prev := journey[level]
		if prev == nil {
			// prev is nil if we extend the height of the tree
			// journey array won't have an entry for it.
			prev = sl.head
		}
		new_node.tower[level] = prev.tower[level]
		prev.tower[level] = new_node
	}

	// update current height of skiplist
	if height > sl.height {
		sl.height = height
	}
}

func (sl *SkipList) shrink() {
	for level := sl.height - 1; level >= 0; level-- {
		if sl.head.tower[level] == nil {
			sl.height--
		} else {
			break
		}
	}
}

func (sl *SkipList) Delete(key []byte) bool {
	n, journey := sl.search(key)

	// no such key exists
	if n == nil {
		return false
	}

	//bottom to top level
	for level := 0; level < sl.height; level++ {
		prev := journey[level]

		if prev.tower[level] != n {
			break
		}

		prev.tower[level] = n.tower[level]
		n.tower[level] = nil
	}

	n = nil
	// shrink height if  the removed node was the only node residing on
	// that particular level of the skip list.
	sl.shrink()
	return true
}
