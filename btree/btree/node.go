package btree

import "bytes"

const (
	degree      = 5               // min child pointers a non-leaf node can have
	maxChildren = 2 * degree      // 10
	maxItems    = maxChildren - 1 // 9
	minItems    = degree - 1      // 4
)

type node struct {
	// use fixed-size arrays over slices to avoid costly slice expansion operations during insertion.
	// Also, fixed size makes it easier to store Btree on disk.
	items       [maxItems]*item
	children    [maxChildren]*node
	numItems    int
	numChildren int
}

func (n *node) isLeaf() bool {
	return n.numChildren == 0
}

/*
If data item with key k is found in node n, return its index i.
Else, return the index j where the key would have resided if it was present in the node.
Basically, lower bound of the key in the node -- this coincides with position of the child pointer !!
So, we can continue the traversal down the tree if the returned boolean value is false.
*/
func (n *node) search(key []byte) (int, bool) {
	low, high := 0, n.numItems
	var mid int
	for low < high {
		mid = (low + high) / 2
		cmp := bytes.Compare(key, n.items[mid].key)
		switch {
		case cmp > 0:
			low = mid + 1
		case cmp < 0:
			high = mid
		case cmp == 0:
			return mid, true
		}
	}
	return low, false
}

// helper method to insert data item at an arbitrary position of a B-tree node
func (n *node) insertItemAt(pos int, item *item) {
	if pos < n.numItems {
		copy(n.items[pos+1:n.numItems+1], n.items[pos:n.numItems])
	}
	n.items[pos] = item
	n.numItems++
}

// helper method to insert child pointer at an arbitrary position of a B-tree node
func (n *node) insertChildAt(pos int, child *node) {
	if pos < n.numChildren {
		copy(n.children[pos+1:n.numChildren+1], n.children[pos:n.numChildren])
	}
	n.children[pos] = child
	n.numChildren++
}

/*
we split as soon as we reach the parent of a child that is already full.
split() returns the middle item and newly created node, so we can link them to the parent.
Note: This doesn't include splitting the root node. For that check splitRoot() in tree.go
*/
func (n *node) split() (*item, *node) {
	mid := minItems
	midItem := n.items[mid]

	// Create a new node and copy half of the items from the current node to the new node.
	newNode := &node{}
	copy(newNode.items[:], n.items[mid+1:])
	newNode.numItems = minItems

	// Except for leaf nodes, copy half of the child pointers from the current node to the new node.
	if !n.isLeaf() {
		copy(newNode.children[:], n.children[mid+1:])
		newNode.numChildren = minItems + 1
	}

	// Remove data items and child pointers from the current node that were moved to the new node.
	num := n.numItems
	for i := mid; i < num; i++ {
		n.items[i] = nil
		n.numItems--

		if !n.isLeaf() {
			n.children[i+1] = nil
			n.numChildren--
		}
	}

	return midItem, newNode
}

/* 
Returned value is true if we performed insertion. If key already exists, we just update its value and return false.
The algo will start traversing the tree from its root, recursively calling the insert() method until it reaches a 
leaf node suitable for insertion. 
*/
func (n *node) insert(item *item) bool {
	pos, found := n.search(item.key)

	// The data item already exists, so just update its value.
	if found {
		n.items[pos] = item
		return false
	}

	// If we reach a leaf node -> it has sufficient space for the new item so, insert the new item
	if n.isLeaf() {
		n.insertItemAt(pos, item)
		return true
	}

	// If the next node on the traversal path is already full, split it
	if n.children[pos].numItems >= maxItems {
		midItem, newNode := n.children[pos].split()
		n.insertItemAt(pos, midItem)
		n.insertChildAt(pos+1, newNode)

		// We may need to change our direction after promoting the middle item to the parent, depending on its key.
		switch cmp := bytes.Compare(item.key, n.items[pos].key); {
		case cmp < 0:
			// The key we are looking for is still smaller than the key of the middle item that we took from the child,
			// so we can continue following the same direction.
		case cmp > 0:
			// The middle item that we took from the child has a key that is smaller than the one we are looking for,
			// so we need to change our direction.
			pos++
		default:
			// The middle item that we took from the child is the item we are searching for, so just update its value.
			n.items[pos] = item
			return true
		}
	}

	// Continue with the insertion process
	return n.children[pos].insert(item)
}
