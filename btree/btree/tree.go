package btree

import "fmt"

/*
Btree only keeps a pointer to root node of the tree.
A tree is made up of nodes. Each node contains data items.
*/
type Btree struct {
	root *node
}

func NewBTree() *Btree {
	return &Btree{}
}

// Searching the entire tree.
func (t *Btree) Find(key []byte) ([]byte, error) {
	for next := t.root; next != nil; {
		pos, found := next.search(key)
		if found {
			return next.items[pos].val, nil
		}
		next = next.children[pos]
	}
	return nil, fmt.Errorf("key %s not found", key)
}

/*
Create a new root node.
The existing root then becomes the new root's left child.
The new node created after splitting the existing root becomes new root's right child.
*/
func (t *Btree) splitRoot() {
	newRoot := &node{}
	midItem, newNode := t.root.split()
	newRoot.insertItemAt(0, midItem)
	newRoot.insertChildAt(0, t.root)
	newRoot.insertChildAt(1, newNode)
	t.root = newRoot
}

func (t *Btree) Insert(key, val []byte) {
	i := &item{key, val}

	// The tree is empty, so initialize a new node.
	if t.root == nil {
		t.root = &node{}
	}

	// The tree root is full, so perform a split on the root.
	if t.root.numItems >= maxItems {
		t.splitRoot()
	}

	// Begin insertion.
	t.root.insert(i)
}

func (t *Btree) Delete(key []byte) bool {
	if t.root == nil {
		return false
	}
	deletedItem := t.root.delete(key, false)

	if t.root.numItems == 0 {
		if t.root.isLeaf() {
			t.root = nil
		} else {
			t.root = t.root.children[0]
		}
	}

	if deletedItem != nil {
		return true
	}
	return false
}
