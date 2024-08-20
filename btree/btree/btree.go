package btree

const (
	degree      = 5
	maxChildren = 2 * degree      // 10
	maxItems    = maxChildren - 1 // 9
	minItems    = degree - 1      // 4
)

// data item in a node. key uniquely identifies a data item and used for sorting them.
type item struct {
	key []byte
	val []byte
}

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

// Btree only keeps a pointer to root node of the tree
type Btree struct {
	root *node
}

func NewBTree() *Btree {
	return &Btree{}
}