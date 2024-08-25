package btree

/*
data item in a node.
key uniquely identifies a data item and used for sorting them.
val contains actual data
*/
type item struct {
	key []byte
	val []byte
}
