## Memtable
- Most DBs use skiplists as underlying DS for memtable. Skiplist-based memtable provide good overall performance for both read/write operations regardless of whether sequential or random access patterns are used. [Ref](https://www.cloudcentric.dev/exploring-memtables/)
- Memtable -conversion to `.sst`-> SSTable
- Deletion requires marking keys using `tombstones` because all memtables except the current one are read-only. So, we can't delete the key(s) from them.
  - For this, we use a byte called `OpKey` and append the value of our kv-pair to it.
      - encoded value = `OpKey` + value
      - `OpKey` = 0 (delete) and 1 (insert)

## Skiplist
- Skiplist is an ordered map (i.e it has ordered keys): [Ref](ttps://pkg.go.dev/github.com/huandu/skiplist#section-readme)
- It is a multi-leveled sorted linked list, where each level acts as an express lane to skip over a certain number of elements from the preceding levels. It is a lighter alternative to balanced BSTs.
- Key features:
  - On each level, nodes are sorted by keys.
  - All nodes in a skip list occupy its lowest level (level 0)
  - Each node has a "tower" with forward pointer(s) to the next node(s) on each corresponding level.
  - Probabilistic balancing of keys (using random no. generator)
  - Search/Insert/Delete operations 
    - Avg time: O(log n)
    - Worst case: O(n) when all elements have level = 1
- Why do we need it?
    - Although worst case time for balanced trees (AVL, self-balancing, etc) for Search/Insert/Delete is O(log n), they give poor perf. when input data is sorted due to constant rebalancing.
    - Skip lists have balance properties similar to that of search trees built by random insertions, yet do not require insertions to be random.
    - `MaxHeight` = `L(n)` = `log<sub>1/p</sub>n`
      - n = predicted max no. of elements that we expect to store.
      - p = fraction of nodes with level `i` pointers that also have level `i+1` pointers
- Search/Insert/Delete: [Ref](https://www.cloudcentric.dev/implementing-a-skip-list-in-go/)
  - During search, use `journey` array to keep track of immediate predecessor on each level. This helps with inserts/deletes. 
  - We have to randomly generate a height for every new node before inserting it into the list
  - level = [0, MaxHeight-1]; height = [1, MaxHeight]
  - Interesting how author has generated a probability distribution for the height of a node.