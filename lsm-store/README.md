## SSTable
- Do buffered I/O of 4KB to match OS page size and block size on disks.
- `storageManager` manages primary data folder of storage engine that contains all `.sst` files produced.
- `writer.go` converts a memtable to a `.sst` file.
- `.sst` Format
  - M-1: data block: keyLen (2B)|valLen (2B)|key (keyLen bytes)|opKind (1B)|val (valLen bytes)
    - We made assumption that all kv-pairs with their metadata will fit into 1KiB size buffer.
    - 1KiB buffer allows size of key & val > 255 bytes -> we used uint16 (2B) to store keyLen & valLen.
    - 2 issues -- 1. user can enter larger blobs and 2. majority kv-pairs are short in size, so their lengths can fit in type smaller than uint16.
    - Solution: Varint Encoding [Ref](https://www.cloudcentric.dev/exploring-sstables/)
      - e.g 64-bit integers require 8 bytes. With varint encoding, we can encode 64-bit integers in 1-10 bytes. So, it saves space for smaller integers but might be less efficient for large integers (as 10 bytes in worst case).
      - Previously, we used 2B to store keyLen & valLen -> total 4B. For smaller integers, these can fit in 1B each -> total 2B or 50% savings.
      - To work with varints, swap out fixed 1KB buffer with bytes.Buffer that can be dynamically resized.

## Memtable
- Most DBs use skiplists as underlying DS for memtable. Skiplist-based memtable provide good overall performance for both read/write operations regardless of whether sequential or random access patterns are used. [Ref](https://www.cloudcentric.dev/exploring-memtables/)
- Read-only memtables -conversion to `.sst`-> SSTables. We don't touch the mutable memtable.
  - Trigger condition: When a new record is added, check if size of all memtables (mutable + non-mutable) exceeds the configured threshold.
  - `.sst` files are sorted by keys in ascending order. So, we need to scan the first level of skiplist to get this.
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