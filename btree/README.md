Reference: https://www.cloudcentric.dev/implementing-a-b-tree-in-go/

### Btree
- self-balancing search tree
- fanout (max no. of children for a node) = f
- Insertion, Deletion, Search -> O(log<sub>f</sub>N)
- `Search tree`: nodes are in sorted order
- `Balanced tree`: heights of the left and right subtrees of any node differ by no more than 1 (i.e., the overall height of a tree with "N" nodes leans towards "log<sub>2</sub>N")
  - In unbalanced trees, Insertion, Deletion, Search can degrade to O(N)
  - Balance is maintained via `rotations` as nodes are added/removed.

### Why BSTs are bad as an on-disk data structure?
- Balanced BSTs are good as in-memory data structures but impractical for disks.
  - In case of disks, data is transferred to and from main memory in `blocks`. Even if we want to read/write a single byte of data, a `block` of data is read from disk to main memory, updated in-memory and flushed back to disk. So, a simple insertion/deletion can cause multiple rotations which can rearrange a large portion of the tree -> rewrite large portions of data on-disk -> $$$$$
  - Due to low fanout, tree is usually tall and assuming every node resides at a different location on disk due to constant rebalancing, traversing the tree might require log<sub>f</sub>N disk scans.

### How Btree helps?
- Btree solves both of these issues using `2 key ideas`:
  - Bringing child pointers and data items closer. So, data that was scattered across 100s of BST nodes can now be stored in a single Btree node. A single Btree node occupies a full `block` of data on disk. This decreases the `rotations`.
  - Increased fanout -> less height

### Variants of Btree
- `Btree`: Every node contains raw data, plus keys and child pointers for navigation.
- `B+tree`: Root node and internal nodes contain keys and child pointers. Only leaf nodes contain raw data. Leaf nodes are linked to each other via singly/doubly linked list for easier sequential scanning of all keys in ascending/descending order.
- `B*tree`: Similar to `B+tree` but ensure each node is atleast 2/3 full instead of 1/2 full. They also delay split operation by using a local distribution technique to move keys from an overflowing node to its neighboring nodes before resorting to a split.

### Anatomy of a Btree
- `degree`: Represents the min no. of children a non-leaf node can point to. 
  - For a B-Tree with `degree of d` -> `non-leaf node` can store `[d, 2d]` child pointers.
  - Root node is allowed to have minimum of 2 children irrespective of the `degree`. 
  - Leaf nodes have no children.
  - Min allowed degree is 2.
  - The higher the degree, the larger the nodes, and consequently, the more data we can fit into a single data block on disk. In production, 100s of data items are stored in a single B-Tree node in order to reduce disk I/O.
- A non-leaf node with `N children` is stated to always hold exactly `N - 1 data items`

    ```
    const (
        degree      = 5
        maxChildren = 2 * degree       // 10
        maxItems    = maxChildren - 1  // 9
        minItems    = degree - 1       // 4
    )
    ```
