# B-sapling

B-sapling is a small raw-based in-disk slotted B+Tree implementation

- [x] Low level Design of the BTree, Page and Node structs
- [ ] Implement the page functionalities, add tests and docs
- [ ] Implement the node functionalities, add tests and docs
- [ ] Implement the btree functionalities, add tests and docs

## TODO, Need planning

- [ ] Add WAL file
- [ ] Handle cache eviction process on the root field from btree struct
- [ ] Add concurrent handling, how to deal with different thread read from or write to the files
