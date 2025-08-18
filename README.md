# B-sapling

B-sapling is a small feature-free lack-of-concurrency raw-based in-disk slotted B+Tree implementation

- [x] Low level Design of the BTree, Page and Node structs
- [x] Implement the page functionalities, add tests and docs
- [ ] Implement the node functionalities, add tests and docs
- [ ] Implement the btree functionalities, add tests and docs

## TODOs, Need planning

- [ ] Add logging, mentoring, observation
- [ ] Vacuum and Maintenance process to reclaim the wasted spaces in the pages because of delete operation (defragmentation)
- [ ] Add WAL file
- [ ] Handle cache eviction process on the root field from btree struct
- [ ] Add concurrent handling, how to deal with different thread read from or write to the files
