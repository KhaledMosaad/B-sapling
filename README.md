# B-sapling

B-sapling is a small feature-free lack-of-concurrency raw-based in-disk slotted B+Tree implementation

## How to use

```go
	// storage folder is ignored from git so we can insert the actual data their
  db, err := sapling.Open("./storage/fast.db")

	if err != nil {
		panic(err)
	}
	err = db.Upsert([]byte("My Key 2"), []byte("My Value 3"))
	if err != nil {
		panic(err)
	}

	value, err := db.Find([]byte("My Key"))
	if err != nil {
		// The key is not exist, or the database is closed
	}

	stringVal := string(value)
	fmt.Println(stringVal)

	err = db.Close()
	if err != nil {
		panic(err)
	}
```

## Want to do

- [x] Low level Design of the BTree, Page and Node structsJus, t a dump design for now
- [x] Implement the basic functionality, upsert/remove/get/open/close (Still need to implement remove)
- [x] WIP: Implement split function on overflow pages/upsert
- [ ] Implement merge/rebalance on underflow pages/remove
- [x] Vacuum to flush the pages in the disk by close, WIP: (time, resources)
- [ ] WIP: Add Unit Tests
- [ ] TigerStyle assertion programming
- [ ] Refactor/ Add storage manager to manage pages and nodes
- [ ] Maintenance process to reclaim the wasted spaces in the pages because of delete operation (defragmentation)
- [ ] Add logging, mentoring, observation
- [ ] Add WAL file, maybe WAL2?
- [ ] Handle cache eviction process on the root field from btree struct (root page can't be evicted from cache)
- [ ] Add concurrent processing, how to deal with different threads read/write operations
