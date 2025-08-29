package sapling

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/KhaledMosaad/B-sapling/db"
	"github.com/KhaledMosaad/B-sapling/storage"
)

// This is the orchestrator object of the Database
type BTree struct {
	// root might not be empty for the first opening of the database
	root *storage.Node
	// Preparing for concurrency operations
	// TODO: should be stored in the file header because of the remove path, the TotalPageSize/pageSize won't work
	nodeCount atomic.Uint32
	open      bool
	wlock     sync.Mutex // simple write lock
	mng       *storage.Manager
}

var _ db.DB = &BTree{}

// Initialize the database, It will create the database file if not exists
func Open(path string) (*BTree, error) {
	// default value
	if path == "" {
		path = "./local/fast.db"
	}

	b := &BTree{}
	b.wlock.Lock()
	defer b.wlock.Unlock()

	pageSize := os.Getpagesize()
	mng, root, err := storage.NewManager(pageSize, path, &b.nodeCount)

	if err != nil {
		return nil, err
	}

	b.root = root
	b.mng = mng
	b.open = true

	return b, nil
}

// Set node will find the node that will be the parent and insert new node to it then write it as a page
// return success, split , error
func (b *BTree) Upsert(key []byte, value []byte) (bool, bool, error) {
	// TODO: key and value must be under min(65535 (max(uint16)), b.pageSize) bytes
	if !b.open {
		return false, false, errors.New("Database was closed")
	}

	b.wlock.Lock()
	defer b.wlock.Unlock()

	node, pos, found, err := b.findNode(key)

	if err != nil {
		return false, false, err
	}

	if found {
		// Do update and return
		// TODO: Check if the node have available space for the new value, if not split to update :)
		node.FreeLength += len(node.Pairs[pos].Value) - len(value)
		node.Pairs[pos].Value = value
		node.Dirty = true
		return true, false, nil
	}
	// node must be leaf, assert that
	// Should pairs be linked list to insert in o(1) instead of coping to a new array
	node.Pairs = slices.Insert(node.Pairs, pos, storage.Pair{Key: key, Value: value})
	node.FreeLength -= 4 + len(key) + len(value)
	node.Dirty = true

	if node.FreeLength < 0 {
		node.Split(b.root, &b.nodeCount, b.mng.PageSize)
		return true, true, nil
	}
	fmt.Println("Upsert/ Node:", node, pos, found)
	return true, false, nil
}

// Remove node will remove the page from storage
func (b *BTree) Remove(key []byte) error {
	// TODO: Create a new node, we will need to traverse the current nodes to see where we insert it, specifically the leaf nodes
	// TODO: key might be always fixed-sized value int64 will be great, but any for now
	return nil
}

// Find node from the database, the read path will be db.FindNode(key) => from the root node read pages until you find the needed page, return it as a node
// If found it will return the target node, pos, true, nil error the pos is the position that contains the target value in the node.pairs
// If error will return nil, -1, false, error So the caller need to check for the error first
// If not found it will return node (to be inserted in), pos, false, nil the pos is the position that key should be inserted in
// TODO: This function should return the path stack ds to help the caller with split and merge operation (Breadcrumbs), or just follow parent ref?
func (b *BTree) findNode(key []byte) (*storage.Node, int, bool, error) {
	if !b.open {
		return nil, -1, false, errors.New("Database was closed")
	}

	// if the node is root and has empty pairs, the db is empty and we should return the first position we can insert into
	if len(b.root.Pairs) == 0 {
		return b.root, 0, false, nil
	}

	targetPair := storage.Pair{Key: key, Value: make([]byte, 0)}
	node := b.root
	pos := -1
	found := false

	for {
		// read the node if it's not fetched from disk yet and convert it to node
		if len(node.Pairs) == 0 {
			var err error = nil
			node, err = b.mng.Read(node.ID)
			if err != nil {
				return nil, -1, false, err
			}
		}

		pos, found = slices.BinarySearchFunc(node.Pairs, targetPair, func(x, k storage.Pair) int {
			return bytes.Compare(x.Key, k.Key)
		})

		// FIXME: The value is not set if the found node is leaf but not the root
		if (node.Typ & storage.INTERNAL_NODE) == storage.INTERNAL_NODE {
			// this is safe because node.children length = pairs length + right most reference
			// and every internal node fetched from disk have a non-nil children filled with node.ID value
			node.Children[pos].Parent = node
			node = node.Children[pos]
		}

		// base case is to reach a leaf node return early because either we found the target or not found and have a position to insert it
		if (node.Typ & storage.LEAF_NODE) == storage.LEAF_NODE {
			break
		}
	}

	return node, pos, found, nil
}

func (b *BTree) Find(key []byte) ([]byte, error) {
	if !b.open {
		return nil, errors.New("Database was closed")
	}

	node, pos, found, err := b.findNode(key)
	fmt.Println("Find/ Node:", node, pos, found)

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, errors.New("Value not exist")
	}

	return node.Pairs[pos].Value, nil
}

func (b *BTree) Close() error {
	b.wlock.Lock()
	defer b.wlock.Unlock()

	if !b.open {
		return errors.New("Database already closed")
	}

	if err := b.vacuum(); err != nil {
		return err
	}

	if err := b.mng.Close(); err != nil {
		return err
	}

	b.open = false
	return nil
}

// Basic Vacuum process to write the dirty nodes into the file
func (b *BTree) vacuum() error {
	return b.mng.WriteNodeTree(b.root)
}
