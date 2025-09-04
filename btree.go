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
	"github.com/nikoksr/assert-go"
	"github.com/rs/zerolog/log"
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
	assert.Assert(len(key) > 0 && len(key) < 65530, fmt.Sprintf("The key length must be between 0 - 65530 key: %v value: %v", string(key), string(value)))
	assert.Assert(len(value) > 0 && len(value) < 65530, fmt.Sprintf("The value length must be between 0 - 65530 key: %v value: %v", string(key), string(value)))
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
		node.FreeLength += (len(node.Pairs[pos].Value) - len(value))
		node.Pairs[pos].Value = value
		node.Dirty = true
		if node.FreeLength < 0 {
			assert.Debug(true, "Doing split", node, pos, found)
			node, err = node.Split(b.root, &b.nodeCount, b.mng.PageSize)
			if err != nil {
				return false, true, err
			}
			return true, true, nil
		}
		return true, false, nil
	}
	// node must be leaf, assert that
	// Should pairs be linked list to insert in o(1) instead of coping to a new array
	pair := storage.Pair{Key: key, Value: value}
	node.Pairs = slices.Insert(node.Pairs, pos, pair)
	node.FreeLength = node.FreeLength - (storage.CELL_CONST_SIZE + len(key) + len(value))
	node.Dirty = true

	if node.FreeLength < 0 {
		node, err = node.Split(b.root, &b.nodeCount, b.mng.PageSize)
		if err != nil {
			return false, true, err
		}
		return true, true, nil
	}
	assert.Assert(node.FreeLength > 0, fmt.Sprintf("Node free bytes must be greater than zero, nodeId: %v, freeLength: %v", node.ID, node.FreeLength))

	// b.mng.WriteNodeTree(node)
	assert.Debug(true, "Upsert/ Node:", node, pos, found)
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
		// do the binary search on the current node
		pos, found = slices.BinarySearchFunc(node.Pairs, targetPair, func(x, k storage.Pair) int {
			return bytes.Compare(x.Key, k.Key)
		})
		log.Trace().Uint32("Node id:", node.ID).Any("Node Pairs", node.Pairs).Msg("Finding node")

		// base case is to reach a leaf node return early because either we found the target or not found and have a position to insert it
		if (node.Typ & storage.LEAF_NODE) == storage.LEAF_NODE {
			break
		}

		// read the node if it's not fetched from disk yet and convert it to node
		// root node always live in the memory
		if len(node.Children[pos].Pairs) == 0 {
			var err error = nil

			assert.Assert(node.Children[pos].ID <= b.nodeCount.Load(), "Page id can not be greater than the total number of node count")
			node.Children[pos], err = b.mng.Read(node.Children[pos].ID)
			if err != nil {
				return nil, -1, false, err
			}

			node.Children[pos].Parent = node
		}
		node = node.Children[pos]
	}

	return node, pos, found, nil
}

func (b *BTree) Find(key []byte) ([]byte, error) {
	assert.Assert(len(key) > 0 && len(key) < 65530, fmt.Sprintf("The key length must be between 0 - 65530 key: %v", string(key)))
	if !b.open {
		return nil, errors.New("Database was closed")
	}

	node, pos, found, err := b.findNode(key)

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, errors.New("Value not exist")
	}

	return node.Pairs[pos].Value, nil
}

func (b *BTree) Close() error {
	log.Info().Msg("Closed called")
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
