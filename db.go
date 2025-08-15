package sapling

import "os"

// This is the orchestrator object of the Database
type BTree struct {
	root      *node
	path      string
	nodeCount uint32
	file      *os.File
	pageSize  int // platform specific page size based on system memory page size
	open      bool
}

// Initialize the database
func (b *BTree) init(path string) error {
	b.pageSize = os.Getpagesize()
	b.path = path
	// TODO: Initialize the database BTree object with default values
	// TODO: If the database not exist create the file and insert the root node
	// TODO: Initialize the file header and make the root page at page 1, meaning that the first pageSize is reserved
	return nil
}

// Add node will find the node that will be the parent and insert new node to it then write it as a page
func (b *BTree) AddNode(key any, value any) error {
	// TODO: Create a new node, we will need to traverse the current nodes to see where we insert it, specifically the leaf nodes
	// TODO: key might be always fixed-sized value int64 will be great, but any for now
	return nil
}

// Remove node will remove the page from storage
func (b *BTree) RemoveNode(key any) error {
	// TODO: Create a new node, we will need to traverse the current nodes to see where we insert it, specifically the leaf nodes
	// TODO: key might be always fixed-sized value int64 will be great, but any for now
	return nil
}

// Find node from the database, the read path will be db.FindNode(key) => from the root node read pages until you find the needed page, return it as a node
func (b *BTree) FindNode(key any) (*node, error) {
	// TODO: Traverse the btree storage and find the leaf node with that key
	return nil, nil
}

func (b *BTree) Close() error {
	// TODO: safely close the database
	return nil
}
