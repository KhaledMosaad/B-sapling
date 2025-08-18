package sapling

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
)

// This is the orchestrator object of the Database
type BTree struct {
	root      *node
	path      string
	nodeCount atomic.Uint32
	file      *os.File
	pageSize  int // platform specific page size based on system page size
	open      bool
}

// Initialize the database
func Open(path string) (*BTree, error) {
	b := &BTree{}
	b.pageSize = os.Getpagesize()
	b.path = path
	// Open file with O_DIRECT to bypass the kernel cache/write-back ...
	var err error = nil
	b.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_DIRECT, 0644)

	if err != nil {
		return nil, err
	}
	b.open = true

	// Page offsets 0 reserved, get the page 1 as it's the root

	rootPage, err := read(b, 1)

	// handle if the root page not exist create new root page
	if errors.Is(io.EOF, err) {
		// This root page must be committed to the disk fist
		b.root = &node{
			ID:       1,
			children: make([]*node, 0),
			parent:   nil,
			typ:      ROOT_NODE | INTERNAL_NODE, // due to the right most reference implementation
			dirty:    false,
		}

		rootPage, err := b.root.page(b)
		if err != nil {
			panic(err)
		}
		_, err = rootPage.flush(b)

		if err != nil {
			panic(err)
		}
		b.nodeCount.Store(1)
		return b, nil
	}

	if err != nil {
		panic(err)
	}

	b.root, err = rootPage.toNode()
	if err != nil {
		panic(err)
	}

	fi, err := b.file.Stat()
	if err != nil {
		panic(err)
	}

	b.nodeCount.Store(uint32(fi.Size() / int64(b.pageSize)))
	return b, nil
}

// Set node will find the node that will be the parent and insert new node to it then write it as a page
func (b *BTree) SetNode(key string, value string) error {
	// TODO: Create a new node, we will need to traverse the current nodes to see where we insert it, specifically the leaf nodes
	// TODO: key might be always fixed-sized value int64 will be great, but any for now
	return nil
}

// Remove node will remove the page from storage
func (b *BTree) RemoveNode(key string) error {
	// TODO: Create a new node, we will need to traverse the current nodes to see where we insert it, specifically the leaf nodes
	// TODO: key might be always fixed-sized value int64 will be great, but any for now
	return nil
}

// Find node from the database, the read path will be db.FindNode(key) => from the root node read pages until you find the needed page, return it as a node
func (b *BTree) FindNode(key string) (*node, error) {
	// TODO: Traverse the btree storage and find the leaf node with that key
	// TODO: Apply binary search for the pointer offsets access the cell and compare the value with the current search key
	// TODO: Do we need the Breadcrumbs (collecting the references we visited while traverse for better split and merge time) using stack?
	return nil, nil
}

func (b *BTree) Close() error {
	// TODO: safely close the database, call the vacuum process
	return nil
}
