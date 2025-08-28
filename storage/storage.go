package storage

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
)

type StorageManager interface {
	Read(nid uint32) (*Node, error)
	Write(n *Node) (bool, error)
	Split(n *Node) (*Node, error)
	WriteNodeTree(n *Node) error
	Close() error
}

// This is a btree storage manager struct
type Manager struct {
	PageSize int
	file     *os.File
	path     string
}

var _ StorageManager = &Manager{}

// A new Manager return mnger, root, error
func NewManager(pageSize int, path string, nodeCount *atomic.Uint32) (*Manager, *Node, error) {
	mng := &Manager{
		path:     path,
		PageSize: pageSize,
	}
	var err error = nil

	// Open file with O_DIRECT to bypass the kernel cache/write-back ...
	mng.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_DIRECT, 0644)

	if err != nil {
		return nil, nil, err
	}

	rootPage, err := read(mng, 1)

	// handle if the root page not exist create new root page
	if errors.Is(err, io.EOF) {
		// This root page must be committed to the disk first
		root := &Node{
			ID:       1,
			Children: nil,
			Parent:   nil,
			// due to the right most reference implementation, treat the root node as a leaf until first split operation
			// and will be converted to ROOT | INTERNAL on split promotion
			Typ:        ROOT_NODE | LEAF_NODE,
			Dirty:      false,
			FreeLength: mng.PageSize - HEADER_SIZE,
		}

		rootPage, err := root.page(mng.PageSize)
		if err != nil {
			panic(err)
		}
		_, err = rootPage.flush(mng)

		if err != nil {
			panic(err)
		}
		nodeCount.Store(1)
		return mng, root, nil
	}

	if err != nil {
		panic(err)
	}

	root, err := rootPage.toNode()
	if err != nil {
		panic(err)
	}

	fi, err := mng.file.Stat()
	if err != nil {
		panic(err)
	}

	nodeCount.Store(uint32(fi.Size() / int64(mng.PageSize)))

	return mng, root, nil
}

// This is a read operation happening on the disk
// It will Read a page from disk and return it's node
func (mng *Manager) Read(nid uint32) (*Node, error) {
	// Page offsets 0 reserved, get the page 1 as it's the root
	page, err := read(mng, nid)
	if err != nil {
		return nil, err
	}

	node, err := page.toNode()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (mng *Manager) Write(n *Node) (bool, error) {

	return false, nil
}

func (mng *Manager) Split(n *Node) (*Node, error) {
	return nil, nil
}

// Run basic DFS on the tree and write dirty pages starting from n to the end of the tree
func (mng *Manager) WriteNodeTree(n *Node) error {
	if n.Dirty {
		page, err := n.page(mng.PageSize)
		if err != nil {
			return err
		}

		written, err := page.flush(mng)
		if err != nil {
			return err
		}

		if !written {
			return errors.New("The page didn't written in the db file with unknown reason for now.")
		}
		n.Dirty = false
	}

	// go depth for internal memory nodes to write it
	if n.Typ&INTERNAL_NODE == INTERNAL_NODE {
		for i := 0; i < len(n.Children); i++ {
			if err := mng.WriteNodeTree(n.Children[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mng *Manager) Close() error {
	err := mng.file.Close()
	if err != nil {
		return err
	}
	return nil
}
