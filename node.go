package sapling

import "errors"

type NodeType uint8

const (
	ROOT_NODE NodeType = 1 << iota // 00000001
	INTERNAL_NODE
	LEAF_NODE
)

type pair struct {
	key   string
	value string
}

// Node is the in-memory representation of the in-disk page
type node struct {
	ID uint32
	// last child in this array is the right-most reference only has a value (reference) only applied for non-leaf nodes
	children  []*node
	parent    *node
	typ       NodeType
	dirty     bool
	pairs     []pair
	rightMost *uint32
}

// Get the disk page from the current node reference
func (n *node) page(db *BTree) (*page, error) {
	// TODO: convert the node into page
	page := &page{
		header: header{
			pageID: n.ID,
		},
	}

	if n.typ == ROOT_NODE {
		page.header.freeStart = 16 // the header size
		page.header.freeEnd = uint16(db.pageSize)
		page.header.typ = ROOT_PAGE
		page.header.cellCount = 0
		page.pointers = []pointer{}
		page.cells = []cell{}
	} else if n.typ == INTERNAL_NODE {
		return nil, errors.New("INTERNAL_NODE Not implemented yet...")
	} else if n.typ == LEAF_NODE {
		return nil, errors.New("LEAF_NODE Not implemented yet...")
	} else {
		return nil, errors.New("unknown node type")
	}

	// assume that the children always sorted

	return page, nil
}

// Add new child to the current node
func (n *node) addChild() error {
	// TODO: add child to the current node
	// Do I need to flush the page into the disk after adding new child?
	return nil
}

// Delete the current node
func (n *node) delete() error {
	// TODO: delete the current node, what is the consequences of doing so, what need to change?
	// I think this is a very hard maintainable part for the disk
	return nil
}

// Update the current node
func (n *node) update(value any) error {
	// TODO: Update the current node with the sent value
	return nil
}

// Split the current node
func (n *node) split() error {
	// TODO: Split the current node into two sibling nodes
	return nil
}

// Merge the current node
func (n *node) merge(sibling *node) error {
	// TODO: Merge the current node with it's sibling
	return nil
}
