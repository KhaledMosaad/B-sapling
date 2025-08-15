package sapling

type NodeType uint8

const (
	ROOT_NODE NodeType = 1 << iota // 00000001
	INTERNAL_NODE
	LEAF_NODE
)

// Node is the in-memory representation of the in-disk page
type node struct {
	ID       uint32
	children []*node
	parent   *node
	typ      NodeType
}

// Get the disk page from the current node reference
func (n *node) page() (*page, error) {
	// TODO: convert the node into page
	return nil, nil
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
