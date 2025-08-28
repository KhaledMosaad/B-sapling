package storage

import (
	"encoding/binary"
	"sync/atomic"
)

type NodeType uint8

const (
	ROOT_NODE NodeType = 1 << iota // 00000001
	INTERNAL_NODE
	LEAF_NODE
)

type Pair struct {
	Key   []byte
	Value []byte
}

// Node is the in-memory representation of the in-disk page
type Node struct {
	// ID is equivalent to page.header.pageID
	ID uint32
	// last child in this array is the right-most reference only has a value (reference) only applied for non-leaf nodes
	// Children length will be larger than pairs length by 1 the last child for non-leaf node is right most reference
	// they are aligned with each other on sorting manner, meaning that pairs[i] has refer to the Children[i], the last child is the right-most reference
	// Children is nil if the node is leaf, in this case the pairs will have the data records
	// Children only exist for internal nodes
	Children []*Node
	Parent   *Node
	Typ      NodeType
	Dirty    bool
	Pairs    []Pair
	// The free number of bytes that the page has as free
	FreeLength int
}

// Get the disk page from the current node reference
func (n *Node) page(pageSize int) (*page, error) {
	page := &page{
		header: header{
			pageID: n.ID,
			typ:    PageType(n.Typ),
		},
	}

	nOfPairs := len(n.Pairs)
	page.header.freeStart = uint16(nOfPairs*4) + 16
	page.header.cellCount = uint16(nOfPairs)
	page.pointers = make([]pointer, nOfPairs)
	page.cells = make([]cell, nOfPairs)

	endOffset := pageSize
	for i := 0; i < nOfPairs; i++ {
		// 2 bytes for keySize +  2 bytes for ValueSize + keySize + valueSize
		keySize := uint16(len(n.Pairs[i].Key))
		valueSize := uint16(len(n.Pairs[i].Value))
		cellSize := 2 + 2 + keySize + valueSize
		page.cells[i] = cell{
			keySize:   keySize,
			valueSize: valueSize,
			key:       n.Pairs[i].Key,
			value:     n.Pairs[i].Value,
		}

		endOffset -= int(cellSize)
		page.pointers[i] = pointer{
			offset: uint16(endOffset),
			length: cellSize,
		}
	}

	// root node can be root and (internal or leaf) at the same time
	if (n.Typ & INTERNAL_NODE) == INTERNAL_NODE {
		// Internal pages handling for right most reference
		// assert the children > pairs by one
		endOffset -= 4
		page.rightMostRef = &n.Children[len(n.Pairs)].ID
	}
	page.header.freeEnd = uint16(endOffset)
	return page, nil
}

// split a node into two sibling nodes and return their parent
// The current node n will have another inserted element unless it's the root node
// In case of the root node we will make two new children leaf or internal nodes,
// the root node will have only one value, and the rest will split their values between new nodes
// and adding a rightMostRef to the root to point the new left node
func (n *Node) Split(root *Node, nodeCount *atomic.Uint32, pageSize int) (*Node, error) {
	// Root node case
	if n == root {
		if n.Typ&LEAF_NODE == LEAF_NODE {
			// we don't have rightMostRef here but n will end up with one
			midpoint := len(n.Pairs) / 2
			rightPairs := n.Pairs[0:midpoint]
			rightNodeCellsSize := accumulatePairLength(rightPairs, 0)
			rightNode := &Node{
				ID:     nodeCount.Add(1),
				Parent: n,
				Typ:    LEAF_NODE,
				Dirty:  true,
				Pairs:  rightPairs,
				// pointers size, (keySize, valueSize) - 16 page header size
				FreeLength: pageSize - 8*midpoint - 16 - rightNodeCellsSize,
			}

			leftPairs := n.Pairs[midpoint:]
			leftNodeCellsSize := accumulatePairLength(leftPairs, 0)
			leftNode := &Node{
				ID:         nodeCount.Add(1),
				Parent:     n,
				Typ:        LEAF_NODE,
				Dirty:      true,
				Pairs:      leftPairs,
				FreeLength: pageSize - 8*midpoint - 16 - leftNodeCellsSize,
			}

			n.Typ = ROOT_NODE | INTERNAL_NODE
			n.Dirty = true
			n.Parent = nil

			n.Children = []*Node{rightNode, leftNode}
			n.FreeLength = pageSize - 16 - len(n.Pairs[midpoint].Key) - 4 // page header - len of key - value size
			newKey := n.Pairs[midpoint].Key
			newValue := make([]byte, 4)
			binary.LittleEndian.PutUint32(newValue, rightNode.ID)
			n.Pairs = []Pair{{newKey, newValue}}
		}
	}
	return n, nil
}

// Delete the current node
func (n *Node) delete() error {
	// TODO: delete the current node, what is the consequences of doing so, what need to change?
	// Implement call the rebalance/merge if the sibling node + current node size < db.pageSize
	// I think this is a very hard maintainable part for the disk
	return nil
}

// It takes an initial value for the accumulation.
func accumulatePairLength(s []Pair, initial int) int {
	result := initial
	for _, p := range s {
		result += len(p.Key) + len(p.Value)
	}
	return result
}
