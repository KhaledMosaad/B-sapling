package sapling

import "errors"

// retrieves a page offset from the the db file based on the current page size.
func getPageOffset(id uint32, pageSize uint64) uint64 {
	pos := uint64(id) * pageSize
	return pos
}

func convertPageTypeToNodeType(pageType PageType) NodeType {
	switch pageType {
	case ROOT_PAGE:
		return ROOT_NODE
	case INTERNAL_PAGE:
		return INTERNAL_NODE
	case LEAF_PAGE:
		return LEAF_NODE
	default:
		panic(errors.New("Not Implemented page type"))
	}
}

func convertNodeTypeToPageType(nodeType NodeType) PageType {
	switch nodeType {
	case ROOT_NODE:
		return ROOT_PAGE
	case INTERNAL_NODE:
		return INTERNAL_PAGE
	case LEAF_NODE:
		return LEAF_PAGE
	default:
		panic(errors.New("Not Implemented node type"))
	}
}
