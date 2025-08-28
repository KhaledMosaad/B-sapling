package storage

import (
	"encoding/binary"
	"log"

	"github.com/KhaledMosaad/B-sapling/utils"
)

const HEADER_SIZE = 16

type PageType uint8

const (
	ROOT_PAGE PageType = 1 << iota // 00000001
	INTERNAL_PAGE
	LEAF_PAGE
)

/*
 * This is a in-disk slotted pages implementation for B+Tree
 * Pages have header, offsets pointers to the cells (ordered), cells, free space, (optional) available offsets pairs (from, to)
 * Page is the node representation in the disk slotted pages looks like this:
 * +----------------+---------------------------------+
 * | PageHeader | pointer1 pointer2 pointer3 ...      |
 * +-----------+----+---------------------------------+
 * | ... pointerN |                                   |
 * +-----------+--------------------------------------+
 * |           ^ freeStart                            |
 * |                                                  |
 * |             v freeEnd                            |
 * +-------------+------------------------------------+
 * |             | dataN ...                          |
 * +-------------+------------------+-----------------+
 * |       ... data4 data3 data2 data1                |
 * +--------------------------------+-----------------+
 *
 */
type page struct {
	header   header
	pointers []pointer
	// For the internal cells the valueSize must be 4 and the value must be valid pageID
	cells []cell
	// This is only applied for non-leaf nodes, will be taken from nodes.children[len(nodes.children)-1]
	rightMostRef *uint32
}

type header struct {
	// PAGE HEADER 16 Byte
	pageID    uint32   // 4
	freeStart uint16   // 2
	freeEnd   uint16   // 2
	cellCount uint16   // 2
	typ       PageType // 1

	reserved [5]byte // 5
}

type pointer struct {
	offset uint16
	length uint16
}

// nodes/pages will hold cells contains the data records, variable sized cells incase of leaf node and pageID size incase of internal page/node
type cell struct {
	keySize   uint16 // 2
	valueSize uint16 // 2
	key       []byte
	value     []byte
}

// write (create, update) the page in disk using Little endian byte order
func (p *page) flush(mng *Manager) (bool, error) {
	buff := make([]byte, mng.PageSize)

	pageOffset := int64(p.header.pageID * uint32(mng.PageSize))

	// assign header
	offset := 0

	binary.LittleEndian.PutUint32(buff[offset:], p.header.pageID)
	offset += 4

	binary.LittleEndian.PutUint16(buff[offset:], p.header.freeStart)
	offset += 2

	binary.LittleEndian.PutUint16(buff[offset:], p.header.freeEnd)
	offset += 2

	binary.LittleEndian.PutUint16(buff[offset:], p.header.cellCount)
	offset += 2

	buff[offset] = byte(p.header.typ)
	offset += 6 // 1 for typ + 5 reserved

	// The update/insert will rewrite the whole page
	// pointers grows down the page (from the start to the end)
	// calculate them in the Node.toPage function, the pointer offset will be internally offset
	endOffset := mng.PageSize
	for i := 0; i < len(p.pointers); i++ {
		pointer := p.pointers[i]

		// Add the pointer offset and length to the buffer
		binary.LittleEndian.PutUint16(buff[offset:], pointer.offset)
		offset += 2
		binary.LittleEndian.PutUint16(buff[offset:], pointer.length)
		offset += 2

		// Add cell to the buffer based on the pointer
		endOffset -= int(pointer.length)
		cellOffset := endOffset
		binary.LittleEndian.PutUint16(buff[cellOffset:], p.cells[i].keySize)
		cellOffset += 2
		binary.LittleEndian.PutUint16(buff[cellOffset:], p.cells[i].valueSize)
		cellOffset += 2
		copy(buff[cellOffset:], p.cells[i].key)
		cellOffset += int(p.cells[i].keySize)
		copy(buff[cellOffset:], p.cells[i].value)
	}

	// Handle the right most value for the page so that the page will have (pointers + 1) references, this is only apply for non-leaf pages
	if p.header.typ&INTERNAL_PAGE == INTERNAL_PAGE {
		// the right most reference has 4 bytes length
		endOffset -= 4
		binary.LittleEndian.PutUint32(buff[endOffset:], *p.rightMostRef)
	}

	n, err := mng.file.WriteAt(buff, pageOffset)
	if err != nil {
		return false, err
	}

	log.Printf("written %v bytes to disk at pageID %v", n, p.header.pageID)
	return true, nil
}

// Read page from disk
func read(mng *Manager, pid uint32) (*page, error) {
	poffset := utils.GetPageOffset(pid, uint64(mng.PageSize))

	buff := make([]byte, mng.PageSize)
	_, err := mng.file.ReadAt(buff, int64(poffset))

	if err != nil {
		return nil, err
	}

	page := &page{}
	offset := 0

	// Page Header reading
	page.header.pageID = binary.LittleEndian.Uint32(buff[offset:])
	offset += 4
	page.header.freeStart = binary.LittleEndian.Uint16(buff[offset:])
	offset += 2
	page.header.freeEnd = binary.LittleEndian.Uint16(buff[offset:])
	offset += 2
	page.header.cellCount = binary.LittleEndian.Uint16(buff[offset:])
	offset += 2
	page.header.typ = PageType(buff[offset])
	offset += 6 // typ = 1 , reserved = 5

	// append cells and pointers
	for offset < int(page.header.freeStart) {
		// append pointer
		point := pointer{}
		point.offset = binary.LittleEndian.Uint16(buff[offset:])
		offset += 2
		point.length = binary.LittleEndian.Uint16(buff[offset:])
		offset += 2
		page.pointers = append(page.pointers, point)

		// append cell
		cell := cell{}

		cellOffset := point.offset
		cell.keySize = binary.LittleEndian.Uint16(buff[cellOffset:])
		cellOffset += 2
		cell.valueSize = binary.LittleEndian.Uint16(buff[cellOffset:])
		cellOffset += 2

		cell.key = buff[cellOffset : cellOffset+cell.keySize]

		cellOffset += cell.keySize
		cell.value = buff[cellOffset : cellOffset+cell.valueSize]

		page.cells = append(page.cells, cell)
	}

	// Handle the right most value for the page so that the page will have (pointers + 1) references, this is only apply for non-leaf pages
	// Any read internal page should fetch the right most value
	if page.header.typ&INTERNAL_PAGE == INTERNAL_PAGE {
		temp := binary.LittleEndian.Uint32(buff[page.header.freeEnd:])
		page.rightMostRef = &temp
	}

	log.Printf("read page %v from disk", page.header.pageID)
	return page, nil
}

// remove page from db
func (p *page) remove(mng *Manager) error {
	// TODO: remove the page from disk, add proper handling for the page ids handling
	// This can be done when implementing file header to reference the unused pages
	return nil
}

// convert the current page to node (in-memory structure)
func (p *page) toNode() (*Node, error) {
	nod := &Node{
		ID:         p.header.pageID,
		Typ:        NodeType(p.header.typ),
		Parent:     nil,
		Dirty:      false,
		Pairs:      make([]Pair, len(p.cells)),
		FreeLength: int(p.header.freeEnd - p.header.freeStart),
	}

	if (nod.Typ & INTERNAL_NODE) == INTERNAL_NODE {
		// assert that the p.rightMostRef is not nil or 0
		nod.Children = make([]*Node, len(p.cells)+1)
		nod.Children[len(p.cells)] = &Node{ID: *p.rightMostRef}
	}

	for i := 0; i < len(p.cells); i++ {
		pair := Pair{
			Key:   p.cells[i].key,
			Value: p.cells[i].value,
		}
		nod.Pairs[i] = pair

		// internal nodes should have value of uint32 as a reference for the child page
		// FIXME: What if they already exist in the memory? This need to be handled on the eviction policy
		// Most of the time it won't be exist but just incase, we need to handle this
		if (nod.Typ & INTERNAL_NODE) == INTERNAL_NODE {
			nod.Children[i] = &Node{
				ID: binary.LittleEndian.Uint32(pair.Value),
			}
		}
	}
	return nod, nil
}
