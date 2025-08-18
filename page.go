package sapling

import (
	"encoding/binary"
	"log"
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
	// TODO: Handle the right most value for the page so that the page will have (pointers + 1) references, this is only apply for non-leaf pages
	// For the internal cells the valueSize must be 4 and the value must be valid pageID
	cells []cell
}

// TODO: add checksum value to the page header
type header struct {
	// PAGE HEADER 16 Byte
	pageID    uint32   // 4
	freeStart uint16   // 2
	freeEnd   uint16   // 2
	cellCount uint16   // 2
	typ       PageType // 1

	reserved [5]byte // 5
}

type pointer struct { // 4
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
func (p *page) flush(db *BTree) (bool, error) {
	buff := make([]byte, db.pageSize)

	pageOffset := int64(p.header.pageID * uint32(db.pageSize))

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
	endOffset := db.pageSize
	for i := 0; i < len(p.pointers); i++ {
		pointer := p.pointers[i]

		// Add the pointer offset and length to the buffer
		binary.LittleEndian.PutUint16(buff[offset:], pointer.offset)
		offset += 2
		binary.LittleEndian.PutUint16(buff[offset:], pointer.length)
		offset += 2

		// Add cell to the buffer based on the pointer
		// TODO: I AM HERE
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
	// TODO: Handle the right most value for the page so that the page will have (pointers + 1) references, this is only apply for non-leaf pages

	// n, err := buff.WriteTo(db.file)
	n, err := db.file.WriteAt(buff, pageOffset)
	if err != nil {
		return false, err
	}

	log.Printf("written %v bytes to disk at pageID %v", n, p.header.pageID)
	return true, nil
}

// Read page from disk
func read(db *BTree, pid uint32) (*page, error) {
	poffset := getPageOffset(pid, uint64(db.pageSize))

	buff := make([]byte, db.pageSize)
	_, err := db.file.ReadAt(buff, int64(poffset))

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
		point.length = binary.LittleEndian.Uint16(buff[offset+2:])
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
	// TODO: Handle the right most value for the page so that the page will have (pointers + 1) references, this is only apply for non-leaf pages

	return page, nil
}

// remove page from db
func (p *page) remove(db *BTree) error {
	// TODO: remove the page from disk, add proper handling for the page ids handling
	return nil
}

// convert the current page to node (in-memory structure)
func (p *page) toNode() (*node, error) {
	node := &node{
		ID:     p.header.pageID,
		typ:    convertPageTypeToNodeType(p.header.typ),
		parent: nil,
		dirty:  false,
		pairs:  make([]pair, len(p.cells)),
	}

	for i := 0; i < len(p.cells); i++ {
		pair := pair{
			key:   string(p.cells[i].key),
			value: string(p.cells[i].value),
		}

		node.pairs[i] = pair
	}
	return node, nil
}
