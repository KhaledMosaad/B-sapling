package sapling

const HEADER_SIZE = 16

type PageType uint8

const (
	INTERNAL_PAGE PageType = 1 << iota // 00000001
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
	pointers []pointers
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

type pointers struct { // 3
	offset uint8
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
func (p *page) write(db *BTree) (bool, error) {
	// TODO: declare array of bytes with db.pageSize size
	// TODO: Add header, pointers, cells (at the end) to that array
	// TODO: flush this array of bytes into the disk
	// Take care if the page is updated not a new page
	// p.pointers come with the write offset of it's cell this is can be done on the toPage Node function
	// Do we need to sync the file on each page flush? or vacuum process to do so?
	// Do we need to separate the write from update?
	return false, nil
}

// Read page from disk
func read(db *BTree, pid uint32) (*page, error) {
	// TODO: get the offset from the disk file using the page id p.pageID

	// TODO: Assign the values into new in memory page and return it (same reference)
	return nil, nil
}

// remove page from db
func (p *page) remove(db *BTree) error {
	// TODO: remove the page from disk, add proper handling for the page ids handling
	return nil
}

// convert the current page to node (in-memory structure)
func (p *page) toNode() (*node, error) {
	// TODO: Convert the p of type page to node to facilitate the in memory usage
	return nil, nil
}
