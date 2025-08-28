package utils

// retrieves a page offset from the the db file based on the current page size.
func GetPageOffset(id uint32, pageSize uint64) uint64 {
	pos := uint64(id) * pageSize
	return pos
}
