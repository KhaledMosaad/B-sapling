package db

// An interface define what/how we can deal with the database
type DB interface {
	Find(key []byte) ([]byte, error)
	Upsert(key []byte, value []byte) (bool, bool, error)
	Remove(key []byte) error
	Close() error
}
