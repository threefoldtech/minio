package meta

import "time"

//ScanMode scan mode type
type ScanMode int

const (
	// ScanModeRecursive scans entire prefix recursively
	ScanModeRecursive = iota
	// ScanModeDelimited only scans direct object under a prefix
	ScanModeDelimited
)

// Record type returned by a store.Scan operation
type Record struct {
	Path Path
	Data []byte
	Time time.Time
	Link bool
}

// Store defines the interface to a low level metastore
type Store interface {
	Set(path Path, data []byte) error
	Get(path Path) (Record, error)
	Del(path Path) error
	Exists(path Path) (bool, error)
	Link(link, target Path) error
	List(path Path) ([]Path, error)
	Scan(path Path, after []byte, limit int, mode ScanMode) (Scan, error)
	Close() error
}

// Scan is result of a scan process
type Scan struct {
	Truncated bool
	After     []byte
	Results   []Path
}
