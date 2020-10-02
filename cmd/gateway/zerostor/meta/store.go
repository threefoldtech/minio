package meta

import (
	"context"
	"fmt"
	"time"
)

var (
	// ErrUnsupportedScanMode error
	ErrUnsupportedScanMode = fmt.Errorf("unsupported scan mode for this store type")
)

//ScanMode scan mode type
type ScanMode int

const (
	// ScanModeFlat scans entire prefix recursively
	ScanModeFlat = iota
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
	Scan(ctx context.Context, path Path, mode ScanMode) (<-chan Scan, error)
	Close() error
}

// Scan is result of a scan process
type Scan struct {
	Path  Path
	Error error
}
