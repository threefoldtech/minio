package badger

import (
	"context"
	"fmt"
	"os"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	log "github.com/sirupsen/logrus"
)

type badgerSimpleStore struct {
	db *badger.DB
}

const (
	//MetaTypeDirectory directory type
	MetaTypeDirectory byte = iota
	//MetaTypeFile file type
	MetaTypeFile
	//MetaTypeLink link type
	MetaTypeLink
)

var (
	isPrefixOptions = badger.IteratorOptions{PrefetchValues: false}
)

// newBadgerSimpleStore creates a new badger store. that stores
// keys in flat format. key = path.
func newBadgerSimpleStore(dir string) (meta.Store, error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}

	return &badgerSimpleStore{db}, nil
}

func (s *badgerSimpleStore) Close() error {
	return s.db.Close()
}

func (s *badgerSimpleStore) Set(path meta.Path, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if path.IsDir() {
			return fmt.Errorf("simple store does not support directory entries")
		}

		entry := badger.NewEntry([]byte(path.Relative()), data).WithMeta(MetaTypeFile)
		return txn.SetEntry(entry)
	})
}

func (s *badgerSimpleStore) isPrefix(txn *badger.Txn, key string) bool {
	it := txn.NewIterator(isPrefixOptions)
	defer it.Close()
	k := []byte(key + "/")
	it.Seek(k)
	return it.ValidForPrefix(k)
}

func (s *badgerSimpleStore) Get(path meta.Path) (meta.Record, error) {
	var data []byte
	var stamp time.Time
	var link bool
	err := s.db.View(func(txn *badger.Txn) error {
		p := path.Relative()

		item, err := txn.Get([]byte(p))
		if err == badger.ErrKeyNotFound {
			return os.ErrNotExist
		}

		stamp = time.Unix(int64(item.Version()), 0)
		typ := item.UserMeta()
		if typ == MetaTypeDirectory {
			path = meta.DirPath(path.Collection, path.Relative())
			return nil
		} else if typ == MetaTypeLink {
			link = true
		}

		path = meta.FilePath(path.Collection, path.Relative())

		return item.Value(func(val []byte) error {
			data = val
			return nil
		})

	})

	return meta.Record{Path: path, Data: data, Time: stamp, Link: link}, err
}

// Del deletes item, if path is a prefix, all keys are deleted.
func (s *badgerSimpleStore) Del(path meta.Path) error {
	log.WithField("path", path.String()).Debug("deleting path")
	//TODO: this should be done in patches to avoid allocating a big
	//arrays
	var keys [][]byte

	err := s.db.View(func(txn *badger.Txn) error {
		prefix := []byte(path.Relative() + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}

		keys = append(keys, []byte(path.Relative()))
		return nil
	})

	if err != nil {
		return err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete(key)
			if err == badger.ErrKeyNotFound {
				continue
			} else if err != nil {
				log.WithField("err", err).WithField("key", key).Warn("failed to delete item")
			}
		}

		return nil
	})

	return err
}

// Exists checks if path exists, or is a prefix
func (s *badgerSimpleStore) Exists(path meta.Path) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(path.Relative()))
		if err == badger.ErrKeyNotFound {
			exists = s.isPrefix(txn, path.Relative())
			return nil
		} else if err != nil {
			return err
		}

		exists = true
		return nil
	})

	return exists, err
}

func (s *badgerSimpleStore) Link(link, target meta.Path) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(link.Relative()), []byte(target.String())).WithMeta(MetaTypeLink)
		return txn.SetEntry(entry)
	})

	return err
}

func (s *badgerSimpleStore) List(path meta.Path) ([]meta.Path, error) {
	var paths []meta.Path
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	results, err := s.scanFlat(ctx, path)
	if err != nil {
		return nil, err
	}

	for result := range results {
		if result.Error != nil {
			return paths, result.Error
		}

		paths = append(paths, result.Path)
		if len(paths) > 10000 {
			break
		}
	}

	return paths, nil
}

func (s *badgerSimpleStore) scanFlat(ctx context.Context, path meta.Path) (<-chan meta.Scan, error) {
	prefix := []byte(path.Relative())
	ch := make(chan meta.Scan)
	push := func(s meta.Scan) bool {
		select {
		case ch <- s:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(ch)

		err := s.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			// should be one level under this prefix

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				key := item.Key()

				typ := item.UserMeta()
				var itemPath meta.Path
				if typ == MetaTypeDirectory {
					itemPath = meta.DirPath(path.Collection, string(key))
				} else {
					itemPath = meta.FilePath(path.Collection, string(key))
				}

				if !push(meta.Scan{Path: itemPath}) {
					return nil
				}
			}

			return nil
		})

		if err != nil {
			push(meta.Scan{Error: err})
		}
	}()

	return ch, nil
}

func (s *badgerSimpleStore) Scan(ctx context.Context, path meta.Path, mode meta.ScanMode) (<-chan meta.Scan, error) {
	switch mode {
	case meta.ScanModeFlat:
		return s.scanFlat(ctx, path)
	default:
		return nil, fmt.Errorf("unsupported scan mode: '%d'", mode)
	}
}
