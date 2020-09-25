package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/pkg/errors"
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

// NewBadgerSimpleStore creates a new badger store. that stores
// keys in flat format. key = path.
func NewBadgerSimpleStore(dir string) (meta.Store, error) {
	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "db")))
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
		m := MetaTypeFile
		if path.IsDir() {
			if len(data) > 0 {
				return fmt.Errorf("invalid set path expected an object, got a directory: '%s'", path)
			}

			m = MetaTypeDirectory
		}

		entry := badger.NewEntry([]byte(path.String()), data).WithMeta(m)
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
	err := s.db.View(func(txn *badger.Txn) error {
		p := path.String()

		for {
			item, err := txn.Get([]byte(p))
			if err == badger.ErrKeyNotFound {
				if s.isPrefix(txn, p) {
					path = meta.NewPath(path.Collection, path.Relative(), "")
					return nil
				}
				// not even a prefix. return err not exist
				return os.ErrNotExist
			}

			stamp = time.Unix(int64(item.Version()), 0)
			typ := item.UserMeta()
			if typ == MetaTypeDirectory {
				path = meta.NewPath(path.Collection, path.Relative(), "")
				return nil
			} else if typ == MetaTypeLink {
				// resolve the target and try again
				err := item.Value(func(val []byte) error {
					p = string(val)
					return nil
				})

				if err != nil {
					return err
				}
				continue
			}

			path = meta.FilePath(path.Collection, path.Relative())

			return item.Value(func(val []byte) error {
				data = val
				return nil
			})
		}

	})

	return meta.Record{Path: path, Data: data, Time: stamp}, err
}

func (s *badgerSimpleStore) Del(path meta.Path) error {
	log.WithField("path", path.String()).Debug("deleting path")
	//TODO: this should be done in patches to avoid allocating a big
	//arrays
	var keys [][]byte

	err := s.db.View(func(txn *badger.Txn) error {
		prefix := []byte(path.String() + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}

		keys = append(keys, []byte(path.String()))
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

func (s *badgerSimpleStore) Exists(path meta.Path) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(path.String()))
		if err == badger.ErrKeyNotFound {
			exists = s.isPrefix(txn, path.String())
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
		entry := badger.NewEntry([]byte(link.String()), []byte(target.String())).WithMeta(MetaTypeLink)
		return txn.SetEntry(entry)
	})

	return err
}

func (s *badgerSimpleStore) keyToPath(key string, m byte) (meta.Path, error) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return meta.Path{}, fmt.Errorf("invalid key format: '%s'", string(key))
	}
	collection := meta.Collection(parts[0])
	prefix := parts[1]
	if m == MetaTypeDirectory {
		return meta.NewPath(collection, prefix, ""), nil
	}

	return meta.FilePath(collection, prefix), nil
}

func (s *badgerSimpleStore) List(path meta.Path) ([]meta.Path, error) {
	return s.scanDelimited(path, nil, 10000)
}

func (s *badgerSimpleStore) scanDelimited(path meta.Path, after []byte, limit int) ([]meta.Path, error) {
	entries := make(map[string]byte)
	trim := path.String() + "/"
	prefix := []byte(path.String() + "/")
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		// should be one level under this prefix
		if len(after) == 0 {
			after = prefix
		}

		for it.Seek(after); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			key := item.Key()
			strKey := string(key)
			//filepath.SplitList(path string)
			strKey = strings.TrimPrefix(strKey, trim)
			parts := strings.Split(strKey, "/")
			if len(parts) == 0 {
				//how ?
				continue
			} else if len(parts) == 1 {
				// direct object
				entries[parts[0]] = item.UserMeta()
			} else {
				// more than one, that a sub dir entry
				// with no directory entry
				entries[parts[0]] = MetaTypeDirectory
			}

			if len(entries) >= limit {
				return nil
			}
		}

		return nil
	})

	if err != nil {
		return nil, errors.Wrapf(err, "failed to scan '%s'", path.String())
	}

	var paths []meta.Path

	for entry, meta := range entries {
		key := filepath.Join(trim, entry)
		path, err := s.keyToPath(key, meta)
		if err != nil {
			return paths, err
		}
		paths = append(paths, path)
	}

	return paths, err
}

func (s *badgerSimpleStore) scanRecursive(path meta.Path, after []byte, limit int) ([]meta.Path, error) {
	prefix := []byte(path.String())
	var paths []meta.Path
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		// should be one level under this prefix
		if len(after) == 0 {
			after = prefix
		}
		for it.Seek(after); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			path, err := s.keyToPath(string(key), item.UserMeta())
			if err != nil {
				return err
			}
			paths = append(paths, path)

			if len(paths) >= limit {
				return nil
			}
		}

		return nil
	})

	return paths, err
}

func (s *badgerSimpleStore) Scan(path meta.Path, after string, limit int, mode meta.ScanMode) ([]meta.Path, error) {
	switch mode {
	case meta.ScanModeDelimited:
		return s.scanDelimited(path, []byte(after), limit)
	case meta.ScanModeRecursive:
		return s.scanRecursive(path, []byte(after), limit)
	}

	return nil, fmt.Errorf("unsupported scan mode: '%d'", mode)
}
