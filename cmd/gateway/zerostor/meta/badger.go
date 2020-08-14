package meta

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type badgerStore struct {
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

//NewBadgerStore creates a new badger store
func NewBadgerStore(dir string) (Store, error) {
	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dir, "db")))
	if err != nil {
		return nil, err
	}

	return &badgerStore{db}, nil
}

func (s *badgerStore) Close() error {
	return s.db.Close()
}

func (s *badgerStore) Set(path Path, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		m := MetaTypeFile
		if path.IsDir() {
			if len(data) < 0 {
				return fmt.Errorf("invalid set path expected an object, got a directory: '%s'", path)
			}

			m = MetaTypeDirectory
		}

		entry := badger.NewEntry([]byte(path.String()), data).WithMeta(m)
		return txn.SetEntry(entry)
	})
}

func (s *badgerStore) isPrefix(txn *badger.Txn, key string) bool {
	it := txn.NewIterator(isPrefixOptions)
	defer it.Close()
	k := []byte(key + "/")
	it.Seek(k)
	return it.ValidForPrefix(k)
}

func (s *badgerStore) Get(path Path) (Record, error) {
	var data []byte
	var stamp time.Time
	err := s.db.View(func(txn *badger.Txn) error {
		p := path.String()

		for {
			item, err := txn.Get([]byte(p))
			if err == badger.ErrKeyNotFound {
				if s.isPrefix(txn, p) {
					path = NewPath(path.Collection, path.Relative(), "")
					return nil
				}
				// not even a prefix. return err not exist
				return os.ErrNotExist
			}

			stamp = time.Unix(int64(item.Version()), 0)
			typ := item.UserMeta()
			if typ == MetaTypeDirectory {
				path = NewPath(path.Collection, path.Relative(), "")
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

			path = FromPath(path.Collection, path.Relative())

			return item.Value(func(val []byte) error {
				data = val
				return nil
			})
		}

	})

	return Record{Path: path, Data: data, Time: stamp}, err
}

func (s *badgerStore) Del(path Path) error {
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

func (s *badgerStore) Exists(path Path) (bool, error) {
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

func (s *badgerStore) Link(link, target Path) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(link.String()), []byte(target.String())).WithMeta(MetaTypeLink)
		return txn.SetEntry(entry)
	})

	return err
}

func (s *badgerStore) keyToPath(key string, meta byte) (Path, error) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return Path{}, fmt.Errorf("invalid key format: '%s'", string(key))
	}
	collection := Collection(parts[0])
	prefix := parts[1]
	if meta == MetaTypeDirectory {
		return NewPath(collection, prefix, ""), nil
	}

	return FromPath(collection, prefix), nil
}

func (s *badgerStore) List(path Path) ([]Path, error) {
	return s.scanDelimited(path, nil, 10000)
}

func (s *badgerStore) scanDelimited(path Path, after []byte, limit int) ([]Path, error) {
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

	var paths []Path

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

func (s *badgerStore) scanRecursive(path Path, after []byte, limit int) ([]Path, error) {
	prefix := []byte(path.String())
	var paths []Path
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

func (s *badgerStore) Scan(path Path, after string, limit int, mode ScanMode) ([]Path, error) {
	switch mode {
	case ScanModeDelimited:
		return s.scanDelimited(path, []byte(after), limit)
	case ScanModeRecursive:
		return s.scanRecursive(path, []byte(after), limit)
	}

	return nil, fmt.Errorf("unsupported scan mode: '%d'", mode)
}
