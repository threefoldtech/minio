package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/google/uuid"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/pkg/errors"
)

const (
	emptyInode = inode("00000000-0000-0000-0000-000000000000")
)

type inode string

func newInode() inode {
	return inode(uuid.New().String())
}

type badgerInodeStore struct {
	db *badger.DB
}

// NewBadgerInodeStore creates a new badger store. that stores
// keys in flat format. key = path.
func newBadgerInodeStore(dir string) (meta.Store, error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}

	return &badgerInodeStore{db}, nil
}

func (s *badgerInodeStore) Close() error {
	return s.db.Close()
}

// splitPath into parts. path is always absolute
func (s *badgerInodeStore) splitPath(path string) []string {
	parts := strings.Split(path, string(filepath.Separator))
	filtered := parts[:0]
	for _, part := range parts {
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}

	return filtered
}

func (s *badgerInodeStore) getKey(parent inode, name string) []byte {
	return []byte(filepath.Join(string(parent), name))
}

// path is always absolute
func (s *badgerInodeStore) MkdirAll(txn *badger.Txn, path string) (inode, error) {
	parts := s.splitPath(path)
	ind := emptyInode
	for _, part := range parts {
		key := s.getKey(ind, part)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// create a new entry in db.
			ind = newInode()
			if err := txn.SetEntry(badger.NewEntry(key, []byte(ind)).WithMeta(MetaTypeDirectory)); err != nil {
				return emptyInode, errors.Wrapf(err, "failed to create directory entry '%s' (item: '%s')", path, part)
			}
			continue
		} else if err != nil {
			return emptyInode, errors.Wrapf(err, "failed to retrieve entry '%s' (item: '%s')", path, part)
		}

		if item.UserMeta() != MetaTypeDirectory {
			return emptyInode, fmt.Errorf("path already exists as a file '%s' (item: '%s')", path, part)
		}

		if err := item.Value(func(val []byte) error {
			ind = inode(val)
			return nil
		}); err != nil {
			return emptyInode, errors.Wrapf(err, "failed to get inode of path '%s' (item: '%s')", path, part)
		}
	}

	return ind, nil
}

func (s *badgerInodeStore) Set(path meta.Path, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		//TODO: check inode cache for base
		ind, err := s.MkdirAll(txn, path.Prefix)
		if err != nil {
			return err
		}

		if path.IsDir() {
			return nil
		}
		key := s.getKey(ind, path.Base())
		entry := badger.NewEntry(key, data).WithMeta(MetaTypeFile)
		return txn.SetEntry(entry)
	})
}

func (s *badgerInodeStore) isPrefix(txn *badger.Txn, key string) bool {
	it := txn.NewIterator(isPrefixOptions)
	defer it.Close()
	k := []byte(key + "/")
	it.Seek(k)
	return it.ValidForPrefix(k)
}

func (s *badgerInodeStore) getDir(txn *badger.Txn, path string) (inode, error) {
	//todo: cache lookup in a LRU cache
	parts := s.splitPath(path)
	ind := emptyInode
	for _, part := range parts {
		key := s.getKey(ind, part)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return emptyInode, os.ErrNotExist
		}

		m := item.UserMeta()
		if m != MetaTypeDirectory {
			return emptyInode, fmt.Errorf("entry '%s' (item: '%s') is not a directory", path, path)
		}

		if err := item.Value(func(val []byte) error {
			ind = inode(val)
			return nil
		}); err != nil {
			return emptyInode, err
		}
	}

	return ind, nil
}

func (s *badgerInodeStore) Get(path meta.Path) (meta.Record, error) {
	var data []byte
	var stamp time.Time
	var link bool
	err := s.db.View(func(txn *badger.Txn) error {
		dir, base := filepath.Split(path.Relative())
		parent, err := s.getDir(txn, dir)
		if err != nil {
			return err
		}

		key := s.getKey(parent, base)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return os.ErrNotExist
		} else if err != nil {
			return errors.Wrapf(err, "failed to get object: '%s'", path)
		}

		stamp = time.Unix(int64(item.Version()), 0)
		typ := item.UserMeta()
		if typ == MetaTypeDirectory {
			path = meta.NewPath(path.Collection, path.Relative(), "")
			// no associated value
			return nil
		} else if typ == MetaTypeLink {
			link = true
		}

		path = meta.FromPath(path.Collection, path.Relative())

		return item.Value(func(val []byte) error {
			data = val
			return nil
		})

	})

	return meta.Record{
		Path: path,
		Data: data,
		Time: stamp,
		Link: link,
	}, err
}

func (s *badgerInodeStore) Del(path meta.Path) error {
	//TODO:
	// - How to delete an object
	// - Do we allow deleting of non-empty directories ?
	return fmt.Errorf("not implemented")
	// log.WithField("path", path.String()).Debug("deleting path")
	// //TODO: this should be done in patches to avoid allocating a big
	// //arrays
	// var keys [][]byte

	// err := s.db.View(func(txn *badger.Txn) error {
	// 	prefix := []byte(path.String() + "/")
	// 	it := txn.NewIterator(badger.DefaultIteratorOptions)
	// 	defer it.Close()
	// 	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
	// 		keys = append(keys, it.Item().KeyCopy(nil))
	// 	}

	// 	keys = append(keys, []byte(path.String()))
	// 	return nil
	// })

	// if err != nil {
	// 	return err
	// }

	// err = s.db.Update(func(txn *badger.Txn) error {
	// 	for _, key := range keys {
	// 		err := txn.Delete(key)
	// 		if err == badger.ErrKeyNotFound {
	// 			continue
	// 		} else if err != nil {
	// 			log.WithField("err", err).WithField("key", key).Warn("failed to delete item")
	// 		}
	// 	}

	// 	return nil
	// })

	// return err
}

func (s *badgerInodeStore) Exists(path meta.Path) (bool, error) {
	_, err := s.Get(path)
	if err == os.ErrNotExist {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (s *badgerInodeStore) Link(link, target meta.Path) error {
	if link.IsDir() {
		return fmt.Errorf("invalid link type, expecting file, got directory: %s", link)
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		parent, err := s.MkdirAll(txn, link.Prefix)
		if err != nil {
			return err
		}

		key := s.getKey(parent, link.Base())
		entry := badger.NewEntry(key, []byte(target.String())).WithMeta(MetaTypeLink)
		return txn.SetEntry(entry)
	})

	return err
}

func (s *badgerInodeStore) keyToPath(key string, m byte) (meta.Path, error) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return meta.Path{}, fmt.Errorf("invalid key format: '%s'", string(key))
	}
	collection := meta.Collection(parts[0])
	prefix := parts[1]
	if m == MetaTypeDirectory {
		return meta.NewPath(collection, prefix, ""), nil
	}

	return meta.FromPath(collection, prefix), nil
}

func (s *badgerInodeStore) List(path meta.Path) ([]meta.Path, error) {
	return s.scanDelimited(path, nil, 10000)
}

func (s *badgerInodeStore) scanDelimited(path meta.Path, after []byte, limit int) ([]meta.Path, error) {
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
			//s.splitPath(path string)
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

func (s *badgerInodeStore) scanRecursive(path meta.Path, after []byte, limit int) ([]meta.Path, error) {
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

func (s *badgerInodeStore) Scan(path meta.Path, after string, limit int, mode meta.ScanMode) ([]meta.Path, error) {
	switch mode {
	case meta.ScanModeDelimited:
		return s.scanDelimited(path, []byte(after), limit)
	case meta.ScanModeRecursive:
		return s.scanRecursive(path, []byte(after), limit)
	}

	return nil, fmt.Errorf("unsupported scan mode: '%d'", mode)
}
