package badger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/google/uuid"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

func (s *badgerInodeStore) getPrefix(parent inode) []byte {
	return []byte(fmt.Sprintf("%s%s", parent, string(filepath.Separator)))
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
		// prepare base directory

		if path.IsDir() {
			if len(data) > 0 {
				return fmt.Errorf("invalid set path expected an object, got a directory: '%s'", path)
			}
			_, err := s.MkdirAll(txn, path.Relative())
			return err
		}

		ind, err := s.MkdirAll(txn, path.Prefix)
		if err != nil {
			return err
		}

		key := s.getKey(ind, path.Base())

		if item, err := txn.Get(key); err == nil {
			current := item.UserMeta()

			if current != MetaTypeFile {
				return fmt.Errorf("object already exists as a directory: '%s'", path.Relative())
			}
		}

		entry := badger.NewEntry(key, data).WithMeta(MetaTypeFile)
		return txn.SetEntry(entry)
	})
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
			path = meta.DirPath(path.Collection, path.Relative())
			// no associated value
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

	return meta.Record{
		Path: path,
		Data: data,
		Time: stamp,
		Link: link,
	}, err
}

func (s *badgerInodeStore) Del(path meta.Path) error {
	log.WithField("path", path).Debug("Del")
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

func (s *badgerInodeStore) List(path meta.Path) ([]meta.Path, error) {
	scan, err := s.scanDelimited(path, nil, 10000)
	if err != nil {
		return nil, err
	}

	return scan.Results, nil
}

func (s *badgerInodeStore) scanDelimited(path meta.Path, after []byte, limit int) (meta.Scan, error) {
	// path is a directory, always.
	var paths []meta.Path
	var truncated bool
	err := s.db.View(func(txn *badger.Txn) error {
		dir, err := s.getDir(txn, path.Relative())
		if os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}

		prefix := s.getPrefix(dir)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		// should be one level under this prefix
		if len(after) == 0 {
			after = prefix
		}

		for it.Seek(after); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			if len(paths) >= limit {
				truncated = true
				after = key
				return nil
			}

			name := string(bytes.TrimPrefix(key, prefix))
			m := item.UserMeta()
			if m == MetaTypeDirectory {
				paths = append(paths,
					meta.NewPath(
						path.Collection,
						filepath.Join(path.Relative(), name),
						"",
					),
				)

			} else {
				paths = append(paths,
					meta.NewPath(
						path.Collection,
						path.Relative(),
						name,
					),
				)
			}
		}

		return nil
	})

	if err != nil {
		return meta.Scan{}, errors.Wrapf(err, "failed to scan '%s'", path.String())
	}

	return meta.Scan{Results: paths, Truncated: truncated, After: after}, err
}

func (s *badgerInodeStore) scanRecursive(path meta.Path, after []byte, limit int) (meta.Scan, error) {
	return meta.Scan{}, fmt.Errorf("recursive scan is not implemented")
	// var paths []meta.Path
	// err := s.db.View(func(txn *badger.Txn) error {
	// 	dir := s.getDir(txn, path)
	// 	prefix := s.getPrefix(dir)
	// 	it := txn.NewIterator(badger.DefaultIteratorOptions)
	// 	defer it.Close()
	// 	// should be one level under this prefix
	// 	if len(after) == 0 {
	// 		after = prefix
	// 	}
	// 	for it.Seek(after); it.ValidForPrefix(prefix); it.Next() {
	// 		item := it.Item()
	// 		key := item.Key()

	// 		path, err := s.keyToPath(string(key), item.UserMeta())
	// 		if err != nil {
	// 			return err
	// 		}
	// 		paths = append(paths, path)

	// 		if len(paths) >= limit {
	// 			return nil
	// 		}
	// 	}

	// 	return nil
	// })

	// return paths, err
}

func (s *badgerInodeStore) Scan(path meta.Path, after []byte, limit int, mode meta.ScanMode) (meta.Scan, error) {
	switch mode {
	case meta.ScanModeDelimited:
		return s.scanDelimited(path, after, limit)
	case meta.ScanModeRecursive:
		return s.scanRecursive(path, after, limit)
	}

	return meta.Scan{}, fmt.Errorf("unsupported scan mode: '%d'", mode)
}
