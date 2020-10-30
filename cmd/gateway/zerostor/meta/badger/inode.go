package badger

import (
	"bytes"
	"context"
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

var (
	errNotDirectory = fmt.Errorf("entry is not a directory")
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
			return emptyInode, errNotDirectory
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

// NOTICE: Get on a inode store does NOT follow links. But that
// is okay because simple store is not public. Hence users can't
// make an instance of simple store. the public store interface
// follows the interface rules of following links.
func (s *badgerInodeStore) Get(path meta.Path) (meta.Record, error) {
	_, record, err := s.get(path)
	return record, err
}

func (s *badgerInodeStore) get(path meta.Path) ([]byte, meta.Record, error) {
	var data []byte
	var stamp time.Time
	var link bool
	var key []byte
	err := s.db.View(func(txn *badger.Txn) error {
		dir, base := filepath.Split(path.Relative())
		parent, err := s.getDir(txn, dir)
		if err != nil {
			return err
		}

		key = s.getKey(parent, base)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return os.ErrNotExist
		} else if err != nil {
			return errors.Wrapf(err, "failed to get object: '%s'", path)
		}

		stamp = time.Unix(int64(item.Version()), 0)
		typ := item.UserMeta()
		path = meta.FilePath(path.Collection, path.Relative())

		if typ == MetaTypeDirectory {
			path = meta.DirPath(path.Collection, path.Relative())
		} else if typ == MetaTypeLink {
			link = true
		}

		// the value here is different for each Type
		// a dir value is it's inode
		// a file value is used defined data
		// a link value is the target path
		return item.Value(func(val []byte) error {
			data = val
			return nil
		})
	})

	return key, meta.Record{
		Path: path,
		Data: data,
		Time: stamp,
		Link: link,
	}, err
}

func (s *badgerInodeStore) Del(path meta.Path) error {
	key, record, err := s.get(path)
	if err == os.ErrNotExist {
		return nil
	} else if err != nil {
		return err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		if !record.Path.IsDir() {
			// it is a file or a link.
			// can be deleted. first fine the parent
			return txn.Delete(key)
		}

		// it's a directory, we first get it's inode
		ind, err := s.getDir(txn, path.Relative())
		if err != nil {
			return err
		}
		// scan it see if it has ANY objects
		prefix := s.getPrefix(ind)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		notEmpty := false
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			notEmpty = true
			break
		}

		if notEmpty {
			return meta.ErrDirectoryNotEmpty
		}

		return txn.Delete(key)
	})

	return err
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
	var paths []meta.Path
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	results, err := s.scanDelimited(ctx, path)
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

func (s *badgerInodeStore) scanDelimited(ctx context.Context, path meta.Path) (<-chan meta.Scan, error) {
	// path is a directory, always.
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
			dir, err := s.getDir(txn, path.Relative())
			if os.IsNotExist(err) || err == errNotDirectory {
				return nil
			} else if err != nil {
				return err
			}

			prefix := s.getPrefix(dir)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				key := item.Key()

				name := string(bytes.TrimPrefix(key, prefix))
				m := item.UserMeta()
				var entry meta.Path

				if m == MetaTypeDirectory {
					entry = meta.DirPath(
						path.Collection,
						path.Relative(), name,
					)
				} else {
					entry = meta.FilePath(
						path.Collection,
						path.Relative(),
						name,
					)
				}

				if !push(meta.Scan{Path: entry}) {
					break
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

func (s *badgerInodeStore) scanRecursive(path meta.Path, after []byte, limit int) (meta.Scan, error) {
	return meta.Scan{}, fmt.Errorf("recursive scan is not implemented")
}

func (s *badgerInodeStore) Scan(ctx context.Context, path meta.Path, mode meta.ScanMode) (<-chan meta.Scan, error) {
	switch mode {
	case meta.ScanModeDelimited:
		return s.scanDelimited(ctx, path)
	default:
		return nil, meta.ErrUnsupportedScanMode
	}

}
