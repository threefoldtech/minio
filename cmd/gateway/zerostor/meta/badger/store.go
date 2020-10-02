package badger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/pkg/errors"
)

type badgerStore struct {
	objects  meta.Store
	uploads  meta.Store
	blobs    meta.Store
	buckets  meta.Store
	versions meta.Store
}

// NewBadgerStore create a new badger store
func NewBadgerStore(root string) (meta.Store, error) {
	if err := os.MkdirAll(root, 0755); err != nil && err != os.ErrExist {
		return nil, errors.Wrap(err, "failed to create store directories")
	}

	store := badgerStore{}
	var err error

	cleanup := func() {
		store.Close()
	}

	store.blobs, err = newBadgerSimpleStore(filepath.Join(root, string(meta.BlobCollection)))
	if err != nil {
		cleanup()
		return nil, err
	}

	store.uploads, err = newBadgerSimpleStore(filepath.Join(root, string(meta.UploadCollection)))
	if err != nil {
		cleanup()
		return nil, err
	}

	store.buckets, err = newBadgerSimpleStore(filepath.Join(root, string(meta.BucketCollection)))
	if err != nil {
		cleanup()
		return nil, err
	}

	store.versions, err = newBadgerSimpleStore(filepath.Join(root, string(meta.VersionCollection)))
	if err != nil {
		cleanup()
		return nil, err
	}

	store.objects, err = newBadgerInodeStore(filepath.Join(root, string(meta.ObjectCollection)))
	if err != nil {
		cleanup()
		return nil, err
	}

	return &store, nil
}

func (s *badgerStore) storeFor(collection meta.Collection) (meta.Store, error) {
	var store meta.Store
	switch collection {
	case meta.BlobCollection:
		store = s.blobs
	case meta.ObjectCollection:
		store = s.objects
	case meta.UploadCollection:
		store = s.uploads
	case meta.BucketCollection:
		store = s.buckets
	case meta.VersionCollection:
		store = s.versions
	}

	if store == nil {
		return nil, fmt.Errorf("store for collection '%s' is not initalized", collection)
	}

	return store, nil
}

func (s *badgerStore) Set(path meta.Path, data []byte) error {
	store, err := s.storeFor(path.Collection)
	if err != nil {
		return err
	}

	return store.Set(path, data)
}

func (s *badgerStore) Get(path meta.Path) (meta.Record, error) {

	for {
		store, err := s.storeFor(path.Collection)
		if err != nil {
			return meta.Record{}, err
		}

		record, err := store.Get(path)
		if err != nil {
			return record, err
		}

		if !record.Link {
			return record, err
		}

		parts := strings.SplitN(string(record.Data), string(filepath.Separator), 2)
		if len(parts) != 2 {
			return record, fmt.Errorf("invalid link format: (%s) -> (%s)", path.String(), string(record.Data))
		}

		path = meta.FilePath(meta.Collection(parts[0]), parts[1])
	}

}

func (s *badgerStore) Del(path meta.Path) error {
	store, err := s.storeFor(path.Collection)
	if err != nil {
		return err
	}

	return store.Del(path)
}

func (s *badgerStore) Exists(path meta.Path) (bool, error) {
	store, err := s.storeFor(path.Collection)
	if err != nil {
		return false, err
	}

	return store.Exists(path)
}

func (s *badgerStore) Link(link, target meta.Path) error {
	store, err := s.storeFor(link.Collection)
	if err != nil {
		return err
	}

	return store.Link(link, target)
}

func (s *badgerStore) List(path meta.Path) ([]meta.Path, error) {
	store, err := s.storeFor(path.Collection)
	if err != nil {
		return nil, err
	}

	return store.List(path)
}

func (s *badgerStore) Scan(ctx context.Context, path meta.Path, mode meta.ScanMode) (<-chan meta.Scan, error) {
	store, err := s.storeFor(path.Collection)
	if err != nil {
		return nil, err
	}

	return store.Scan(ctx, path, mode)
}

func (s *badgerStore) Close() error {
	for _, store := range []meta.Store{s.blobs, s.buckets, s.objects, s.uploads} {
		if store == nil {
			continue
		}

		store.Close()
	}

	return nil
}
