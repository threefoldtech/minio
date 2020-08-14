// +build linux,filesystem

package meta

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

const (
	bucketsDir = "buckets"
	blobsDir   = "blobs"
	objectsDir = "objects"
	uploadsDir = "uploads"
)

type filesystemStore struct {
	buckets string
	blobs   string
	objects string
	uploads string
}

func ensureDirAll(dir string) error {
	err := os.MkdirAll(dir, 0766)
	if os.IsExist(err) {
		return nil
	}

	return err
}

// NewFilesystemStore create a new filesystem store
func NewFilesystemStore(root string) (Store, error) {
	buckets := filepath.Join(root, bucketsDir)
	blobs := filepath.Join(root, blobsDir)
	objects := filepath.Join(root, objectsDir)
	uploads := filepath.Join(root, uploadsDir)
	for _, dir := range []string{buckets, blobs, objects, uploads} {
		if err := ensureDirAll(buckets); err != nil {
			return nil, errors.Wrapf(err, "failed to create store directory: %s", dir)
		}
	}

	return &filesystemStore{
		buckets: buckets,
		blobs:   blobs,
		objects: objects,
		uploads: uploads,
	}, nil
}

func (s *filesystemStore) Close() error {
	return nil
}

func (s *filesystemStore) rel(path Path) (string, error) {
	var root string
	switch path.Collection {
	case BucketCollection:
		root = s.buckets
	case BlobCollection:
		root = s.blobs
	case ObjectCollection:
		root = s.objects
	case UploadCollection:
		root = s.uploads
	default:
		return "", fmt.Errorf("unknown collection '%s'", path.Collection)
	}

	return filepath.Join(root, path.Prefix, path.Name), nil
}

func (s *filesystemStore) Set(path Path, data []byte) error {
	rel, err := s.rel(path)
	if err != nil {
		return err
	}

	if path.IsDir() {
		if len(data) < 0 {
			return fmt.Errorf("invalid set path expected an object, got a directory: '%s'", path)
		}

		if err := ensureDirAll(rel); err != nil {
			return errors.Wrapf(err, "failed to create dir '%s'", rel)
		}

		return nil
	}

	dir := filepath.Dir(rel)
	if err := ensureDirAll(dir); err != nil {
		return errors.Wrapf(err, "failed to create dir '%s'", dir)
	}

	if err := ioutil.WriteFile(rel, data, 0644); err != nil {
		return errors.Wrapf(err, "failed to write file: %s", rel)
	}

	return nil
}

func (s *filesystemStore) Get(path Path) (rec Record, err error) {
	rel, err := s.rel(path)
	if err != nil {
		return rec, err
	}

	fd, err := os.Open(rel)
	if err != nil {
		// this will include the file does not exist error
		return rec, err
	}

	defer fd.Close()

	stat, err := fd.Stat()
	if err != nil {
		return rec, errors.Wrapf(err, "failed to stat file: %s", rel)
	}

	rec.Time = stat.ModTime()
	if stat.IsDir() {
		// we set prefix only because this path is pointing to a directory
		rec.Path = NewPath(path.Collection, path.Relative(), "")
		return rec, nil
	}

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		return rec, errors.Wrapf(err, "failed to read object data: %s", rel)
	}

	rec.Path = FromPath(path.Collection, path.Relative())
	rec.Data = data
	rec.Time = stat.ModTime()

	return
}

func (s *filesystemStore) Del(path Path) error {
	rel, err := s.rel(path)
	if err != nil {
		return err
	}

	err = os.RemoveAll(rel)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "failed to delete object: '%s'", err)
	}

	return nil
}

func (s *filesystemStore) Exists(path Path) (bool, error) {
	rel, err := s.rel(path)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(rel)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.Wrapf(err, "failed to get stat of: '%s'", rel)
	}

	return true, nil
}

func (s *filesystemStore) Link(link, target Path) error {
	relLink, err := s.rel(link)
	if err != nil {
		return err
	}

	if err := ensureDirAll(filepath.Dir(relLink)); err != nil {
		return err
	}

	relTarget, err := s.rel(target)
	if err != nil {
		return err
	}

	linkDir, _ := filepath.Split(relLink)
	relative, err := filepath.Rel(linkDir, relTarget)
	if err != nil {
		return err
	}
	if err := os.Symlink(relative, relLink); err != nil {
		return errors.Wrapf(err, "failed to link '%s' -> '%s'", relLink, relative)
	}

	return nil
}

func (s *filesystemStore) List(path Path) ([]Path, error) {
	rel, err := s.rel(path)
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(rel)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil && strings.Contains(err.Error(), "not a directory") {
		// list operation on a file
		return []Path{FromPath(path.Collection, path.Relative())}, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "failed to list directory: '%s'", rel)
	}

	paths := make([]Path, 0, len(files))
	prefix := path.Relative()
	for _, file := range files {
		if file.IsDir() {
			// add a path with prefix only
			paths = append(paths, NewPath(path.Collection, filepath.Join(prefix, file.Name()), ""))
		} else {
			paths = append(paths, NewPath(path.Collection, prefix, file.Name()))
		}
	}

	return paths, nil
}

func (s *filesystemStore) Scan(root Path, after string, limit int, mode ScanMode) ([]Path, error) {
	realRoot, err := s.rel(root)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(realRoot)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "failed to stat file: %s", err)
	}

	if !stat.IsDir() {
		return []Path{FromPath(root.Collection, root.Relative())}, nil
	}

	var paths []Path
	base := root.Relative() // this is the part after the collection.
	err = filepath.Walk(realRoot, func(path string, info os.FileInfo, err error) error {
		if realRoot == path {
			return nil
		}

		if err != nil {
			return errors.Wrapf(err, "failed to walk '%s'", realRoot)
		}

		if limit == 0 {
			return errMaxKeyReached
		}

		relative, err := filepath.Rel(realRoot, path)
		if err != nil {
			return err
		}

		if strings.Compare(relative, after) <= 0 {
			//scanning until reach the "after"
			return nil
		}

		limit--

		// we scan only one level so it's safe to use base as prefix
		if info.IsDir() {
			paths = append(paths, NewPath(root.Collection, filepath.Join(base, relative), ""))
			if mode == ScanModeDelimited {
				return filepath.SkipDir
			}

			return nil
		}

		paths = append(paths, FromPath(root.Collection, base, relative))
		return nil
	})

	return paths, err
}
