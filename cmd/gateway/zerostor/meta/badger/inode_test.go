package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/stretchr/testify/require"
)

const (
	testCollection = meta.Collection("test")
)

func getTestInodeStore(t *testing.T, name string) meta.Store {
	db := filepath.Join(os.TempDir(), name)
	os.RemoveAll(db)
	store, err := newBadgerInodeStore(db)

	require.NoError(t, err)

	return store
}

func TestInodeStoreCreateDirectory(t *testing.T) {
	store := getTestInodeStore(t, "inode_create_dir")
	defer store.Close()

	err := store.Set(meta.DirPath(testCollection, "documents"), nil)
	require.NoError(t, err)

	err = store.Set(meta.DirPath(testCollection, "videos", "movies"), nil)
	require.NoError(t, err)

	record, err := store.Get(meta.DirPath(testCollection, "documents"))
	require.NoError(t, err)

	require.True(t, record.Path.IsDir())
	require.Equal(t, "documents", record.Path.Relative())
}

func TestInodeStoreCreateFile(t *testing.T) {
	store := getTestInodeStore(t, "inode_create_file")
	defer store.Close()

	err := store.Set(meta.FilePath(testCollection, "test.txt"), []byte("hello world"))
	require.NoError(t, err)

	record, err := store.Get(meta.FilePath(testCollection, "test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "test.txt", record.Path.Relative())
}

func TestInodeStoreCreateMany(t *testing.T) {
	store := getTestInodeStore(t, "inode_create_many")
	defer store.Close()

	err := store.Set(meta.FilePath(testCollection, "documents/test.txt"), []byte("hello world"))
	require.NoError(t, err)

	err = store.Set(meta.FilePath(testCollection, "documents/data.txt"), []byte("some data"))
	require.NoError(t, err)

	err = store.Set(meta.DirPath(testCollection, "documents/important"), nil)
	require.NoError(t, err)

	record, err := store.Get(meta.FilePath(testCollection, "documents", "test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "documents/test.txt", record.Path.Relative())

	// list
	objects, err := store.List(meta.DirPath(testCollection, "documents"))
	require.NoError(t, err)

	require.Len(t, objects, 3)

	require.False(t, objects[0].IsDir())
	require.True(t, objects[1].IsDir())
	require.False(t, objects[2].IsDir())
	require.Equal(t, "documents/data.txt", objects[0].Relative())
	require.Equal(t, "documents/important", objects[1].Relative())
	require.Equal(t, "documents/test.txt", objects[2].Relative())

	// get

	record, err = store.Get(meta.FilePath(testCollection, "documents/test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "documents/test.txt", record.Path.Relative())

	record, err = store.Get(meta.FilePath(testCollection, "documents/important"))
	require.NoError(t, err)

	require.True(t, record.Path.IsDir())
	require.Equal(t, "documents/important", record.Path.Relative())
}

func TestInodeList(t *testing.T) {
	files := []string{
		"readme.txt",
		"docs/document-1.txt",
		"docs/document-2.txt",
		"docs/document-3.txt",
		"videos/video-1.mp4",
		"videos/video-2.mp4",
		"videos/video-3.mp4",
		"archive/2009/photos/photo-1.jpg",
		"archive/2009/photos/photo-2.jpg",
		"archive/2009/photos/photo-3.jpg",
		"archive/2009/photos/photo-4.jpg",
		"archive/2010/photos/photo-1.jpg",
		"archive/2010/photos/photo-2.jpg",
		"archive/2010/photos/photo-3.jpg",
		"archive/2010/photos/photo-4.jpg",
	}

	data := []byte("some random data!")

	store := getTestInodeStore(t, "inode_list")
	defer store.Close()

	for _, file := range files {
		path := meta.FilePath(testCollection, file)
		err := store.Set(path, data)
		require.NoError(t, err)
	}

	cases := []struct {
		List     meta.Path
		Expected []string
	}{
		{
			List:     meta.DirPath(testCollection),
			Expected: []string{"readme.txt", "docs", "videos", "archive"},
		},
		{
			List:     meta.DirPath(testCollection, "docs"),
			Expected: []string{"document-1.txt", "document-2.txt", "document-3.txt"},
		},
		{
			List:     meta.DirPath(testCollection, "archive"),
			Expected: []string{"2009", "2010"},
		},
		{
			List:     meta.DirPath(testCollection, "archive", "2009"),
			Expected: []string{"photos"},
		},
	}

	for _, c := range cases {
		t.Run(c.List.String(), func(t *testing.T) {
			paths, err := store.List(c.List)
			require.NoError(t, err)

			require.NoError(t, pathsMatch(paths, c.Expected...))
		})
	}

}

func pathsMatch(l []meta.Path, p ...string) error {
	if len(l) != len(p) {
		return fmt.Errorf("invalid count '%d' expecting '%d'", len(p), len(l))
	}

	sort.Strings(p)

	for _, x := range l {
		if sort.SearchStrings(p, x.Relative()) < 0 {
			return fmt.Errorf("path '%s' not expected", x.Relative())
		}
	}
	return nil
}
