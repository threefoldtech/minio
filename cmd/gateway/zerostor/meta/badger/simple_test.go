package badger

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/stretchr/testify/require"
)

func getTestSimpleeStore(t *testing.T, name string) meta.Store {
	db := filepath.Join(os.TempDir(), name)
	os.RemoveAll(db)
	store, err := newBadgerSimpleStore(db)

	require.NoError(t, err)

	return store
}

func TestSimpleStoreCreateDirectory(t *testing.T) {
	store := getTestSimpleeStore(t, "simple_create_dir")
	defer store.Close()

	err := store.Set(meta.DirPath(testCollection, "documents"), nil)
	require.Errorf(t, err, "simple store does not support directory entries")
}

func TestSimpleStoreCreateFile(t *testing.T) {
	store := getTestSimpleeStore(t, "simple_create_file")
	defer store.Close()

	err := store.Set(meta.FilePath(testCollection, "test.txt"), []byte("hello world"))
	require.NoError(t, err)

	record, err := store.Get(meta.FilePath(testCollection, "test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "test.txt", record.Path.Relative())
}

func TestSimpleStoreCreateMany(t *testing.T) {
	store := getTestSimpleeStore(t, "simple_create_many")
	defer store.Close()

	err := store.Set(meta.FilePath(testCollection, "documents/test.txt"), []byte("hello world"))
	require.NoError(t, err)

	err = store.Set(meta.FilePath(testCollection, "documents/data.txt"), []byte("some data"))
	require.NoError(t, err)

	record, err := store.Get(meta.FilePath(testCollection, "documents", "test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "documents/test.txt", record.Path.Relative())

	record, err = store.Get(meta.FilePath(testCollection, "documents"))
	require.Error(t, err, os.ErrNotExist)

	// list
	objects, err := store.List(meta.DirPath(testCollection, "documents"))
	require.NoError(t, err)

	require.Len(t, objects, 2)

	require.False(t, objects[0].IsDir())
	require.False(t, objects[1].IsDir())
	require.Equal(t, "documents/data.txt", objects[0].Relative())
	require.Equal(t, "documents/test.txt", objects[1].Relative())

	// get

	record, err = store.Get(meta.FilePath(testCollection, "documents/test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "documents/test.txt", record.Path.Relative())
}

func TestSimpleList(t *testing.T) {
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

	store := getTestSimpleeStore(t, "inode_list")
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
			Expected: files,
		},
		{
			List:     meta.DirPath(testCollection, "docs"),
			Expected: files[1:4],
		},
		{
			List:     meta.DirPath(testCollection, "archive"),
			Expected: files[7:],
		},
		{
			List:     meta.DirPath(testCollection, "archive", "2009"),
			Expected: files[7:11],
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
