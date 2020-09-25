package badger

import (
	"os"
	"path/filepath"
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

func TestInodStoreCreateDirectory(t *testing.T) {
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

func TestInodStoreCreateFile(t *testing.T) {
	store := getTestInodeStore(t, "inode_create_file")
	defer store.Close()

	err := store.Set(meta.FilePath(testCollection, "test.txt"), []byte("hello world"))
	require.NoError(t, err)

	record, err := store.Get(meta.FilePath(testCollection, "test.txt"))
	require.NoError(t, err)

	require.False(t, record.Path.IsDir())
	require.Equal(t, "test.txt", record.Path.Relative())
}

func TestInodStoreCreateMany(t *testing.T) {
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

}
