package meta_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/meta/badger"
	"github.com/stretchr/testify/require"
)

func getTestStore(t *testing.T, name string) meta.Store {
	db := filepath.Join(os.TempDir(), name)
	os.RemoveAll(db)
	store, err := badger.NewBadgerStore(db)

	require.NoError(t, err)

	return store
}

func TestManagerList(t *testing.T) {
	store := getTestStore(t, "manager-list")
	mgr := meta.NewMetaManager(store, "")
	defer mgr.Close()

	require := require.New(t)

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

	const bucket = "bucket"
	for _, file := range files {
		_, err := mgr.ObjectEnsure(bucket, file)
		require.NoError(err)
	}
	ctx := context.Background()

	cases := []struct {
		Prefix  string
		After   string
		Results []string
	}{
		{
			Results: []string{"readme.txt", "docs", "videos", "archive"},
		},
		{
			Prefix:  "archive/",
			Results: []string{"archive/2009", "archive/2010"},
		},
		{
			Prefix:  "videos/",
			After:   "videos/videos-3.mp4",
			Results: []string{},
		},
		{
			Prefix: "archive/2009/photos/",
			After:  "archive/2009/photos/photo-1.jpg",
			Results: []string{
				"archive/2009/photos/photo-2.jpg",
				"archive/2009/photos/photo-3.jpg",
				"archive/2009/photos/photo-4.jpg",
			},
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("prefix: %s (after: %s)", c.Prefix, c.After), func(t *testing.T) {

			input, err := mgr.ObjectList(ctx, bucket, c.Prefix, c.After)
			require.NoError(err)

			names := []string{}
			for result := range input {
				require.NoError(result.Error)

				names = append(names, result.Info.Name)
			}

			require.Len(names, len(c.Results))

			sort.Strings(names)
			sort.Strings(c.Results)
			require.EqualValues(names, c.Results)
		})
	}

}
