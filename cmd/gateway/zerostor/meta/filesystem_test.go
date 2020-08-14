// +build linux,filesystem

package meta

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio/minio/cmd"
	"github.com/stretchr/testify/require"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

func TestScan(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "scan-*")
	require := require.New(t)
	require.NoError(err)

	defer os.RemoveAll(dir)
	files := []string{
		"documents/important/accounts.txt",
		"documents/important/passwords.txt",
		"documents/dairies/1982-2090.pdf",
		"media/movies/titanic/titanic.mp4",
		"media/movies/the god father.mp4",
		"media/series/friends/s1.mp4",
		"media/series/friends/s2.mp4",
		"media/music/sound of silence.mp3",
	}

	// store, err := NewFilesystemStore(dir)
	// require.NoError(err)

	store, err := NewBadgerStore(dir)
	require.NoError(err)

	manager := NewMetaManager(store, "")
	mgr := manager.(*metaManager)
	bucket := "bucket"

	for _, file := range files {

		_, err = mgr.PutObject(&metatypes.Metadata{}, bucket, file)
		require.NoError(err)
	}

	ctx := context.Background()
	results := cmd.ListObjectsV2Info{}

	// //TODO: we need to re-support flat scan
	// _, err = mgr.scan(ctx, "bucket", "", "", false, 500, &results)
	// require.NoError(err)

	// require.Len(results.Objects, 17) // files plus directories

	results = cmd.ListObjectsV2Info{}
	_, err = mgr.scan(ctx, "bucket", "", "", true, 500, &results)
	require.NoError(err)

	require.Len(results.Objects, 0)
	require.Len(results.Prefixes, 2)

	results = cmd.ListObjectsV2Info{}
	_, err = mgr.scan(ctx, "bucket", "/media/movies/", "", true, 500, &results)
	require.NoError(err)

	require.Len(results.Objects, 1)
	require.Len(results.Prefixes, 1)

	results = cmd.ListObjectsV2Info{}
	_, err = mgr.scan(ctx, "bucket", "/Veeam/Archive/", "", true, 500, &results)
	require.NoError(err)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(results)

	require.Len(results.Objects, 0)
	require.Len(results.Prefixes, 0)

}
