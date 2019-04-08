package zerostor

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline"
	zdbtest "github.com/threefoldtech/0-stor/client/datastor/zerodb/test"
)

func newTestZsManager(t *testing.T, namespace string) (ConfigManager, client.Config, func()) {
	metaDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg, zStorCleanup := newTestZstorClient(t, namespace)

	// creates 0-stor  wrapper
	cfgManager, err := newConfigManager(config.Config{Config: cfg}, metaDir, "")
	if err != nil {
		t.Fatalf("failed to create meta config manager: %v", err)
	}

	cleanups := func() {
		os.RemoveAll(metaDir)
		zStorCleanup()
		cfgManager.Close()
	}
	return cfgManager, cfg, cleanups
}

func newTestZstorClient(t *testing.T, namespace string) (client.Config, func()) {
	// creates in-memory 0-db server
	shards, serverClean := testZdbServer(t, 4)

	// creates 0-stor client config
	cfg := client.Config{
		Namespace: namespace,
		DataStor:  client.DataStorConfig{Shards: shards, Pipeline: pipeline.Config{BlockSize: 1024, Distribution: pipeline.ObjectDistributionConfig{DataShardCount: 1}}},
	}
	return cfg, func() {
		serverClean()
	}
}

func testZdbServer(t *testing.T, n int) (shards []string, cleanups func()) {
	var (
		namespace    = "ns"
		cleanupFuncs []func()
	)

	for i := 0; i < n; i++ {
		_, addr, cleanup, err := zdbtest.NewInMem0DBServer(namespace)
		if err != nil {
			t.Fatalf("failed to create zdb server:%v", err)
		}

		cleanupFuncs = append(cleanupFuncs, cleanup)
		shards = append(shards, addr)
	}

	cleanups = func() {
		for _, cleanup := range cleanupFuncs {
			cleanup()
		}
	}
	return
}
