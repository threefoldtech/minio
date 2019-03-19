package zerostor

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline"
	"github.com/threefoldtech/0-stor/client/datastor/zerodb"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

var (
	//ErrReadOnlyZeroStor raised if minio runs as a slave
	ErrReadOnlyZeroStor = fmt.Errorf("minio running in slave mode")
)

// zsClient defines 0-stor client wrapper
type zsClient struct {
	*client.Client
	mux *sync.RWMutex
}

func (zc *zsClient) Close() {
	zc.mux.RUnlock()
}

func (zc *zsClient) Write(bucket, object string, rd io.Reader, userDefMeta map[string]string) (*metatypes.Metadata, error) {
	key := zc.getKey(bucket, object)

	// convert the header key to canonical header key format
	// so we can use it easily when getting the object info
	userDef := make(map[string]string, len(userDefMeta))
	for k, v := range userDefMeta {
		userDef[strings.ToLower(k)] = v
	}

	return zc.Client.WriteWithUserMeta(key, rd, userDef)
}

func (zc *zsClient) Read(metadata *metatypes.Metadata, writer io.Writer, offset, length int64) error {
	if offset == 0 && (length <= 0 || length >= metadata.Size) {
		return zc.Client.Read(*metadata, writer)
	}
	return zc.Client.ReadRange(*metadata, writer, offset, length)
}

// getKey generates 0-stor key from the given bucket/object
func (zc *zsClient) getKey(bucket, object string) []byte {
	return []byte(filepath.Join(bucket, object))
}

type zsClientManager struct {
	zstorClient *zsClient
	mux         sync.RWMutex
	Cluster     datastor.Cluster
}

func (zm *zsClientManager) Get() *zsClient {
	zm.mux.RLock()
	return zm.zstorClient
}

func (zm *zsClientManager) Open(cfg config.Config) error {
	zm.mux.Lock()
	defer zm.mux.Unlock()
	zm.zstorClient.Client.Close()
	client, _, err := createClient(cfg)
	if err != nil {
		return nil
	}
	zm.zstorClient.Client = client
	return nil
}

func (zm *zsClientManager) Close() error {
	zm.mux.Lock()
	defer zm.mux.Unlock()

	return zm.zstorClient.Client.Close()
}

// createClient creates a 0-stor client from a configuration file
func createClient(cfg config.Config) (*client.Client, datastor.Cluster, error) {
	if cfg.Namespace == "" {
		return nil, nil, fmt.Errorf("empty namespace")
	}

	cluster, err := zerodb.NewCluster(cfg.DataStor.Shards, cfg.Password, cfg.Namespace, nil)
	if err != nil {
		return nil, nil, err
	}

	// create data pipeline, using our datastor cluster
	dataPipeline, err := pipeline.NewPipeline(cfg.DataStor.Pipeline, cluster, 0)
	if err != nil {
		return nil, nil, err
	}

	return client.NewClient(nil, dataPipeline), cluster, nil
}
