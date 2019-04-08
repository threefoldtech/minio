package zerostor

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/tlog"

	"github.com/minio/minio/cmd/gateway/zerostor/config"
	log "github.com/sirupsen/logrus"
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

type metaManager struct {
	meta.Manager
	mux *sync.RWMutex
}

func (m *metaManager) Close() {
	m.mux.RUnlock()
}

// zsClient defines 0-stor client wrapper
type zsClient struct {
	*client.Client
	mux     *sync.RWMutex
	cluster datastor.Cluster
}

func (zc *zsClient) healthReporter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			//in case context was canceled when we were waiting
			return
		case <-time.After(10 * time.Minute):
		}
		log.Debug("checking cluster health")

		for iter := zc.cluster.GetShardIterator(nil); iter.Next(); {
			shard := iter.Shard()
			err := shardHealth(shard)
			if err != nil {
				log.WithFields(log.Fields{
					"shard": shard.Identifier(),
				}).WithError(err).Error("error while checking shard health")
			} else {
				log.WithFields(log.Fields{
					"shard": shard.Identifier(),
				}).Error("shard state is okay")
			}
		}
	}
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

type configManager struct {
	zstorClient *zsClient
	mux         sync.RWMutex
	metaManager metaManager
	cancel      func()
}

func (c *configManager) GetClient() Client {
	c.mux.RLock()
	return c.zstorClient
}

func (c *configManager) GetMeta() metaManager {
	c.mux.RLock()
	return c.metaManager
}

func (c *configManager) Reload(cfg config.Config, metaDir, metaPrivKey string) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.cancel != nil {
		c.zstorClient.Client.Close()
		c.cancel()
	}

	client, cluster, err := createClient(cfg)
	if err != nil {
		return nil
	}
	zsClient := zsClient{
		client,
		&c.mux,
		cluster,
	}
	c.zstorClient = &zsClient
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	meta, err := createMetaManager(ctx, cfg, metaDir, metaPrivKey)
	if err != nil {
		log.Println("failed to create meta manager: ", err.Error())
		return err
	}
	c.metaManager = metaManager{meta, &c.mux}
	go c.zstorClient.healthReporter(ctx)

	return nil
}

func (c *configManager) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.cancel()
	return c.zstorClient.Client.Close()
}

// ConfigManager implements a 0-stor client manager
type ConfigManager interface {
	GetClient() Client
	GetMeta() metaManager
	Reload(cfg config.Config, metaDir, metaPrivKey string) error
	Close() error
}

// Client implements a zerotstor client
type Client interface {
	Close()
	Write(bucket, object string, rd io.Reader, userDefMeta map[string]string) (*metatypes.Metadata, error)
	Read(metadata *metatypes.Metadata, writer io.Writer, offset, length int64) error
	Delete(meta metatypes.Metadata) error
	getKey(bucket, object string) []byte
}

func newConfigManager(cfg config.Config, metaDir, metaPrivKey string) (ConfigManager, error) {
	zsManager := configManager{}
	zsManager.Reload(cfg, metaDir, metaPrivKey)

	return &zsManager, nil
}

// createClient creates a 0-stor client from a configuration file
func createClient(cfg config.Config) (*client.Client, datastor.Cluster, error) {
	if cfg.Namespace == "" {
		return nil, nil, fmt.Errorf("empty namespace")
	}

	cluster, err := zerodb.NewCluster(cfg.DataStor.Shards, cfg.Password, cfg.Namespace, nil, datastor.DefaultSpreadingType)
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

func createMetaManager(ctx context.Context, cfg config.Config, metaDir, metaPrivKey string) (meta.Manager, error) {
	var zoMetaManager meta.Manager
	metaManager, err := meta.InitializeMetaManager(metaDir, metaPrivKey)
	if err != nil {
		log.Println("failed to create meta manager: ", err.Error())
		return nil, err
	}
	zoMetaManager = metaManager

	if cfg.Minio.TLog != nil {
		tlogCfg := cfg.Minio.TLog
		tlogMetaManager, err := tlog.InitializeMetaManager(tlogCfg.Address, tlogCfg.Namespace, tlogCfg.Password, path.Join(metaDir, tlog.StateDir), metaManager)
		if err != nil {
			log.Println("failed to create tlog meta manager: ", err.Error())
			return nil, err
		}

		err = tlogMetaManager.Sync()
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"tlog":      tlogCfg.Address,
				"namespace": tlogCfg.Namespace,
			}).Error("failed to synchronize transaction logger with local meta")
			return nil, err
		}

		go tlogMetaManager.HealthChecker(ctx) //start tlog health checker
		zoMetaManager = tlogMetaManager

	}

	if cfg.Minio.Master != nil { //check if master is configure
		//start synchronizing with master zdb
		masterCfg := cfg.Minio.Master
		syncher := tlog.NewSyncher(masterCfg.Address, masterCfg.Namespace, masterCfg.Password, path.Join(metaDir, "master.state"), metaManager)
		go func() {
			for {
				if err := syncher.Sync(ctx); err != nil {
					log.WithError(err).WithFields(log.Fields{
						"subsystem": "sync",
						"tlog":      masterCfg.Address,
						"namespace": masterCfg.Namespace,
						"master":    true,
					}).Error("failed to do master synching")
					<-time.After(3 * time.Second)
					continue
				}

				//zerostor shutting down. (cancel function called)
				break
			}
		}()
	}

	return zoMetaManager, nil
}

func shardHealth(shard datastor.Shard) error {
	key, err := shard.CreateObject([]byte("test write"))
	if err != nil {
		return err
	}

	return shard.DeleteObject(key)
}
