package zerostor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/meta/badger"
	"github.com/minio/minio/cmd/gateway/zerostor/tlog"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

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

type metaManagerWrapper struct {
	meta.Manager
	release func()
}

// Close release this meta manager instance
func (m *metaManagerWrapper) Close() error {
	m.release()
	return nil
}

type metricType string

const (
	metricDataIOErrors  metricType = "data_io_errors"
	metricIndexIOErrors metricType = "index_io_errors"
	metricDataFaults    metricType = "data_faults"
	metricIndexFaults   metricType = "index_faults"
	metricConnError     metricType = "connection_errors"
)

var (
	gauges = make(map[string]metricsSet)
)

type metricsSet struct {
	metrics map[metricType]prometheus.Counter
	history map[metricType]float64

	// Size and free are defined alone because
	// they are gauges not counters
	Size prometheus.Gauge
	Free prometheus.Gauge
}

func newMetricsSet(address string, ns string) metricsSet {
	set := []metricType{
		metricDataIOErrors,
		metricDataFaults,
		metricIndexIOErrors,
		metricIndexFaults,
		metricConnError,
	}
	metrics := make(map[metricType]prometheus.Counter)
	history := make(map[metricType]float64)
	opts := prometheus.CounterOpts{
		Namespace: "minio",
		Subsystem: "zerostor",
		ConstLabels: prometheus.Labels{
			"address":   address,
			"namespace": ns,
		},
	}

	for _, typ := range set {
		mOpts := opts
		mOpts.Name = string(typ)
		metrics[typ] = promauto.NewCounter(mOpts)
		history[typ] = 0
	}
	sizeOpts := prometheus.GaugeOpts(opts)
	sizeOpts.Name = "data_size"
	freeOpts := prometheus.GaugeOpts(opts)
	freeOpts.Name = "data_free_space"

	return metricsSet{
		metrics: metrics,
		history: history,
		Size:    promauto.NewGauge(sizeOpts),
		Free:    promauto.NewGauge(freeOpts),
	}
}

// Set takes a total count, then the metric set makes sure it
// only add the difference from last reported value to the counter
// because we only receive full count from namespace info
func (m *metricsSet) Set(metric metricType, value float64) {
	old := m.history[metric]
	if value > old {
		m.metrics[metric].Add(value - old)
		m.history[metric] = value
	}
}

func (m *metricsSet) Add(metric metricType, value float64) {
	m.metrics[metric].Add(value)
	m.history[metric] += value
}

// storeClientWrapper defines 0-stor client wrapper
type storClientWrapper struct {
	*client.Client
	release func()
	cluster datastor.Cluster
}

func (zc *storClientWrapper) Inner() *client.Client {
	return zc.Client
}

func (zc *storClientWrapper) healthReporter(ctx context.Context) {
	for iter := zc.cluster.GetShardIterator(nil); iter.Next(); {
		shard := iter.Shard()

		if _, ok := gauges[shard.Identifier()]; ok {
			continue
		}

		gauges[shard.Identifier()] = newMetricsSet(shard.Address(), shard.Namespace())
	}

	for {

		log.Debug("checking cluster health")

		for iter := zc.cluster.GetShardIterator(nil); iter.Next(); {
			shard := iter.Shard()
			gauge := gauges[shard.Identifier()]
			ns, err := shard.GetNamespace()
			if err != nil {
				gauge.Add(metricConnError, 1)
				log.WithFields(log.Fields{
					"shard": shard.Identifier(),
				}).WithError(err).Error("error while checking shard health")
				continue
			}

			gauge.Size.Set(float64(ns.Used))
			gauge.Free.Set(float64(ns.Free))

			if ns.Health == nil {
				// health is not supported by this shard
				// type
				continue
			}

			health := ns.Health
			gauge.Set(metricDataFaults, float64(health.DataFaults))
			gauge.Set(metricDataIOErrors, float64(health.DataIOErrors))
			gauge.Set(metricIndexFaults, float64(health.IndexFaults))
			gauge.Set(metricIndexIOErrors, float64(health.IndexIOErrors))
		}

		select {
		case <-ctx.Done():
			//in case context was canceled when we were waiting
			return
		case <-time.After(1 * time.Minute):
		}
	}
}

func (zc *storClientWrapper) Close() {
	zc.release()
}

func (zc *storClientWrapper) Write(rd io.Reader, userDefMeta map[string]string) (*metatypes.Metadata, error) {
	key := []byte(uuid.New().String())
	// convert the header key to canonical header key format
	// so we can use it easily when getting the object info
	userDef := make(map[string]string, len(userDefMeta))
	for k, v := range userDefMeta {
		userDef[strings.ToLower(k)] = v
	}

	return zc.Client.WriteWithUserMeta(key, rd, userDef)
}

func (zc *storClientWrapper) Read(metadata *metatypes.Metadata, writer io.Writer, offset, length int64) error {
	if offset == 0 && (length <= 0 || length >= metadata.Size) {
		return zc.Client.Read(*metadata, writer)
	}
	return zc.Client.ReadRange(*metadata, writer, offset, length)
}

// getKey generates 0-stor key from the given bucket/object
func (zc *storClientWrapper) getKey(bucket, object string) []byte {
	return []byte(filepath.Join(bucket, object))
}

type configManager struct {
	mux sync.RWMutex

	metaDir     string
	metaPrivKey string

	metaManager *metaManagerWrapper
	client      *storClientWrapper

	cancel func()
}

func (c *configManager) borrow() {
	c.mux.RLock()
}

func (c *configManager) release() {
	c.mux.RUnlock()
}

func (c *configManager) GetClient() Client {
	c.borrow()
	return c.client
}

func (c *configManager) GetMeta() meta.Manager {
	c.borrow()
	return c.metaManager
}

func (c *configManager) Reload(cfg config.Config) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.close(); err != nil {
		return err
	}

	client, cluster, err := createClient(cfg)
	if err != nil {
		return err
	}

	wrapper := &storClientWrapper{
		Client:  client,
		release: c.release,
		cluster: cluster,
	}

	c.client = wrapper

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	meta, err := NewMetaManager(ctx, cfg, c.metaDir, c.metaPrivKey)
	if err != nil {
		log.Println("failed to create meta manager: ", err.Error())
		return err
	}

	c.metaManager = &metaManagerWrapper{
		Manager: meta,
		release: c.release,
	}

	go c.client.healthReporter(ctx)

	return nil
}

func (c *configManager) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.close()
}

func (c *configManager) close() error {

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}

	if c.client != nil {
		if err := c.client.Client.Close(); err != nil {
			return err
		}
		c.client = nil
	}

	if c.metaManager != nil {
		if err := c.metaManager.Manager.Close(); err != nil {
			return err
		}

		c.metaManager = nil
	}

	return nil
}

// ConfigManager implements a 0-stor client manager
type ConfigManager interface {
	GetClient() Client
	GetMeta() meta.Manager
	Reload(cfg config.Config) error
	Close() error
}

// Client implements a zerotstor client
type Client interface {
	Close()
	Write(rd io.Reader, userDefMeta map[string]string) (*metatypes.Metadata, error)
	Read(metadata *metatypes.Metadata, writer io.Writer, offset, length int64) error
	Delete(meta metatypes.Metadata) error
	Inner() *client.Client
}

//NewConfigManager creates a new configuration manager from config
func NewConfigManager(cfg config.Config, metaDir, metaPrivKey string) (ConfigManager, error) {
	zsManager := configManager{
		metaDir:     metaDir,
		metaPrivKey: metaPrivKey,
	}

	err := zsManager.Reload(cfg)

	return &zsManager, err
}

// createClient creates a 0-stor client from a configuration file
func createClient(cfg config.Config) (*client.Client, datastor.Cluster, error) {
	cluster, err := zerodb.NewCluster(cfg.DataStor.Shards, cfg.Password, cfg.Namespace, nil, datastor.DefaultSpreadingType)
	if err != nil {
		return nil, nil, err
	}

	jobs := cfg.Jobs
	if jobs == 0 {
		jobs = runtime.NumCPU()
	}

	// create data pipeline, using our datastor cluster
	dataPipeline, err := pipeline.NewPipeline(cfg.DataStor.Pipeline, cluster, jobs)
	if err != nil {
		return nil, nil, err
	}

	return client.NewClient(nil, dataPipeline), cluster, nil
}

func isFileExists(p string) bool {
	stat, err := os.Stat(p)
	if err != nil {
		return false
	}

	return !stat.IsDir()
}

// NewMetaManager creates a new meta config manager from configuration
func NewMetaManager(ctx context.Context, cfg config.Config, metaDir, metaPrivKey string) (meta.Manager, error) {

	//store, err := meta.NewFilesystemStore(metaDir)
	if err := os.MkdirAll(metaDir, 0766); err != nil && !os.IsExist(err) {
		return nil, errors.Wrapf(err, "failed to create meta directory")
	}

	store, err := badger.NewBadgerStore(metaDir)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create badger meta store")
	}

	tlogStateFile := path.Join(metaDir, tlog.StateFile)
	masterStateFile := path.Join(metaDir, tlog.MasterStateFile)

	if cfg.Minio.TLog != nil {
		// Here we run as master.
		// in case of promotion, we need to make sure we use the last state file used by the slave
		// so we don't resync everything from the tlog.
		if isFileExists(masterStateFile) {
			os.Rename(masterStateFile, tlogStateFile)
		}

		tlogCfg := cfg.Minio.TLog
		tlogStore, err := tlog.NewTLogger(tlogCfg.Address, tlogCfg.Namespace, tlogCfg.Password, tlogStateFile, store)
		if err != nil {
			log.Println("failed to create tlog meta manager: ", err.Error())
			return nil, err
		}

		err = tlogStore.Sync()
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"tlog":      tlogCfg.Address,
				"namespace": tlogCfg.Namespace,
			}).Error("failed to synchronize transaction logger with local meta")
			return nil, err
		}

		go tlogStore.HealthChecker(ctx) //start tlog health checker
		store = tlogStore

	}

	if cfg.Minio.Master != nil {
		// Here we run as a "slave" since a master is configured
		// in case of demotion, we need to make sure we use the last state file used by the master
		// so we don't resync everything from the tlog.
		if isFileExists(tlogStateFile) {
			os.Rename(tlogStateFile, masterStateFile)
		}

		masterCfg := cfg.Minio.Master
		syncher := tlog.NewSyncher(masterCfg.Address, masterCfg.Namespace, masterCfg.Password, masterStateFile, store)
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
				log.Debug("synchronization terminated")
				break
			}
		}()
	}

	return meta.NewMetaManager(store, metaPrivKey), nil
}
