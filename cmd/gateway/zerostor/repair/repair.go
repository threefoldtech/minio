// Repair package provide the utility to validate all objects
// The repair utility will also fix objects if they are not optimal and
// then update the local metadata.

// On updating the metadata, the repair utility must make sure tlog records are
// created for the new meta objects so slave servers get the new `fixed` meta

// Repair should do nothing on a slave setup

package repair

import (
	"path"

	log "github.com/sirupsen/logrus"

	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/tlog"
	"github.com/pkg/errors"
	"github.com/threefoldtech/0-stor/client"
)

const (
	//TmpStateFile where the repair tlog state is kept
	TmpStateFile = "/tmp/state.tmp"
)

func valid(cfg *config.Config) error {
	if cfg.Minio.Master != nil {
		//running as slave
		log.Info("running in slave mode, nothing to do here")
		return nil
	}

	return nil
}

// CheckAndRepair is entry point for the repair tool
func CheckAndRepair(confFile, metaDir, privKey string) error {
	// - Create a 0-stor client based on given config
	// - Iterate over the metadata directory
	cfg, err := config.Load(confFile)
	if err != nil {
		return errors.Wrap(err, "loading config failed")
	}

	log.WithField("config", cfg).Info("config")
	if err = valid(&cfg); err != nil {
		return errors.Wrap(err, "config validatin failed")
	}

	if err != nil {
		return errors.Wrap(err, "getting marshalar pair failed")
	}

	client, err := client.NewClientFromConfig(cfg.Config, nil, 0)
	if err != nil {
		return errors.Wrap(err, "client creation failed")
	}
	defer client.Close()

	metaManager, err := meta.InitializeMetaManager(metaDir, privKey)
	if err != nil {
		log.Println("failed to create meta manager: ", err.Error())
		return err
	}

	if cfg.Minio.TLog != nil {
		metaManager, err = tlog.InitializeMetaManager(cfg.Minio.TLog.Address, cfg.Minio.TLog.Namespace, cfg.Minio.TLog.Password, path.Join(metaDir, tlog.StateDir), metaManager)
		if err != nil {
			log.Println("failed to create tlog meta manager: ", err.Error())
			return err
		}
	}

	c := checker{
		namespace: []byte(cfg.Namespace),
		client:    client,
		meta:      metaManager,
	}

	return c.checkAndRepair(metaDir)
}
