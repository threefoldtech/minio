// Repair package provide the utility to validate all objects
// The repair utility will also fix objects if they are not optimal and
// then update the local metadata.

// On updating the metadata, the repair utility must make sure tlog records are
// created for the new meta objects so slave servers get the new `fixed` meta

// Repair should do nothing on a slave setup

package repair

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/pkg/errors"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/metastor/encoding"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
	"github.com/threefoldtech/0-stor/client/processing"
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

	if len(cfg.Minio.ZerostorMeta.Type) != 0 && cfg.Minio.ZerostorMeta.Type != config.MetaTypeFile {
		return fmt.Errorf("support only file metastore")
	}

	return nil
}

// func getTLogger(cfg *config.TLog) (*tlog.TLogger, error) {
// 	return tlog.NewZDBTLogger(
// 		cfg.Address, cfg.Namespace, cfg.Password,
// 		TmpStateFile, nil, nil, multipart.MultipartBucket,
// 	)
// }

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

	marshaler, err := getMarshalFuncPair(privKey)
	if err != nil {
		return errors.Wrap(err, "getting marshalar pair failed")
	}

	client, err := client.NewClientFromConfig(cfg.Config, nil, 0)
	//defer client.Close()
	if err != nil {
		return errors.Wrap(err, "client creation failed")
	}

	// var logger *tlog.TLogger

	// if cfg.Minio.TLog != nil {
	// 	var err error
	// 	logger, err = getTLogger(cfg.Minio.TLog)
	// 	if err != nil {
	// 		return errors.Wrap(err, "getting tlogger failed")
	// 	}
	// }

	metaManager, err := meta.InitializeMetaManager(metaDir)

	c := checker{
		namespace: []byte(cfg.Namespace),
		client:    client,
		marshaler: marshaler,
		meta:      metaManager,
		// tlog:      logger,
	}

	return c.checkAndRepair(metaDir)
}

func getMarshalFuncPair(privKey string) (*encoding.MarshalFuncPair, error) {
	pair, err := encoding.NewMarshalFuncPair(encoding.DefaultMarshalType)
	if err != nil {
		return nil, err
	}

	if len(privKey) == 0 {
		return pair, nil
	}

	encode := func(md metatypes.Metadata) ([]byte, error) {
		processor, err := processing.NewEncrypterDecrypter(
			processing.DefaultEncryptionType, []byte(privKey),
		)
		if err != nil {
			return nil, err
		}

		bytes, err := pair.Marshal(md)
		if err != nil {
			return nil, err
		}
		return processor.WriteProcess(bytes)
	}

	decode := func(bytes []byte, md *metatypes.Metadata) error {
		processor, err := processing.NewEncrypterDecrypter(
			processing.DefaultEncryptionType, []byte(privKey),
		)
		if err != nil {
			return err
		}
		bytes, err = processor.ReadProcess(bytes)
		if err != nil {
			return err
		}
		return pair.Unmarshal(bytes, md)
	}

	return &encoding.MarshalFuncPair{Marshal: encode, Unmarshal: decode}, nil
}
