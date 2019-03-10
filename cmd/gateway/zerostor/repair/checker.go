package repair

import (
	"bytes"
	"context"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	log "github.com/sirupsen/logrus"

	// "github.com/minio/minio/cmd/gateway/zerostor/tlog"
	"github.com/pkg/errors"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline/storage"
	"github.com/threefoldtech/0-stor/client/metastor/encoding"
)

type repairResult int

const (
	_ = iota
	repairResultNotNeeded
	repairResultSucceed
	repairResultFailed
)

type checker struct {
	namespace []byte
	client    *client.Client
	marshaler *encoding.MarshalFuncPair
	// tlog      *tlog.TLogger
	meta meta.Manager
}

type repairStats struct {
	total    int64
	repaired int64
	failed   int64
}

func (c *checker) checkAndRepair(metaDir string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stats repairStats
	var result repairResult
	var err error

	blobs := c.meta.StreamBlobs(ctx)
	for blob := range blobs {
		if blob.Error != nil {
			result, err = repairResultFailed, errors.Wrap(blob.Error, "load meta failed")
			// if err != nil {

			// 	log.WithFields(log.Fields{"file": wp.Path(), "key": wp.Key()}).WithError(err)
			// }
		} else {
			filename := c.meta.BlobFile(blob.Obj.Filename)
			log.WithFields(log.Fields{"file": filename}).Infof("checking file")
			result, err = c.checkAndRepairMeta(blob.Obj)
			if err != nil {
				log.WithFields(log.Fields{"file": filename, "key": blob.Obj.Key}).WithError(err).Warn("failed to fix file")
			}
		}

		stats.total++
		if result == repairResultFailed {
			stats.failed++
		} else if result == repairResultSucceed {
			stats.repaired++
		}
	}

	log.WithField("dir", metaDir).Infoln("check and repair complete")
	log.Infof("Total processed %d", stats.total)
	log.Infof("Repaired %d", stats.repaired)
	log.Infof("Failed %d", stats.failed)
	return nil
}

func (c *checker) checkAndRepairMeta(objMeta meta.ObjectMeta) (repairResult, error) {

	status, err := c.client.Check(*objMeta.Metadata, false)
	if err != nil {
		return repairResultFailed, errors.Wrap(err, "client repair: error during file check")
	}

	log.WithFields(log.Fields{"key": string(objMeta.Key), "status": status.String()}).Infoln("file status")
	if status == storage.CheckStatusOptimal {
		buf := &bytes.Buffer{}
		err = c.client.Read(*objMeta.Metadata, buf)
		if err == nil {
			//nothing to do here
			return repairResultNotNeeded, nil
		}
	}

	log.WithFields(log.Fields{"key": string(objMeta.Key)}).Debug("start file repair")
	//otherwise we do a repair
	fixed, err := c.client.Repair(*objMeta.Metadata)
	if err != nil {
		return repairResultFailed, errors.Wrap(err, "client repair")
	}
	objMeta.Metadata = fixed

	if err := c.meta.EncodeObjMeta(c.meta.BlobFile(objMeta.Filename), &objMeta); err != nil {
		return repairResultFailed, err
	}

	// if c.tlog != nil {
	// 	c.tlog.Storage().Set(
	// 		c.namespace,
	// 		[]byte(file.Key()),
	// 		bytes,
	// 	)
	// }

	return repairResultSucceed, nil
}

// func (c *checker) loadMeta(path string) (*metatypes.Metadata, error) {
// 	bytes, err := ioutil.ReadFile(path)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var m metatypes.Metadata
// 	return &m, c.marshaler.Unmarshal(bytes, &m)
// }
