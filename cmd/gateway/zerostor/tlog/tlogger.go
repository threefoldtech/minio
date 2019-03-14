package tlog

import (
	"context"
	"time"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	log "github.com/sirupsen/logrus"
)

//Operation defines a tlog record type
type Operation string

const (
	//OperationBucketCreate bucket create
	OperationBucketCreate = "bucket:create"
	//OperationBucketDelete bucket delete
	OperationBucketDelete = "bucket:delete"
	//OperationBucketSetPolicy bucket set-policy
	OperationBucketSetPolicy = "bucket:set-policy"

	//OperationPartPut  put part
	OperationPartPut = "part:put"
	//OperationPartLink  link part
	OperationPartLink = "part:link"

	//OperationBlobDelete delete blob
	OperationBlobDelete = "blob:delete"

	//OperationObjectDelete delete object
	OperationObjectDelete = "object:delete"
	//OperationObjectLink link object
	OperationObjectLink = "object:link"
	//OperationObjectPut put object
	OperationObjectPut = "object:put"
	//OperationObjectWriteMeta write object meta
	OperationObjectWriteMeta = "object:write-meta"
	//OperationObjectWriteStream write object meta stream
	OperationObjectWriteStream = "object:write-stream"

	//OperationUploadNew new upload
	OperationUploadNew = "upload:new"
	//OperationUploadDelete delete upload
	OperationUploadDelete = "upload:delete"
	//OperationUploadComplete complete upload
	OperationUploadComplete = "upload:complete"

	//OperationTest test operation
	OperationTest = "test"
)

/*
Recorder is a transaction log recoder. A recorder should be used as following

recorder.Begin()
defer recorder.End()

key, err := recorder.Record(rec)
//DO YOUR WORK HERE
if no error occurred
recorder.SetState(key)
*/
type Recorder interface {
	//Record creates a record
	Record(record Record) ([]byte, error)
	//Begin begins a record
	Begin()
	//End ends a record
	End()
	//SetState Mark that the current data state is at this record key
	//will allow syncing state later on
	SetState([]byte) error
}

//TLogger struct
type TLogger struct {
	recorder *zdbRecorder
	meta     meta.Manager
}

//InitializeMetaManager creates a new zdb tlogger
func InitializeMetaManager(address, namespace, password, stateFile string, metaManager meta.Manager) (*TLogger, error) {
	recorder, err := newZDBRecorder(address, namespace, password, stateFile)
	if err != nil {
		return nil, err
	}

	return &TLogger{
		recorder: recorder,
		meta:     metaManager,
	}, nil
}

//Sync syncs the backend storage with the latest records from the tlog storage
func (t *TLogger) Sync() error {

	return t.recorder.Play(nil, func(key []byte, rec Record) error {
		if err := rec.Play(t.meta); err != nil {
			logger := log.WithError(err).WithFields(log.Fields{
				"subsystem": "sync",
				"tlog":      t.recorder.p.address,
				"namespace": t.recorder.p.namespace,
				"action":    rec.Action(),
			})

			if _, ok := err.(Warning); ok {
				logger.Warning("failed to process tlog record")
			} else {
				logger.Error("failed to process tlog record")
			}
		}

		return t.recorder.SetState(key)
	})
}

//HealthChecker start health checker for TLogger
func (t *TLogger) HealthChecker(ctx context.Context) {
	for {
		select {
		case <-time.After(10 * time.Minute):
		case <-ctx.Done():
			return
		}

		if err := t.recorder.test(); err != nil {
			log.WithFields(log.Fields{
				"subsystem": "tlog",
				"tlog":      t.recorder.p.address,
				"namespace": t.recorder.p.namespace,
				"master":    false,
			}).WithError(err).Error("error while checking shard health")
		} else {
			log.WithFields(log.Fields{
				"subsystem": "tlog",
				"tlog":      t.recorder.p.address,
				"namespace": t.recorder.p.namespace,
				"master":    false,
			}).Error("tlog state is okay")
		}
	}
}
