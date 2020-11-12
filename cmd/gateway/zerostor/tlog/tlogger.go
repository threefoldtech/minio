package tlog

import (
	"context"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
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

	//OperationObjectMkdir mkdir
	OperationObjectMkdir = "object:mkdir"

	//OperationObjectWriteMeta write object meta
	OperationObjectWriteMeta = "object:write-meta"

	//OperationUploadNew new upload
	OperationUploadNew = "upload:new"
	//OperationUploadDelete delete upload
	OperationUploadDelete = "upload:delete"
	//OperationUploadComplete complete upload
	OperationUploadComplete = "upload:complete"

	//OperationTest test operation
	OperationTest = "test"

	//StateFile Tlog state, records the last state generated by this instance
	// used when the instance is running as a "master"
	StateFile = "tlog.state"

	//MasterStateFile keep track of the last received state from the master tlog
	// used when the instance is running as a "slave"
	MasterStateFile = "master.state"
)

/*
Recorder is a transaction log recoder. A recorder should be used as following

recorder.Begin()
defer recorder.End()
//DO YOUR WORK HERE

err := recorder.Record(rec)
*/
type Recorder interface {
	//Record creates a record and sets the state key if the creation was successful
	Record(record Record, setState bool) ([]byte, error)
	//Begin begins a record
	Begin()
	//End ends a record
	End()

	//SetState sets state at key
	SetState(key []byte) error
}

//TLogger interface
type TLogger interface {
	meta.Manager
	Sync() error
	HealthChecker(ctx context.Context)
}

type fsTLogger struct {
	recorder *zdbRecorder
	meta.Manager
}

//InitializeMetaManager creates a new zdb tlogger
func InitializeMetaManager(address, namespace, password, stateFile string, metaManager meta.Manager) (TLogger, error) {
	recorder, err := newZDBRecorder(address, namespace, password, stateFile)
	if err != nil {
		return nil, err
	}

	return &fsTLogger{
		recorder: recorder,
		Manager:  metaManager,
	}, nil
}