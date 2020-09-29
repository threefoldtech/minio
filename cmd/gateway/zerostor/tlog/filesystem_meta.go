package tlog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/satori/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

// CreateBucket creates bucket given its name
func (t *fsTLogger) CreateBucket(name string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Manager.CreateBucket(name); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationBucketCreate,
		name,
	}, true)
	return err
}

// DeleteBucket deletes a bucket given its name
func (t *fsTLogger) DeleteBucket(name string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Manager.DeleteBucket(name); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationBucketDelete,
		name,
	}, true)
	return err
}

// SetBucketPolicy changes bucket policy
func (t *fsTLogger) SetBucketPolicy(name string, policy *policy.Policy) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Manager.SetBucketPolicy(name, policy); err != nil {
		return err
	}
	polBytes, err := policy.MarshalJSON()
	if err != nil {
		return err
	}

	_, err = t.recorder.Record(Record{
		OperationBucketSetPolicy,
		name,
		polBytes,
	}, true)
	return err
}

// PutObject creates metadata for an object
func (t *fsTLogger) PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, fmt.Errorf("not implemented")

	// t.recorder.Begin()
	// defer t.recorder.End()

	// info, err := t.Manager.PutObject(metaData, bucket, object)
	// if err != nil {
	// 	return info, err
	// }

	// metaBytes, err := json.Marshal(metaData)
	// if err != nil {
	// 	return info, err
	// }
	// _, err = t.recorder.Record(Record{
	// 	OperationObjectPut,
	// 	metaBytes,
	// 	bucket,
	// 	object,
	// }, true)
	// return info, err
}

// PutObject creates metadata for an object
func (t *fsTLogger) Mkdir(bucket, object string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	err := t.Manager.Mkdir(bucket, object)
	if err != nil {
		return err
	}

	_, err = t.recorder.Record(Record{
		OperationObjectMkdir,
		bucket,
		object,
	}, true)

	return err
}

// PutObjectPart creates metadata for an object upload part
func (t *fsTLogger) PutObjectPart(objMeta meta.Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error) {
	return minio.PartInfo{}, fmt.Errorf("not implemented")

	// t.recorder.Begin()
	// defer t.recorder.End()

	// info, err := t.Manager.UploadPutPart(objMeta, bucket, uploadID, partID)
	// if err != nil {
	// 	return info, err
	// }

	// metaBytes, err := json.Marshal(objMeta)
	// if err != nil {
	// 	return info, err
	// }
	// _, err = t.recorder.Record(Record{
	// 	OperationPartPut,
	// 	metaBytes,
	// 	bucket,
	// 	uploadID,
	// 	partID,
	// }, true)
	// return info, err
}

// DeleteBlob deletes a metadata blob file
func (t *fsTLogger) DeleteBlob(blob string) error {
	return fmt.Errorf("not implemented")
	// t.recorder.Begin()
	// defer t.recorder.End()

	// if err := t.Manager.DeleteBlob(blob); err != nil {
	// 	return err
	// }

	// _, err := t.recorder.Record(Record{
	// 	OperationBlobDelete,
	// 	blob,
	// }, true)
	// return err
}

// DeleteUpload deletes the temporary multipart upload dir
func (t *fsTLogger) DeleteUpload(bucket, uploadID string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Manager.UploadDelete(bucket, uploadID); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationUploadDelete,
		bucket,
		uploadID,
	}, true)
	return err
}

// DeleteObject deletes an object file from a bucket
func (t *fsTLogger) DeleteObject(bucket, object string) error {
	return fmt.Errorf("not implemeneted")
	// t.recorder.Begin()
	// defer t.recorder.End()

	// if err := t.Manager.DeleteObject(bucket, object); err != nil {
	// 	return err
	// }
	// _, err := t.recorder.Record(Record{
	// 	OperationObjectDelete,
	// 	bucket,
	// 	object,
	// }, true)
	// return err
}

// LinkObject creates a symlink from the object file under /objects to the first metadata blob file
func (t *fsTLogger) LinkObject(bucket, object, blob string) error {
	return fmt.Errorf("not implemented")
	// t.recorder.Begin()
	// defer t.recorder.End()

	// if err := t.Manager.LinkObject(bucket, object, blob); err != nil {
	// 	return err
	// }
	// _, err := t.recorder.Record(Record{
	// 	OperationObjectLink,
	// 	bucket,
	// 	object,
	// 	blob,
	// }, true)
	// return err
}

// LinkPart links a multipart upload part to a metadata blob file
func (t *fsTLogger) LinkPart(bucket, uploadID, partID, blob string) error {
	return fmt.Errorf("not implemented")
	/*
		t.recorder.Begin()
		defer t.recorder.End()

		if err := t.Manager.LinkPart(bucket, uploadID, partID, blob); err != nil {
			return err
		}

		_, err := t.recorder.Record(Record{
			OperationPartLink,
			bucket,
			uploadID,
			partID,
			blob,
		}, true)
		return err
	*/
}

// NewMultipartUpload initializes a new multipart upload
func (t *fsTLogger) NewMultipartUpload(bucket, object, uploadID string, meta map[string]string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	uploadID, err := t.Manager.UploadCreate(bucket, object, meta)
	if err != nil {
		return err
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = t.recorder.Record(Record{
		OperationUploadNew,
		bucket,
		object,
		uploadID,
		metaBytes,
	}, true)
	return err
}

// WriteObjMeta write meta.ObjectMeta
func (t *fsTLogger) WriteObjMeta(obj *meta.Metadata) error {
	return fmt.Errorf("not implemented")
	// t.recorder.Begin()
	// defer t.recorder.End()

	// if err := t.Manager.SetBlob(obj); err != nil {
	// 	return err
	// }

	// metaBytes, err := json.Marshal(obj)
	// if err != nil {
	// 	return err
	// }

	// _, err = t.recorder.Record(Record{
	// 	OperationObjectWriteMeta,
	// 	metaBytes,
	// }, true)
	// return err
}

// CompleteMultipartUpload completes a multipart upload by linking all metadata blobs
func (t *fsTLogger) CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, fmt.Errorf("not implemented")
	/*
		t.recorder.Begin()
		defer t.recorder.End()

		info, err := t.Manager.UploadComplete(bucket, object, uploadID, parts)
		if err != nil {
			return info, err
		}

		partsBytes, err := json.Marshal(parts)
		if err != nil {
			return info, err
		}

		_, err = t.recorder.Record(Record{
			OperationUploadComplete,
			bucket,
			object,
			uploadID,
			partsBytes,
		}, true)
		return info, err
	*/
}

// WriteMetaStream writes a stream of metadata to disk, links them, and returns the first blob
func (t *fsTLogger) WriteMetaStream(cb func() (*metatypes.Metadata, error)) (meta.Metadata, error) {
	var totalSize int64
	var modTime int64
	var previousPart meta.Metadata
	var firstPart meta.Metadata
	var objMeta meta.Metadata
	counter := 0

	for {
		metaData, err := cb()
		if err == io.EOF {
			break
		} else if err != nil {
			return meta.Metadata{}, err
		}

		totalSize += metaData.Size
		modTime = metaData.LastWriteEpoch
		objMeta = meta.Metadata{
			Metadata: *metaData,
			Filename: uuid.NewV4().String(),
		}

		// if this is not the first iteration set the NextBlob on the previous blob and save it if it is not the first blob
		if counter > 0 {
			previousPart.NextBlob = objMeta.Filename
			if counter == 1 {
				// update the first part
				firstPart = previousPart
			} else {
				if err := t.WriteObjMeta(&previousPart); err != nil {
					return meta.Metadata{}, err
				}
			}
		} else { // if this is the first iteration, mark the first blob
			firstPart = objMeta
		}
		previousPart = objMeta
		counter++
	}

	// write the meta of the last received metadata
	if err := t.WriteObjMeta(&objMeta); err != nil {
		return meta.Metadata{}, err
	}

	firstPart.ObjectSize = totalSize
	firstPart.ObjectModTime = modTime
	firstPart.ObjectUserMeta = firstPart.UserDefined

	// update the the first meta part with the size and mod time
	if err := t.WriteObjMeta(&firstPart); err != nil {
		return meta.Metadata{}, err
	}

	return firstPart, nil
}

//Sync syncs the backend storage with the latest records from the tlog storage
func (t *fsTLogger) Sync() error {

	return t.recorder.Play(nil, func(key []byte, rec Record) error {
		if err := rec.Play(t.Manager); err != nil {
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
func (t *fsTLogger) HealthChecker(ctx context.Context) {
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
