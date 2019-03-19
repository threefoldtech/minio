package tlog

import (
	"context"
	"encoding/json"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/pkg/policy"
	"github.com/satori/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

// CreateBucket creates bucket given its name
func (t *fsTLogger) CreateBucket(name string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.CreateBucket(name); err != nil {
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

	if err := t.meta.DeleteBucket(name); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationBucketDelete,
		name,
	}, true)
	return err
}

// ListBuckets lists all buckets
func (t *fsTLogger) ListBuckets() (map[string]*meta.Bucket, error) {
	return t.meta.ListBuckets()
}

// GetBucket returns a Bucket given its name
func (t *fsTLogger) GetBucket(name string) (*meta.Bucket, error) {
	return t.meta.GetBucket(name)
}

// SetBucketPolicy changes bucket policy
func (t *fsTLogger) SetBucketPolicy(name string, policy *policy.Policy) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.SetBucketPolicy(name, policy); err != nil {
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
	t.recorder.Begin()
	defer t.recorder.End()

	info, err := t.meta.PutObject(metaData, bucket, object)
	if err != nil {
		return info, err
	}

	metaBytes, err := json.Marshal(metaData)
	if err != nil {
		return info, err
	}
	_, err = t.recorder.Record(Record{
		OperationObjectPut,
		metaBytes,
		bucket,
		object,
	}, true)
	return info, err
}

// PutObjectPart creates metadata for an object upload part
func (t *fsTLogger) PutObjectPart(metaData *metatypes.Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	info, err := t.meta.PutObjectPart(metaData, bucket, uploadID, partID)
	if err != nil {
		return info, err
	}

	metaBytes, err := json.Marshal(metaData)
	if err != nil {
		return info, err
	}
	_, err = t.recorder.Record(Record{
		OperationPartPut,
		metaBytes,
		bucket,
		uploadID,
		partID,
	}, true)
	return info, err
}

// DeleteBlob deletes a metadata blob file
func (t *fsTLogger) DeleteBlob(blob string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.DeleteBlob(blob); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationBlobDelete,
		blob,
	}, true)
	return err
}

// DeleteUpload deletes the temporary multipart upload dir
func (t *fsTLogger) DeleteUpload(bucket, uploadID string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.DeleteUpload(bucket, uploadID); err != nil {
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
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.DeleteObject(bucket, object); err != nil {
		return err
	}
	_, err := t.recorder.Record(Record{
		OperationObjectDelete,
		bucket,
		object,
	}, true)
	return err
}

// LinkObject creates a symlink from the object file under /objects to the first metadata blob file
func (t *fsTLogger) LinkObject(bucket, object, blob string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.LinkObject(bucket, object, blob); err != nil {
		return err
	}
	_, err := t.recorder.Record(Record{
		OperationObjectLink,
		bucket,
		object,
		blob,
	}, true)
	return err
}

// LinkPart links a multipart upload part to a metadata blob file
func (t *fsTLogger) LinkPart(bucket, uploadID, partID, blob string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.LinkPart(bucket, uploadID, partID, blob); err != nil {
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
}

// ListObjects lists objects in a bucket
func (t *fsTLogger) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	return t.meta.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

// ListObjectsV2 lists objects in a bucket
func (t *fsTLogger) ListObjectsV2(ctx context.Context, bucket, prefix,
	continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	return t.meta.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// NewMultipartUpload initializes a new multipart upload
func (t *fsTLogger) NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	upload, err := t.meta.NewMultipartUpload(bucket, object, opts)
	if err != nil {
		return "", err
	}
	optsBytes, err := json.Marshal(opts)
	if err != nil {
		return "", err
	}
	_, err = t.recorder.Record(Record{
		OperationUploadNew,
		bucket,
		object,
		optsBytes,
	}, true)
	return upload, err
}

// ListUploadParts lists multipart upload parts
func (t *fsTLogger) ListUploadParts(bucket, uploadID string) ([]minio.PartInfo, error) {
	return t.meta.ListUploadParts(bucket, uploadID)
}

// WriteObjMeta write meta.ObjectMeta
func (t *fsTLogger) WriteObjMeta(obj *meta.ObjectMeta) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.meta.WriteObjMeta(obj); err != nil {
		return err
	}

	metaBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	_, err = t.recorder.Record(Record{
		OperationObjectWriteMeta,
		metaBytes,
	}, true)
	return err
}

// CompleteMultipartUpload completes a multipart upload by linking all metadata blobs
func (t *fsTLogger) CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	info, err := t.meta.CompleteMultipartUpload(bucket, object, uploadID, parts)
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
}

// GetObjectInfo returns info about a bucket object
func (t *fsTLogger) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	return t.meta.GetObjectInfo(bucket, object)
}

// StreamObjectMeta streams an object metadata blobs through a channel
func (t *fsTLogger) StreamObjectMeta(ctx context.Context, bucket, object string) <-chan meta.Stream {
	return t.meta.StreamObjectMeta(ctx, bucket, object)
}

//WriteMetaStream writes all the incomming meta stream
func (t *fsTLogger) WriteMetaStream(ctx context.Context, c <-chan *metatypes.Metadata, bucket, object string) <-chan error {
	errc := make(chan error)
	go func() {
		var totalSize int64
		var modTime int64
		var previousPart meta.ObjectMeta
		var firstPart meta.ObjectMeta
		var objMeta meta.ObjectMeta
		counter := 0

		defer close(errc)
		for metaData := range c {
			select {
			case <-ctx.Done():
				return
			default:
			}

			totalSize += metaData.Size
			modTime = metaData.LastWriteEpoch
			objMeta := meta.ObjectMeta{
				Metadata: *metaData,
				Filename: uuid.NewV4().String(),
			}

			if counter > 0 {
				previousPart.NextBlob = objMeta.Filename
				if err := t.WriteObjMeta(&previousPart); err != nil {
					errc <- err
				}
				if counter == 1 {
					// link the first blob
					firstPart = previousPart
					if err := t.LinkObject(bucket, object, firstPart.Filename); err != nil {
						errc <- err
					}
				}
			}
			previousPart = objMeta
			counter++
		}

		// write the meta of the last received metadata
		if err := t.WriteObjMeta(&objMeta); err != nil {
			errc <- err
		}
		firstPart.ObjectSize = totalSize
		firstPart.ObjectModTime = modTime
		firstPart.ObjectUserMeta = firstPart.UserDefined

		// update the the first meta part with the size and mod time
		if err := t.WriteObjMeta(&firstPart); err != nil {
			errc <- err
		}
	}()
	return errc
}

// StreamBlobs streams all blobs
func (t *fsTLogger) StreamBlobs(ctx context.Context) <-chan meta.Stream {
	return t.meta.StreamBlobs(ctx)
}

// ValidUpload checks if an upload id is valid
func (t *fsTLogger) ValidUpload(bucket, uploadID string) (bool, error) {
	return t.meta.ValidUpload(bucket, uploadID)
}

// ListMultipartUploads lists multipart uploads that are in progress
func (t *fsTLogger) ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error) {
	return t.meta.ListMultipartUploads(bucket)
}

// StreamMultiPartsMeta streams parts metadata for a multiupload
func (t *fsTLogger) StreamMultiPartsMeta(ctx context.Context, bucket, uploadID string) <-chan meta.Stream {
	return t.meta.StreamMultiPartsMeta(ctx, bucket, uploadID)
}

//Sync syncs the backend storage with the latest records from the tlog storage
func (t *fsTLogger) Sync() error {

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
