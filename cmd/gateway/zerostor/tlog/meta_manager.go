package tlog

import (
	"context"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/pkg/policy"
	"github.com/satori/uuid"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

// CreateBucket creates bucket given its name
func (t *TLogger) CreateBucket(name string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationBucketCreate,
		name,
	})

	if err != nil {
		return err
	}

	if err := t.meta.CreateBucket(name); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// DeleteBucket deletes a bucket given its name
func (t *TLogger) DeleteBucket(name string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationBucketDelete,
		name,
	})

	if err != nil {
		return err
	}

	if err := t.meta.DeleteBucket(name); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// ListBuckets lists all buckets
func (t *TLogger) ListBuckets() (map[string]*meta.Bucket, error) {
	return t.meta.ListBuckets()
}

// GetBucket returns a Bucket given its name
func (t *TLogger) GetBucket(name string) (*meta.Bucket, error) {
	return t.meta.GetBucket(name)
}

// SetBucketPolicy changes bucket policy
func (t *TLogger) SetBucketPolicy(name string, policy *policy.Policy) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationBucketSetPolicy,
		name,
		policy,
	})

	if err != nil {
		return err
	}

	if err := t.meta.SetBucketPolicy(name, policy); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// PutObject creates metadata for an object
func (t *TLogger) PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationObjectPut,
		metaData,
		bucket,
		object,
	})

	if err != nil {
		return minio.ObjectInfo{}, err
	}

	info, err := t.meta.PutObject(metaData, bucket, object)
	if err != nil {
		return info, err
	}

	return info, t.recorder.SetState(rec)
}

// PutObjectPart creates metadata for an object upload part
func (t *TLogger) PutObjectPart(metaData *metatypes.Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationPartPut,
		metaData,
		bucket,
		uploadID,
		partID,
	})

	if err != nil {
		return minio.PartInfo{}, err
	}

	info, err := t.meta.PutObjectPart(metaData, bucket, uploadID, partID)
	if err != nil {
		return info, err
	}

	return info, t.recorder.SetState(rec)
}

// DeleteBlob deletes a metadata blob file
func (t *TLogger) DeleteBlob(blob string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationBlobDelete,
		blob,
	})

	if err != nil {
		return err
	}

	if err := t.meta.DeleteBlob(blob); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// DeleteUpload deletes the temporary multipart upload dir
func (t *TLogger) DeleteUpload(bucket, uploadID string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationUploadDelete,
		bucket,
		uploadID,
	})

	if err != nil {
		return err
	}

	if err := t.meta.DeleteUpload(bucket, uploadID); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// DeleteObject deletes an object file from a bucket
func (t *TLogger) DeleteObject(bucket, object string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationObjectDelete,
		bucket,
		object,
	})

	if err != nil {
		return err
	}

	if err := t.meta.DeleteObject(bucket, object); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// LinkObject creates a symlink from the object file under /objects to the first metadata blob file
func (t *TLogger) LinkObject(bucket, object, blob string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationObjectLink,
		bucket,
		object,
		blob,
	})

	if err != nil {
		return err
	}

	if err := t.meta.LinkObject(bucket, object, blob); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// LinkPark links a multipart upload part to a metadata blob file
func (t *TLogger) LinkPark(bucket, uploadID, partID, blob string) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationPartLink,
		bucket,
		uploadID,
		partID,
		blob,
	})

	if err != nil {
		return err
	}

	if err := t.meta.LinkPark(bucket, uploadID, partID, blob); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// ListObjects lists objects in a bucket
func (t *TLogger) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	return t.meta.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

// ListObjectsV2 lists objects in a bucket
func (t *TLogger) ListObjectsV2(ctx context.Context, bucket, prefix,
	continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	return t.meta.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// NewMultipartUpload initializes a new multipart upload
func (t *TLogger) NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationUploadNew,
		bucket,
		object,
		opts,
	})

	if err != nil {
		return "", err
	}

	upload, err := t.meta.NewMultipartUpload(bucket, object, opts)
	if err != nil {
		return "", err
	}

	return upload, t.recorder.SetState(rec)
}

// ListUploadParts lists multipart upload parts
func (t *TLogger) ListUploadParts(bucket, uploadID string) ([]minio.PartInfo, error) {
	return t.meta.ListUploadParts(bucket, uploadID)
}

// WriteObjMeta write meta.ObjectMeta
func (t *TLogger) WriteObjMeta(obj *meta.ObjectMeta) error {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationObjectWriteMeta,
		obj,
	})

	if err != nil {
		return err
	}

	if err := t.meta.WriteObjMeta(obj); err != nil {
		return err
	}

	return t.recorder.SetState(rec)
}

// CompleteMultipartUpload completes a multipart upload by linking all metadata blobs
func (t *TLogger) CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error) {
	t.recorder.Begin()
	defer t.recorder.End()

	rec, err := t.recorder.Record(Record{
		OperationUploadComplete,
		bucket,
		object,
		uploadID,
		parts,
	})

	if err != nil {
		return minio.ObjectInfo{}, err
	}

	info, err := t.meta.CompleteMultipartUpload(bucket, object, uploadID, parts)
	if err != nil {
		return info, err
	}

	return info, t.recorder.SetState(rec)
}

// GetObjectInfo returns info about a bucket object
func (t *TLogger) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	return t.meta.GetObjectInfo(bucket, object)
}

// StreamObjectMeta streams an object metadata blobs through a channel
func (t *TLogger) StreamObjectMeta(ctx context.Context, bucket, object string) <-chan meta.Stream {
	return t.meta.StreamObjectMeta(ctx, bucket, object)
}

//WriteMetaStream writes all the incomming meta stream
func (t *TLogger) WriteMetaStream(ctx context.Context, c <-chan *metatypes.Metadata, bucket, object string) <-chan error {
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
func (t *TLogger) StreamBlobs(ctx context.Context) <-chan meta.Stream {
	return t.meta.StreamBlobs(ctx)
}

// ValidUpload checks if an upload id is valid
func (t *TLogger) ValidUpload(bucket, uploadID string) (bool, error) {
	return t.meta.ValidUpload(bucket, uploadID)
}

// ListMultipartUploads lists multipart uploads that are in progress
func (t *TLogger) ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error) {
	return t.meta.ListMultipartUploads(bucket)
}

// StreamMultiPartsMeta streams parts metadata for a multiupload
func (t *TLogger) StreamMultiPartsMeta(ctx context.Context, bucket, uploadID string) <-chan meta.Stream {
	return t.meta.StreamMultiPartsMeta(ctx, bucket, uploadID)
}
