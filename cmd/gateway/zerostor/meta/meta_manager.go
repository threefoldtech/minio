package meta

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/policy"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

var (
	defaultPolicy = policy.Policy{
		Version: policy.DefaultVersion,
	}
	errMaxKeyReached = errors.New("max keys reached")
)

// Manager interface for meta managers
type Manager interface {
	CreateBucket(string) error
	GetBucket(name string) (*Bucket, error)
	DeleteBucket(string) error
	ListBuckets() (map[string]*Bucket, error)
	SetBucketPolicy(name string, policy *policy.Policy) error
	SetObjectLink(bucket, object, fileID string) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error)
	NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error)
	ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error)
	SetPartLink(bucket, uploadID, partID, fileID string) error
	ListPartsInfo(bucket, uploadID string) ([]minio.PartInfo, error)
	ListPartsMeta(bucket, uploadID string) (map[int]*ObjectMeta, error)
	CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error)
	DeleteBlob(blob string) error
	DeleteUploadDir(bucket, uploadID string) error
	DeleteObjectFile(bucket, object string) error
	PutObjectPart(metaData *metatypes.Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error)
	PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error)
	GetObjectInfo(bucket, object string) (minio.ObjectInfo, error)
	StreamObjectMeta(done <-chan struct{}, bucket, object string) <-chan result
	ValidUpload(bucket, uploadID string) (bool, error)
}

// Bucket defines a bucket
type Bucket struct {
	Name    string        `json:"name"`
	Created time.Time     `json:"created"`
	Policy  policy.Policy `json:"policy"`
}

// ObjectMeta defines meta for an object
type ObjectMeta struct {
	*metatypes.Metadata
	NextBlob       string
	ObjectSize     int64
	ObjectModTime  int64
	ObjectUserMeta map[string]string
	Filename       string
}

// MultiPartInfo represents info/metadata of a multipart upload
type MultiPartInfo struct {
	minio.MultipartInfo
	Metadata map[string]string
}

// InitializeMetaManager creates the the meta manager and loads the buckets
func InitializeMetaManager(dir string) (Manager, error) {
	meta := &Meta{
		objDir:    filepath.Join(dir, objectDir),
		bucketDir: filepath.Join(dir, bucketDir),
		blobDir:   filepath.Join(dir, blobDir),
		uploadDir: filepath.Join(dir, uploadDir),
	}
	if err := meta.createDirs(); err != nil {
		return nil, err
	}

	return meta, nil
}

// CreateObjectInfo creates minio ObjectInfo from 0-stor metadata
func CreateObjectInfo(bucket, object string, md *ObjectMeta) minio.ObjectInfo {
	etag := getUserMetadataValue(ETagKey, md.ObjectUserMeta)
	if etag == "" {
		etag = object
	}

	storageClass := "STANDARD"
	if class, ok := md.ObjectUserMeta[amzStorageClass]; ok {
		storageClass = class
	}

	info := minio.ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		Size:            md.ObjectSize,
		ModTime:         zstorEpochToTimestamp(md.ObjectModTime),
		ETag:            etag,
		ContentType:     getUserMetadataValue(contentTypeKey, md.ObjectUserMeta),
		ContentEncoding: getUserMetadataValue(contentEncodingKey, md.ObjectUserMeta),
		StorageClass:    storageClass,
	}

	delete(md.ObjectUserMeta, contentTypeKey)
	delete(md.ObjectUserMeta, contentEncodingKey)
	delete(md.ObjectUserMeta, amzStorageClass)
	delete(md.ObjectUserMeta, ETagKey)
	delete(md.ObjectUserMeta, nextBlobKey)

	info.UserDefined = md.ObjectUserMeta

	return info
}

func getUserMetadataValue(key string, userMeta map[string]string) string {
	v, _ := userMeta[key]
	return v
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
