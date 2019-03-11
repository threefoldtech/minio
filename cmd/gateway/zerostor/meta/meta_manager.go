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

const (
	contentTypeKey     = "content-type"
	contentEncodingKey = "content-encoding"
	amzStorageClass    = "x-amz-storage-class"
	fileMetaDirSize    = 4096 // size of dir always 4096
	// ETagKey is metadata etag key
	ETagKey = "etag"
	// MinPartSize limit on min part size for partial upload
	MinPartSize = 5 * 1024 * 1024
)

var (
	defaultPolicy = policy.Policy{
		Version: policy.DefaultVersion,
	}
	errMaxKeyReached = errors.New("max keys reached")
)

// Manager interface for metadata managers
type Manager interface {
	WriteObjMeta(obj *ObjectMeta) error
	CreateBucket(string) error
	GetBucket(name string) (*Bucket, error)
	DeleteBucket(string) error
	ListBuckets() (map[string]*Bucket, error)
	SetBucketPolicy(name string, policy *policy.Policy) error
	SetObjectLink(bucket, object, blob string) error
	SetPartLink(bucket, uploadID, partID, blob string) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error)
	NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error)
	ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error)
	DeleteUploadDir(bucket, uploadID string) error
	ListPartsInfo(bucket, uploadID string) ([]minio.PartInfo, error)
	CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error)
	DeleteBlob(blob string) error
	DeleteObjectFile(bucket, object string) error
	PutObjectPart(metaData *metatypes.Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error)
	PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error)
	GetObjectInfo(bucket, object string) (minio.ObjectInfo, error)
	StreamObjectMeta(ctx context.Context, bucket, object string) <-chan Stream
	StreamPartsMeta(ctx context.Context, bucket, uploadID string) <-chan Stream
	StreamBlobs(ctx context.Context) <-chan Stream
	ValidUpload(bucket, uploadID string) (bool, error)
	WriteMetaStream(ctx context.Context, c <-chan metatypes.Metadata) <-chan error
}

// Bucket defines a bucket
type Bucket struct {
	Name    string        `json:"name"`
	Created time.Time     `json:"created"`
	Policy  policy.Policy `json:"policy"`
}

type Stream struct {
	Obj   ObjectMeta
	Error error
}

// InitializeMetaManager creates the the meta manager and loads the buckets
func InitializeMetaManager(dir string) (Manager, error) {
	meta := &filesystemMeta{
		objDir:    filepath.Join(dir, objectDir),
		bucketDir: filepath.Join(dir, bucketDir),
		blobDir:   filepath.Join(dir, blobDir),
		uploadDir: filepath.Join(dir, uploadDir),
	}

	return meta, meta.createDirs()
}

func getUserMetadataValue(key string, userMeta map[string]string) string {
	v, _ := userMeta[key]
	return v
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
