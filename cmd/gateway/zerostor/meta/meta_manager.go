package meta

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/bucket/policy"
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
	IsBucketEmpty(string) (bool, error)
	ListBuckets() (map[string]*Bucket, error)
	SetBucketPolicy(name string, policy *policy.Policy) error
	LinkObject(bucket, object, blob string) error
	LinkPart(bucket, uploadID, partID, blob string) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error)
	NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error)
	ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error)
	DeleteUpload(bucket, uploadID string) error
	ListUploadParts(bucket, uploadID string) ([]minio.PartInfo, error)
	CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error)
	DeleteBlob(blob string) error
	DeleteObject(bucket, object string) error
	PutObjectPart(objMeta ObjectMeta, bucket, uploadID string, partID int) (minio.PartInfo, error)
	PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error)
	GetObjectInfo(bucket, object string) (minio.ObjectInfo, error)
	GetObjectMeta(bucket, object string) (ObjectMeta, error)
	StreamObjectMeta(ctx context.Context, bucket, object string) <-chan Stream
	StreamMultiPartsMeta(ctx context.Context, bucket, uploadID string) <-chan Stream
	StreamBlobs(ctx context.Context) <-chan Stream
	ValidUpload(bucket, uploadID string) (bool, error)
	WriteMetaStream(cb func() (*metatypes.Metadata, error), bucket, object string) (ObjectMeta, error)
}

// Bucket defines a minio bucket
type Bucket struct {
	Name    string        `json:"name"`
	Created time.Time     `json:"created"`
	Policy  policy.Policy `json:"policy"`
}

// ObjectMeta defines meta for an object
type ObjectMeta struct {
	metatypes.Metadata
	NextBlob       string
	ObjectSize     int64
	ObjectModTime  int64
	ObjectUserMeta map[string]string
	Filename       string
}

// Stream is used to stream ObjMeta through a chan
type Stream struct {
	Obj   ObjectMeta
	Error error
}

// InitializeMetaManager creates the the meta manager and loads the buckets
func InitializeMetaManager(dir string, key string) (Manager, error) {
	meta := &filesystemMeta{
		objDir:    filepath.Join(dir, objectDir),
		bucketDir: filepath.Join(dir, bucketDir),
		blobDir:   filepath.Join(dir, blobDir),
		uploadDir: filepath.Join(dir, uploadDir),
		key:       key,
	}

	return meta, meta.initialize()
}

func getUserMetadataValue(key string, userMeta map[string]string) string {
	v := userMeta[key]
	return v
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
