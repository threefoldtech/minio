package meta

import (
	"context"
	"errors"
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

	//ErrDirectoryNotEmpty in case deleting a directory that has children
	ErrDirectoryNotEmpty = errors.New("directory not empty")
)

// Manager interface for metadata managers
type Manager interface {
	BucketCreate(string) error
	BucketGet(name string) (*Bucket, error)
	BucketDelete(string) error
	BucketIsEmpty(string) (bool, error)
	BucketSetPolicy(name string, policy *policy.Policy) error
	BucketsList() (map[string]*Bucket, error)

	// ObjectEnsure returns or create a new initialized object
	// with default version (delete-marker = true)
	ObjectEnsure(bucket, object string) (ObjectID, error)
	// ObjectGet gets object ID
	ObjectGet(bucket, object string) (ObjectID, error)
	// ObjectSet creates a new version that points to given meta
	ObjectSet(id ObjectID, meta string) (version string, err error)
	// ObjectDel creates a new version that points to a delete marker
	ObjectDelete(id ObjectID) error
	// ObjectGetInfo get info for an object
	ObjectGetInfo(bucket, object, version string) (minio.ObjectInfo, error)

	ObjectGetObjectVersions(id ObjectID) ([]string, error)
	// Upload operations
	UploadCreate(bucket, object string, meta map[string]string) (string, error)
	UploadGet(bucket, uploadID string) (info minio.MultipartInfo, err error)
	UploadPutPart(bucket, uploadID string, partID int, meta string) error
	UploadListParts(bucket, uploadID string) ([]minio.PartInfo, error)
	UploadComplete(bucket, object, uploadID string, parts []minio.CompletePart) (Metadata, error)
	//UploadDelete deletes upload metadata (parts info) to fully delete an upload
	//you need to delete its parts first
	UploadDelete(bucket, uploadID string) error
	UploadList(bucket string) (minio.ListMultipartsInfo, error)
	UploadDeletePart(bucket, uploadID string, partID int) error
	UploadExists(bucket, uploadID string) (bool, error)

	// MetaWriteStream calls cb until receive an EOF error. Creates a new blob
	// for each meta object returned by cb. Links blobs together and return
	// the head. Blobs can be streamed again later using the StreamObjectMeta method
	MetaWriteStream(cb func() (*metatypes.Metadata, error)) (Metadata, error)
	// MetaGetStream returns meta stream for an object
	MetaGetStream(ctx context.Context, bucket, object, version string) <-chan Stream

	SetBlob(obj *Metadata) error

	//LinkPart(bucket, uploadID, partID, blob string) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error)

	// DEPRECATED
	Mkdir(bucket, object string) error

	StreamBlobs(ctx context.Context) <-chan Stream

	Close() error
}

// Bucket defines a minio bucket
type Bucket struct {
	Name    string        `json:"name"`
	Created time.Time     `json:"created"`
	Policy  policy.Policy `json:"policy"`
}

//ObjectID type
type ObjectID string

// Metadata defines meta for an object
type Metadata struct {
	metatypes.Metadata
	NextBlob       string
	ObjectSize     int64
	ObjectModTime  int64
	ObjectUserMeta map[string]string
	Filename       string
	IsDir          bool
}

// Stream is used to stream ObjMeta through a chan
type Stream struct {
	Obj   Metadata
	Error error
}

// NewMetaManager creates a new metadata manager using the backend store provided
func NewMetaManager(store Store, key string) Manager {
	mgr := &metaManager{
		store: store,
		key:   key,
	}

	mgr.initialize()

	return mgr
}

func getUserMetadataValue(key string, userMeta map[string]string) string {
	v := userMeta[key]
	return v
}

// EpochToTimestamp converts zerostor epoch time to Go timestamp
func EpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
