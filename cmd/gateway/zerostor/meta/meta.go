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

// Collection defines a collection type under the metastore
type Collection string

const (
	//ObjectCollection defines object collection
	ObjectCollection Collection = "object"
	//BlobCollection defines blob collection
	BlobCollection Collection = "blob"
	//UploadCollection defines upload collection
	UploadCollection Collection = "upload"
	//BucketCollection defines bucket collection
	BucketCollection Collection = "bucket"
	//VersionsCollection defines version collection
	VersionsCollection Collection = "version"
)

//ScanMode scan mode type
type ScanMode int

const (
	// ScanModeRecursive scans entire prefix recursively
	ScanModeRecursive = iota
	// ScanModeDelimited only scans direct object under a prefix
	ScanModeDelimited
)

// Path defines a path to object in the metastore
type Path struct {
	Collection Collection
	// Prefix is basically the "directory" where this object is stored.
	// a Path with Prefix only is assumed to be a directory.
	Prefix string
	// Name is the name of the object, a Path with Name set is assumed to
	// be a file.
	Name string
}

// NewPath creates a new path from collection, prefix and name
func NewPath(collection Collection, prefix string, name string) Path {
	if len(collection) == 0 {
		panic("invalid collection")
	}

	return Path{Collection: collection, Prefix: prefix, Name: name}
}

// FilePath always create a Path to an object (Name will be set to last part)
// to create a path to a directory use NewPath
func FilePath(collection Collection, parts ...string) Path {
	prefix, name := filepath.Split(filepath.Join(parts...))
	return NewPath(collection, prefix, name)
}

// DirPath always create a Path to a directory
// to create a path to a file use FilePath
func DirPath(collection Collection, parts ...string) Path {
	return NewPath(collection, filepath.Join(parts...), "")
}

// Base return the name
func (p *Path) Base() string {
	return p.Name
}

// Relative joins prefix and name
func (p *Path) Relative() string {
	return filepath.Join(p.Prefix, p.Name)
}

// IsDir only check how the Path is constructed, a Path with empty Name (only Prefix)
// is assumed to be a Directory. It's up to the implementation to make sure this
// rule is in effect.
func (p *Path) IsDir() bool {
	return len(p.Name) == 0
}

// Join creates a new path from this path plus the new parts
func (p *Path) Join(parts ...string) Path {
	return FilePath(p.Collection, filepath.Join(p.Prefix, p.Name, filepath.Join(parts...)))
}

func (p *Path) String() string {
	return filepath.Join(string(p.Collection), p.Prefix, p.Name)
}

// Record type returned by a store.Scan operation
type Record struct {
	Path Path
	Data []byte
	Time time.Time
	Link bool
}

// Store defines the interface to a low level metastore
type Store interface {
	Set(path Path, data []byte) error
	Get(path Path) (Record, error)
	Del(path Path) error
	Exists(path Path) (bool, error)
	Link(link, target Path) error
	List(path Path) ([]Path, error)
	Scan(path Path, after string, limit int, mode ScanMode) ([]Path, error)
	Close() error
}

// Manager interface for metadata managers
type Manager interface {
	CreateBucket(string) error
	GetBucket(name string) (*Bucket, error)
	DeleteBucket(string) error
	IsBucketEmpty(string) (bool, error)
	ListBuckets() (map[string]*Bucket, error)
	SetBucketPolicy(name string, policy *policy.Policy) error

	EnsureObject(bucket, object string) (Object, error)
	WriteMetaStream(cb func() (*metatypes.Metadata, error)) (Metadata, error)

	WriteObjMeta(obj *Metadata) error
	LinkObject(bucket, object, blob string) error
	LinkPart(bucket, uploadID, partID, blob string) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error)
	NewMultipartUpload(bucket, object string, uploadID string, meta map[string]string) error
	ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error)
	DeleteUpload(bucket, uploadID string) error
	ListUploadParts(bucket, uploadID string) ([]minio.PartInfo, error)
	CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error)
	DeleteBlob(blob string) error
	DeleteObject(bucket, object string) error
	PutObjectPart(objMeta Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error)
	PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error)
	Mkdir(bucket, object string) error
	GetObjectInfo(bucket, object string) (minio.ObjectInfo, error)
	GetObjectMeta(bucket, object string) (Metadata, error)
	StreamObjectMeta(ctx context.Context, bucket, object string) <-chan Stream
	StreamMultiPartsMeta(ctx context.Context, bucket, uploadID string) <-chan Stream
	StreamBlobs(ctx context.Context) <-chan Stream
	ValidUpload(bucket, uploadID string) (bool, error)

	Close() error
}

// Bucket defines a minio bucket
type Bucket struct {
	Name    string        `json:"name"`
	Created time.Time     `json:"created"`
	Policy  policy.Policy `json:"policy"`
}

// Object is main object entrypoint
type Object struct {
	ID      string
	Version string

	//TODO: add flags here like
	//deleted marker and others
}

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
	return &metaManager{
		store: store,
		key:   key,
	}
}

func getUserMetadataValue(key string, userMeta map[string]string) string {
	v := userMeta[key]
	return v
}

// convert zerostor epoch time to Go timestamp
func zstorEpochToTimestamp(epoch int64) time.Time {
	return time.Unix(epoch/1e9, epoch%1e9)
}
