package meta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
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
	SetChunk(metaData *ObjectMeta) (string, error)
	SetObjectLink(bucket, object, partID string) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error)
	NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error)
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
	NextPart   string
	ObjectSize int64
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
		partDir:   filepath.Join(dir, partDir),
		uploadDir: filepath.Join(dir, uploadDir),
	}
	if err := meta.createDirs(); err != nil {
		return nil, err
	}

	return meta, nil
}

func createWriteFile(filename string, content []byte, perm os.FileMode) error {
	// try to writes file directly
	var (
		file *os.File
		err  error
	)

	if err = os.MkdirAll(filepath.Dir(filename), dirPerm); err != nil {
		return err
	}

	file, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}

	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	//we only truncate the file once we have the lock
	//to not corrupt file data
	if err = file.Truncate(0); err != nil {
		return err
	}

	_, err = file.Write(content)

	return err
}

// CreateObjectInfo creates minio ObjectInfo from 0-stor metadata
func CreateObjectInfo(bucket, object string, md *ObjectMeta) minio.ObjectInfo {
	etag := getUserMetadataValue(ETagKey, md.UserDefined)
	if etag == "" {
		etag = object
	}

	storageClass := "STANDARD"
	if class, ok := md.UserDefined[amzStorageClass]; ok {
		storageClass = class
	}

	info := minio.ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		Size:            md.Size,
		ModTime:         zstorEpochToTimestamp(md.LastWriteEpoch),
		ETag:            etag,
		ContentType:     getUserMetadataValue(contentTypeKey, md.UserDefined),
		ContentEncoding: getUserMetadataValue(contentEncodingKey, md.UserDefined),
		StorageClass:    storageClass,
	}

	delete(md.UserDefined, contentTypeKey)
	delete(md.UserDefined, contentEncodingKey)
	delete(md.UserDefined, amzStorageClass)
	delete(md.UserDefined, ETagKey)
	delete(md.UserDefined, nextPartKey)

	info.UserDefined = md.UserDefined

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

func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err != nil, err
}
