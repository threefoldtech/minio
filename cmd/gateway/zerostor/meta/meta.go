package meta

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/policy"
	"github.com/pkg/errors"
	"github.com/satori/uuid"
	"github.com/threefoldtech/0-stor/client/metastor/db"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"

	log "github.com/sirupsen/logrus"
)

const (
	bucketDir          = "buckets"
	objectDir          = "objects"
	partDir            = "parts"
	uploadDir          = "uploads"
	uploadMetaFile     = ".meta"
	contentTypeKey     = "content-type"
	contentEncodingKey = "content-encoding"
	amzStorageClass    = "x-amz-storage-class"
	nextPartKey        = "next-part"
	ETagKey            = "etag"
	fileMetaDirSize    = 4096 // size of dir always 4096

)

var (
	dirPerm  = os.FileMode(0755)
	filePerm = os.FileMode(0644)
)

// Meta implements the Manager interface
type Meta struct {
	bucketDir string
	objDir    string
	partDir   string
	uploadDir string
}

func (m *Meta) createDirs() error {
	// initialize buckets dir, if not exist
	if err := os.MkdirAll(m.bucketDir, dirPerm); err != nil {
		return err
	}

	// initialize objects dir, if not exist
	if err := os.MkdirAll(m.objDir, dirPerm); err != nil {
		return err
	}

	// initialize parts dir, if not exist
	if err := os.MkdirAll(m.partDir, dirPerm); err != nil {
		return err
	}

	return nil
}

// CreateBucket creates bucket given its name
func (m *Meta) CreateBucket(name string) error {
	if exists, err := exists(m.bucketFileName(name)); err != nil {
		return err
	} else if exists {
		return minio.BucketAlreadyExists{}
	}

	// creates the actual bucket
	if err := m.createBucket(name); err != nil {
		return err
	}
	if err := os.MkdirAll(m.objectsBucket(name), dirPerm); err != nil {
		return err
	}
	return nil
}
func (m *Meta) createBucket(name string) error {
	b := &Bucket{
		Name:    name,
		Created: time.Now(),
		Policy:  defaultPolicy,
	}
	return m.saveBucket(b)
}

func (m *Meta) saveBucket(bkt *Bucket) error {
	f, err := os.OpenFile(m.bucketFileName(bkt.Name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if os.IsNotExist(err) {
		return err
	} else if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    bkt.Name,
		}).Error("failed to save bucket")
		return err
	}

	defer f.Close()
	enc := gob.NewEncoder(f)
	// enc.SetIndent("", "  ")
	return enc.Encode(bkt)
}

// DeleteBucket deletes a bucket given its name
func (m *Meta) DeleteBucket(name string) error {

	if err := os.Remove(filepath.Join(m.bucketDir, name)); err != nil {
		if !os.IsNotExist(err) {
			log.WithError(err).WithFields(log.Fields{
				"subsystem": "disk",
				"bucket":    name,
			}).Error("failed to delete bucket")
		}

		return err
	}

	// @todo: delete meta links too

	if err := os.RemoveAll(filepath.Join(m.objDir, name)); err != nil {
		//we only log a warning if we failed to delete bucket objects
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Warning("failed to delete bucket objects")
	}

	return nil
}

// ListBuckets lists all buckets
func (m *Meta) ListBuckets() (map[string]*Bucket, error) {

	files, err := ioutil.ReadDir(m.bucketDir)
	if err != nil {
		return nil, err
	}
	buckets := make(map[string]*Bucket)

	for _, f := range files {
		bkt, err := m.getBucket(f.Name())
		if err != nil {
			return nil, err
		}

		buckets[bkt.Name] = bkt
	}

	return buckets, nil
}

// GetBucket returns a Bucket given its name
func (m *Meta) GetBucket(name string) (*Bucket, error) {
	return m.getBucket(name)
}

func (m *Meta) getBucket(name string) (*Bucket, error) {
	f, err := os.Open(m.bucketFileName(name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, minio.BucketNotFound{}
		}
		return nil, err
	}
	defer f.Close()

	var bkt Bucket

	if err = gob.NewDecoder(f).Decode(&bkt); err != nil {
		return nil, err
	}
	return &bkt, nil
}

// SetBucketPolicy changes bucket policy
func (m *Meta) SetBucketPolicy(name string, policy *policy.Policy) error {

	bkt, err := m.getBucket(name)
	if err != nil {
		return err
	}

	bkt.Policy = *policy
	return m.saveBucket(bkt)
}

// SetChunk saves the metadata to a file under /parts and returns the file id
func (m *Meta) SetChunk(metaData *ObjectMeta) (string, error) {

	metaBytes := new(bytes.Buffer)
	gob.NewEncoder(metaBytes).Encode(metaData)
	partID := uuid.NewV4().String()
	if err := createWriteFile(m.partFileName(partID), metaBytes.Bytes(), filePerm); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"key":       string(metaData.Key),
		}).Error("failed to write metadata")
		return "", err
	}
	return partID, nil
}

// SetObjectLink creates the object file under /objects and links it to the first part metadata file
func (m *Meta) SetObjectLink(bucket, object, partID string) error {

	objectFile := m.objectFileName(bucket, object)
	partFile := m.partFileName(partID)

	if err := os.MkdirAll(filepath.Dir(objectFile), dirPerm); err != nil {
		return err
	}
	return os.Link(partFile, objectFile)
}

func (m *Meta) partFileName(partID string) string {
	return filepath.Join(m.partDir, partID[0:2], partID)
}

func (m *Meta) objectFileName(bucket, object string) string {
	return filepath.Join(m.objDir, bucket, object)
}

func (m *Meta) bucketFileName(bucket string) string {
	return filepath.Join(m.bucketDir, bucket)
}

func (m *Meta) objectsBucket(bucket string) string {
	return filepath.Join(m.objDir, bucket)
}

func (m *Meta) uploadDirName(bucket, object string) string {
	return filepath.Join(m.uploadDir, bucket, object)
}

func (m *Meta) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	resultV2, err := m.ListObjectsV2(ctx, bucket, prefix, "", delimiter, maxKeys, false, marker)
	if err != nil {
		return result, err
	}

	result.IsTruncated = resultV2.IsTruncated
	if resultV2.IsTruncated {
		next, _ := base64.URLEncoding.DecodeString(resultV2.NextContinuationToken)
		result.NextMarker = string(next)
	}

	result.Objects = make([]minio.ObjectInfo, 0, len(resultV2.Objects))

	for _, obj := range resultV2.Objects {
		//V1 of bucket list does not include directories
		if obj.IsDir {
			continue
		}
		result.Objects = append(result.Objects, obj)
	}

	result.Prefixes = resultV2.Prefixes

	return
}

func (m *Meta) ListObjectsV2(ctx context.Context, bucket, prefix,
	continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	/*
		The next implementation is based on docs at
		https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

		we will only support delimeted '/' since we use filesystem to store
		meta structures.
	*/

	if len(delimiter) != 0 && delimiter != "/" {
		return result, fmt.Errorf("only delimeter / is supported")
	}
	encoder := base64.URLEncoding
	if len(continuationToken) != 0 {
		//this overrides startAfter which is only valid on the first call
		start, decodeErr := encoder.DecodeString(continuationToken)
		if decodeErr != nil {
			return result, fmt.Errorf("invalid continuation token: %s", decodeErr)
		}
		startAfter = string(start)
	}

	result.ContinuationToken = continuationToken
	token, err := m.scan(ctx, bucket, prefix, startAfter, len(delimiter) != 0, maxKeys, &result)
	if err == errMaxKeyReached {
		result.IsTruncated = true
		result.NextContinuationToken = encoder.EncodeToString([]byte(token))
		err = nil
	}

	return result, err
}

func (m *Meta) scan(ctx context.Context, bucket, prefix, after string, delimited bool, maxKeys int, result *minio.ListObjectsV2Info) (string, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"prefix":    prefix,
		"after":     after,
		"delimited": delimited,
		"maxKeys":   maxKeys,
	}).Debug("scan bucket")

	root := filepath.Join(m.objDir, bucket)

	var prefixed string
	if delimited {
		//so prefixed should be a directory
		prefixed = filepath.Join(root, prefix)
	} else {
		//otherwise we search the root
		prefixed = root
	}

	var last string

	stat, err := os.Stat(prefixed)
	if os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}

	if !stat.IsDir() {
		//is a file

		md, decodeErr := m.GetDecodeMeta(filepath.Join(bucket, prefix))
		if decodeErr != nil {
			return "", decodeErr
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, prefix, md))
		return "", nil
	}

	err = filepath.Walk(prefixed, func(path string, info os.FileInfo, err error) error {
		if prefixed == path {
			return nil
		}

		if err != nil {
			return errors.Wrapf(err, "failed to walk '%s'", prefixed)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if maxKeys == 0 {
			return errMaxKeyReached
		}

		name := strings.TrimLeft(strings.TrimPrefix(path, root), "/")
		if strings.Compare(name, after) <= 0 {
			//scanning until reach the "after"
			return nil
		}

		maxKeys--
		if !delimited && !strings.HasPrefix(name, prefix) {
			//match on full prefix and the full object name doesn't have this file/dir
			return nil
		}

		last = name
		if delimited && info.IsDir() {
			result.Prefixes = append(result.Prefixes, filepath.Clean(name)+"/")
			return filepath.SkipDir
		}

		if info.IsDir() {
			//flat listing (no delmeter)
			result.Objects = append(result.Objects, minio.ObjectInfo{
				Bucket: bucket,
				Name:   filepath.Clean(name) + "/",
				ETag:   name,
				IsDir:  true,
			})

			return nil
		}

		//we found a file
		// if file, get metadata of this file
		md, err := m.GetDecodeMeta(filepath.Join(bucket, name))
		if err != nil {
			return err
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, name, md))
		return nil
	})

	return last, err
}

// get the metadata and decode it
func (m *Meta) GetDecodeMeta(key string) (*ObjectMeta, error) {
	rawMd, err := m.getMetaBytes(key)
	if err != nil {
		return nil, err
	}

	var md ObjectMeta
	println("*********************************")

	dec := gob.NewDecoder(bytes.NewReader(rawMd))
	err = dec.Decode(&md)
	return &md, err
}

func (m *Meta) getMetaBytes(key string) ([]byte, error) {

	file, err := os.Open(filepath.Join(m.objDir, key))
	if os.IsNotExist(err) {
		return nil, db.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	defer file.Close()

	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return nil, err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer

	if !fi.IsDir() {
		_, err = buffer.ReadFrom(file)
	} else {
		epoch := fi.ModTime().UnixNano()
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&buffer)
		err = enc.Encode(metatypes.Metadata{
			Size:           fileMetaDirSize,
			Key:            []byte(key),
			StorageSize:    fileMetaDirSize,
			CreationEpoch:  epoch,
			LastWriteEpoch: epoch,
		})
	}
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (m *Meta) NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error) {
	// create upload ID
	uploadID := uuid.NewV4().String()

	info := MultiPartInfo{
		MultipartInfo: minio.MultipartInfo{
			UploadID:  uploadID,
			Object:    object,
			Initiated: time.Now(),
		},
		Metadata: opts.UserDefined,
	}

	// creates the dir
	uploadDir := m.uploadDirName(bucket, uploadID)

	if err := os.MkdirAll(uploadDir, dirPerm); err != nil {
		return uploadID, err
	}

	// creates meta file
	uploadMetaFile := filepath.Join(m.uploadDir, uploadMetaFile)
	f, err := os.OpenFile(uploadMetaFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"bucket":    bucket,
			"object":    object,
			"subsystem": "disk",
		}).Error("failed to initialize multipart upload")
		return "", err
	}
	defer f.Close()

	err = gob.NewEncoder(f).Encode(info)
	return uploadID, err
}
