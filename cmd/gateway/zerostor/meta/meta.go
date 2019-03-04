package meta

import (
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/utils"
	"github.com/minio/minio/pkg/policy"
	"github.com/pkg/errors"
	"github.com/satori/uuid"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
	"github.com/threefoldtech/minio/cmd/gateway/zerostor/meta"

	log "github.com/sirupsen/logrus"
)

const (
	bucketDir          = "buckets"
	objectDir          = "objects"
	blobDir            = "blobs"
	uploadDir          = "uploads"
	uploadMetaFile     = ".meta"
	contentTypeKey     = "content-type"
	contentEncodingKey = "content-encoding"
	amzStorageClass    = "x-amz-storage-class"
	nextBlobKey        = "next-blob"
	ETagKey            = "etag"
	fileMetaDirSize    = 4096 // size of dir always 4096
	// MinPartSize limit on min part size for partial upload
	MinPartSize = 5 * 1024 * 1024
)

var (
	dirPerm  = os.FileMode(0755)
	filePerm = os.FileMode(0644)
)

// Meta implements the Manager interface
type Meta struct {
	bucketDir string
	objDir    string
	blobDir   string
	uploadDir string
}

func (m *Meta) blobFile(fileID string) string {
	return filepath.Join(m.blobDir, fileID[0:2], fileID)
}

func (m *Meta) objectFile(bucket, object string) string {
	return filepath.Join(m.objDir, bucket, object)
}

func (m *Meta) bucketFileName(bucket string) string {
	return filepath.Join(m.bucketDir, bucket)
}

func (m *Meta) bucketObjectsDir(bucket string) string {
	return filepath.Join(m.objDir, bucket)
}

func (m *Meta) bucketUploadsDir(bucket string) string {
	return filepath.Join(m.uploadDir, bucket)
}

func (m *Meta) uploadDirName(bucket, uploadID string) string {
	return filepath.Join(m.bucketUploadsDir(bucket), uploadID)
}

func (m *Meta) partFileName(bucket, uploadID, partID string) string {
	return filepath.Join(m.uploadDir, bucket, uploadID, partID)
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

	// initialize blobs dir, if not exist
	if err := os.MkdirAll(m.blobDir, dirPerm); err != nil {
		return err
	}

	return nil
}

// CreateBucket creates bucket given its name
func (m *Meta) CreateBucket(name string) error {
	if exists, err := utils.Exists(m.bucketFileName(name)); err != nil {
		return err
	} else if exists {
		return minio.BucketAlreadyExists{}
	}

	// creates the actual bucket
	if err := m.createBucket(name); err != nil {
		return err
	}
	if err := os.MkdirAll(m.bucketObjectsDir(name), dirPerm); err != nil {
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

	if err := utils.RemoveFile(filepath.Join(m.bucketDir, name)); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Error("failed to delete bucket")
		return err
	}

	if err := os.RemoveAll(filepath.Join(m.objDir, name)); err != nil {
		//we only log a warning if we failed to delete bucket objects
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Warning("failed to delete bucket objects")
		return err
	}

	if err := os.RemoveAll(filepath.Join(m.uploadDir, name)); err != nil {
		//we only log a warning if we failed to delete bucket objects
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Warning("failed to delete bucket objects")
		return err
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

func (m *Meta) PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error) {
	fileID, objMeta, err := m.createBlob(metaData, false)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	if err = m.SetObjectLink(bucket, object, fileID); err != nil {
		return minio.ObjectInfo{}, err
	}

	return CreateObjectInfo(bucket, object, &objMeta), nil
}

func (m *Meta) PutObjectPart(metaData *metatypes.Metadata, bucket, uploadID string, partID int) (minio.PartInfo, error) {
	fileID, _, err := m.createBlob(metaData, true)
	if err != nil {
		return minio.PartInfo{}, err
	}

	if err = m.SetPartLink(bucket, uploadID, strconv.Itoa(partID), fileID); err != nil {
		return minio.PartInfo{}, err
	}

	return minio.PartInfo{
		PartNumber:   partID,
		LastModified: time.Unix(metaData.CreationEpoch, 0),
		ETag:         metaData.UserDefined[meta.ETagKey],
		Size:         metaData.Size,
	}, nil
}

// createBlob saves the metadata to a file under /blobs and returns the file id
func (m *Meta) createBlob(metaData *metatypes.Metadata, multiUpload bool) (string, ObjectMeta, error) {
	fileID := uuid.NewV4().String()
	objMeta := ObjectMeta{
		Metadata: metaData,
		Filename: fileID,
	}

	if !multiUpload {
		objMeta.ObjectSize = metaData.Size

		objMeta.ObjectUserMeta = metaData.UserDefined
		objMeta.ObjectModTime = metaData.LastWriteEpoch
	}
	filename := m.blobFile(fileID)

	if err := os.MkdirAll(filepath.Dir(filename), dirPerm); err != nil {
		return fileID, ObjectMeta{}, err
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fileID, ObjectMeta{}, err
	}
	defer file.Close()

	return fileID, objMeta, gob.NewEncoder(file).Encode(objMeta)
}

func (m *Meta) DeleteBlob(blob string) error {
	blobFile := m.blobFile(blob)
	if err := utils.RemoveFile(blobFile); err != nil {
		return err
	}

	blobDir := path.Dir(blobFile)
	files, err := ioutil.ReadDir(blobDir)
	if err != nil {
		return nil
	}
	if len(files) == 0 {
		return os.RemoveAll(blobDir)
	}
	return nil
}

func (m *Meta) DeleteUploadDir(bucket, uploadID string) error {
	return os.RemoveAll(m.uploadDirName(bucket, uploadID))
}

func (m *Meta) DeleteObjectFile(bucket, object string) error {
	return utils.RemoveFile(m.objectFile(bucket, object))
}

// SetObjectLink creates the object file under /objects and links it to the first blob metadata file
func (m *Meta) SetObjectLink(bucket, object, fileID string) error {
	if err := m.DeleteObjectFile(bucket, object); err != nil {
		return err

	}

	objectFile := m.objectFile(bucket, object)
	blobFile := m.blobFile(fileID)

	if err := os.MkdirAll(filepath.Dir(objectFile), dirPerm); err != nil {
		return err
	}
	return os.Symlink(blobFile, objectFile)
}

// SetPartLink
func (m *Meta) SetPartLink(bucket, uploadID, partID, fileID string) error {

	partFile := m.partFileName(bucket, uploadID, partID)
	blobFile := m.blobFile(fileID)

	return os.Symlink(blobFile, partFile)
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
		md, decodeErr := m.decodeObjMeta(m.objectFile(bucket, prefix))
		if decodeErr != nil {
			return "", decodeErr
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, prefix, &md))
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

		md, err := m.decodeObjMeta(m.objectFile(bucket, name))
		if err != nil {
			return err
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, name, &md))
		return nil
	})

	return last, err
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
	uploadMetaFile := filepath.Join(uploadDir, uploadMetaFile)
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

func (m *Meta) ListPartsInfo(bucket, uploadID string) ([]minio.PartInfo, error) {
	files, err := ioutil.ReadDir(m.uploadDirName(bucket, uploadID))
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"bucket":    bucket,
			"upload_id": uploadID,
			"subsystem": "disk",
		}).Error("failed to get multipart upload")
		return nil, err
	}
	var infos []minio.PartInfo

	// read-decode each file
	for _, file := range files {
		if file.Name() == uploadMetaFile {
			continue
		}

		metaObj, err := m.decodeObjMeta(m.partFileName(bucket, uploadID, file.Name()))

		if err != nil {
			return nil, err
		}
		partID, err := strconv.ParseInt(file.Name(), 0, 64)
		if err != nil {
			return nil, err
		}

		info := minio.PartInfo{
			PartNumber:   int(partID),
			LastModified: time.Unix(metaObj.CreationEpoch, 0),
			ETag:         metaObj.UserDefined[meta.ETagKey],
			Size:         metaObj.Size,
		}
		infos = append(infos, info)
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].PartNumber != infos[j].PartNumber { // sort by part number first
			return infos[i].PartNumber < infos[j].PartNumber
		}
		return infos[i].LastModified.Before(infos[j].LastModified)

	})

	return infos, nil
}

func (m *Meta) decodeObjMeta(file string) (ObjectMeta, error) {
	// open file
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return ObjectMeta{}, minio.ObjectNotFound{}
	} else if err != nil {
		return ObjectMeta{}, err
	}

	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return ObjectMeta{}, err
	}
	if !stat.IsDir() {
		objMeta := new(ObjectMeta)
		objMeta.Metadata = new(metatypes.Metadata)
		err = gob.NewDecoder(f).Decode(objMeta)
		return *objMeta, err
	}
	epoch := stat.ModTime().UnixNano()

	return ObjectMeta{
		ObjectSize:    fileMetaDirSize,
		ObjectModTime: epoch,
	}, nil

}

func (m *Meta) encodeObjMeta(file string, obj *ObjectMeta) error {

	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(obj)
}

func (m *Meta) CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error) {

	var totalSize int64
	var modTime int64
	var previousPart ObjectMeta
	var firstPart ObjectMeta

	// use upload parts to set NextBlob in all blobs metaData
	for ix, part := range parts {
		metaObj, err := m.decodeObjMeta(m.partFileName(bucket, uploadID, strconv.Itoa(part.PartNumber)))

		if err != nil {
			if os.IsNotExist(err) {
				return minio.ObjectInfo{}, minio.InvalidPart{}
			}
			return minio.ObjectInfo{}, err
		}

		if metaObj.Size < MinPartSize && ix != len(parts)-1 {
			//only last part is allowed to be less than 5M
			return minio.ObjectInfo{}, minio.PartTooSmall{
				PartSize:   metaObj.Size,
				PartNumber: part.PartNumber,
			}
		}

		// calculate the total size of the object
		totalSize += metaObj.Size

		// set the NextBlob and save the file except for the first part which we save later on
		if ix != 0 {
			previousPart.NextBlob = string(metaObj.Filename)
			if ix == 1 {
				firstPart = previousPart
			} else {
				m.encodeObjMeta(m.blobFile(previousPart.Filename), &previousPart)
			}
		}

		if ix == len(parts)-1 {
			m.encodeObjMeta(m.blobFile(metaObj.Filename), &metaObj)
			modTime = metaObj.LastWriteEpoch
		}
		previousPart = metaObj
	}

	f, err := os.Open(filepath.Join(m.uploadDirName(bucket, uploadID), uploadMetaFile))
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer f.Close()

	uploadInfo := MultiPartInfo{}
	err = gob.NewDecoder(f).Decode(&uploadInfo)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// put the object info in the first blob and save it
	firstPart.ObjectUserMeta = uploadInfo.Metadata
	firstPart.ObjectSize = totalSize
	firstPart.ObjectModTime = modTime
	if err := m.encodeObjMeta(m.blobFile(firstPart.Filename), &firstPart); err != nil {
		return minio.ObjectInfo{}, err
	}

	// link the object to the first blob
	if err := m.SetObjectLink(bucket, object, firstPart.Filename); err != nil {
		return minio.ObjectInfo{}, err
	}

	return CreateObjectInfo(bucket, object, &firstPart), m.DeleteUploadDir(bucket, uploadID)
}

func (m *Meta) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	md, err := m.decodeObjMeta(m.objectFile(bucket, object))
	if err != nil {
		println("error in getobject info", err.Error())
		return minio.ObjectInfo{}, err
	}
	return CreateObjectInfo(bucket, object, &md), nil
}

type metaStream struct {
	Obj   ObjectMeta
	Error error
}

func (m *Meta) StreamObjectMeta(ctx context.Context, bucket, object string) <-chan metaStream {
	c := make(chan metaStream)
	go func() {
		defer close(c)
		metaFile := m.objectFile(bucket, object)
		for true {
			println("Reading meta %s", metaFile)
			objMeta, err := m.decodeObjMeta(metaFile)

			select {
			case c <- metaStream{objMeta, err}:
				println("NextBlob", objMeta.NextBlob)
				if objMeta.NextBlob == "" {
					println("last blob...returning")
					return
				}
				metaFile = m.blobFile(objMeta.NextBlob)

			case <-ctx.Done():
				println("Done closed")
				return
			}

		}

	}()
	return c
}

func (m *Meta) ValidUpload(bucket, uploadID string) (bool, error) {
	exists, err := utils.Exists(m.uploadDirName(bucket, uploadID))
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (m *Meta) ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error) {
	files, err := ioutil.ReadDir(m.bucketUploadsDir(bucket))
	if err != nil {
		return minio.ListMultipartsInfo{}, err
	}

	info := new(minio.ListMultipartsInfo)

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		f, err := os.Open(filepath.Join(m.bucketUploadsDir(bucket), file.Name(), uploadMetaFile))
		if err != nil {
			return *info, err
		}
		defer f.Close()

		uploadInfo := MultiPartInfo{}
		err = gob.NewDecoder(f).Decode(&uploadInfo)
		if err != nil {
			return minio.ListMultipartsInfo{}, err
		}

		upload := minio.MultipartInfo{
			UploadID:     file.Name(),
			Object:       uploadInfo.Object,
			StorageClass: uploadInfo.StorageClass,
			Initiated:    uploadInfo.Initiated,
		}
		info.Uploads = append(info.Uploads, upload)

	}
	return *info, nil
}

func (m *Meta) StreamPartsMeta(ctx context.Context, bucket, uploadID string) <-chan metaStream {
	c := make(chan metaStream)

	go func() {
		defer close(c)

		files, err := ioutil.ReadDir(m.uploadDirName(bucket, uploadID))
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"bucket":    bucket,
				"upload_id": uploadID,
				"subsystem": "disk",
			}).Error("failed to get multipart upload")
			c <- metaStream{Error: err}
			return
		}

		// read-decode each file
		for _, file := range files {
			if file.Name() == uploadMetaFile {
				continue
			}

			objMeta, err := m.decodeObjMeta(m.partFileName(bucket, uploadID, file.Name()))

			select {
			case c <- metaStream{objMeta, err}:
				continue
			case <-ctx.Done():
				println("Done closed")
				return
			}
		}

	}()
	return c
}
