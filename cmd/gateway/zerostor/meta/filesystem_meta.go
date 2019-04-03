package meta

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
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

	log "github.com/sirupsen/logrus"
)

const (
	bucketDir      = "buckets"
	objectDir      = "objects"
	blobDir        = "blobs"
	uploadDir      = "uploads"
	uploadMetaFile = ".meta"
)

var (
	dirPerm  = os.FileMode(0755)
	filePerm = os.FileMode(0644)
)

// multiPartInfo represents info/metadata of a multipart upload
type multiPartInfo struct {
	minio.MultipartInfo
	Metadata map[string]string
}

// filesystemMeta implements the Manager interface
type filesystemMeta struct {
	bucketDir string
	objDir    string
	blobDir   string
	uploadDir string
	key       string
	gcm       cipher.AEAD
}

// CreateBucket creates bucket given its name
func (m *filesystemMeta) CreateBucket(name string) error {
	b := &Bucket{
		Name:    name,
		Created: time.Now(),
		Policy:  defaultPolicy,
	}

	// creates the actual bucket
	if err := m.saveBucket(b, false); err != nil {
		return err
	}

	return os.MkdirAll(m.bucketObjectsDir(name), dirPerm)
}

// DeleteBucket deletes a bucket given its name
func (m *filesystemMeta) DeleteBucket(name string) error {

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
		}).Warning("failed to delete bucket uploads")
		return err
	}
	return nil
}

func (m *filesystemMeta) IsBucketEmpty(name string) (bool, error) {

	dir, err := os.Open(m.bucketObjectsDir(name))
	if os.IsNotExist(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	defer dir.Close()

	entries, err := dir.Readdir(1) // one entry is enough
	if err == nil || err == io.EOF {
		return len(entries) == 0, nil
	}

	return false, err
}

// ListBuckets lists all buckets
func (m *filesystemMeta) ListBuckets() (map[string]*Bucket, error) {

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
func (m *filesystemMeta) GetBucket(name string) (*Bucket, error) {
	return m.getBucket(name)
}

// SetBucketPolicy changes bucket policy
func (m *filesystemMeta) SetBucketPolicy(name string, policy *policy.Policy) error {
	bkt, err := m.getBucket(name)
	if err != nil {
		return err
	}

	bkt.Policy = *policy
	return m.saveBucket(bkt, true)
}

// PutObject creates metadata for an object
func (m *filesystemMeta) PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error) {
	objMeta, err := m.createBlob(metaData, false)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	if err = m.LinkObject(bucket, object, objMeta.Filename); err != nil {
		return minio.ObjectInfo{}, err
	}

	return CreateObjectInfo(bucket, object, &objMeta), nil
}

// PutObjectPart creates metadata for an object upload part
func (m *filesystemMeta) PutObjectPart(objMeta ObjectMeta, bucket, uploadID string, partID int) (minio.PartInfo, error) {
	if err := m.LinkPart(bucket, uploadID, strconv.Itoa(partID), objMeta.Filename); err != nil {
		return minio.PartInfo{}, err
	}

	return minio.PartInfo{
		PartNumber:   partID,
		LastModified: zstorEpochToTimestamp(objMeta.ObjectModTime),
		ETag:         objMeta.UserDefined[ETagKey],
		Size:         objMeta.Size,
	}, nil
}

// DeleteBlob deletes a metadata blob file
func (m *filesystemMeta) DeleteBlob(blob string) error {
	blobFile := m.blobFile(blob)
	if err := utils.RemoveFile(blobFile); err != nil {
		return err
	}

	blobDir := filepath.Dir(blobFile)
	files, err := ioutil.ReadDir(blobDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return os.RemoveAll(blobDir)
	}
	return nil
}

// DeleteUpload deletes the temporary multipart upload dir
func (m *filesystemMeta) DeleteUpload(bucket, uploadID string) error {
	return os.RemoveAll(m.uploadDirName(bucket, uploadID))
}

// DeleteObject deletes an object file from a bucket
func (m *filesystemMeta) DeleteObject(bucket, object string) error {
	objFile := m.objectFile(bucket, object)
	if err := utils.RemoveFile(m.objectFile(bucket, object)); err != nil && !os.IsNotExist(err) {
		return err
	}

	bucketDir := m.bucketObjectsDir(bucket)
	prefix := filepath.Dir(objFile)

	for prefix != bucketDir {
		files, err := ioutil.ReadDir(prefix)
		if os.IsNotExist(err) {
			prefix = filepath.Dir(prefix)
			continue
		} else if err != nil {
			return err
		}

		if len(files) == 0 {
			if err := os.RemoveAll(prefix); err != nil && !os.IsNotExist(err) {
				return err
			}
			prefix = filepath.Dir(prefix)
			continue
		}
		break
	}
	return nil
}

// LinkObject creates a symlink from the object file under /objects to the first metadata blob file
func (m *filesystemMeta) LinkObject(bucket, object, fileID string) error {
	if err := m.DeleteObject(bucket, object); err != nil {
		return err

	}

	objectFile := m.objectFile(bucket, object)
	objectDir := filepath.Dir(objectFile)

	blobFile := m.blobFile(fileID)

	if err := os.MkdirAll(objectDir, dirPerm); err != nil {
		return err
	}
	relBlob, err := filepath.Rel(objectDir, blobFile)
	if err != nil {
		return err
	}
	return os.Symlink(relBlob, objectFile)
}

// LinkPart links a multipart upload part to a metadata blob file
func (m *filesystemMeta) LinkPart(bucket, uploadID, partID, fileID string) error {
	partFile := m.partFileName(bucket, uploadID, partID)
	partDir := filepath.Dir(partFile)
	blobFile := m.blobFile(fileID)
	relBlob, err := filepath.Rel(partDir, blobFile)
	if err != nil {
		return err
	}
	return os.Symlink(relBlob, partFile)
}

// ListObjects lists objects in a bucket
func (m *filesystemMeta) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
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

// ListObjectsV2 lists objects in a bucket
func (m *filesystemMeta) ListObjectsV2(ctx context.Context, bucket, prefix,
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

func (m *filesystemMeta) WriteObjMeta(obj *ObjectMeta) error {
	if err := os.MkdirAll(filepath.Dir(m.blobFile(obj.Filename)), dirPerm); err != nil {
		return err
	}

	f, err := os.OpenFile(m.blobFile(obj.Filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"key":       string(obj.Key),
		}).Error("failed to write metadata")
		return err
	}
	defer f.Close()

	return m.encodeData(f, obj)
}

// CompleteMultipartUpload completes a multipart upload by linking all metadata blobs
func (m *filesystemMeta) CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error) {

	var totalSize int64
	var modTime int64
	var previousPart ObjectMeta
	var firstPart ObjectMeta

	// use upload parts to set NextBlob in all blobs metaData
	for ix, part := range parts {
		metaObj, err := m.decodeObjMeta(m.partFileName(bucket, uploadID, strconv.Itoa(part.PartNumber)))

		if ix == 0 {
			firstPart = metaObj
		}

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
		totalSize += metaObj.ObjectSize
		blob := metaObj
		for {
			if blob.NextBlob != "" {
				blob, err = m.decodeObjMeta(m.blobFile(blob.NextBlob))
				if err != nil {
					return minio.ObjectInfo{}, err
				}
				continue
			}
			break
		}

		// set the NextBlob and save the file except for the first part which we save later on
		if ix != 0 {
			previousPart.NextBlob = string(metaObj.Filename)

			// compare keys to make sure it is the first part and not partitioned blob
			if ix == 1 && string(previousPart.Key) == string(firstPart.Key) {
				firstPart = previousPart
			} else {
				m.WriteObjMeta(&previousPart)
			}
		}

		if ix == len(parts)-1 {
			modTime = metaObj.LastWriteEpoch
		}
		previousPart = blob
	}

	f, err := os.Open(filepath.Join(m.uploadDirName(bucket, uploadID), uploadMetaFile))
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	defer f.Close()

	uploadInfo := multiPartInfo{}
	err = m.decodeData(f, &uploadInfo)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// put the object info in the first blob and save it
	firstPart.ObjectUserMeta = uploadInfo.Metadata
	firstPart.ObjectSize = totalSize
	firstPart.ObjectModTime = modTime
	if err := m.WriteObjMeta(&firstPart); err != nil {
		return minio.ObjectInfo{}, err
	}

	// link the object to the first blob
	if err := m.LinkObject(bucket, object, firstPart.Filename); err != nil {
		return minio.ObjectInfo{}, err
	}

	return CreateObjectInfo(bucket, object, &firstPart), m.DeleteUpload(bucket, uploadID)
}

// GetObjectInfo returns info about a bucket object
func (m *filesystemMeta) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	md, err := m.decodeObjMeta(m.objectFile(bucket, object))
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	return CreateObjectInfo(bucket, object, &md), nil
}

// GetObjectInfo returns info about a bucket object
func (m *filesystemMeta) GetObjectMeta(bucket, object string) (ObjectMeta, error) {
	return m.decodeObjMeta(m.objectFile(bucket, object))
}

// StreamObjectMeta streams an object metadata blobs through a channel
func (m *filesystemMeta) StreamObjectMeta(ctx context.Context, bucket, object string) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)
		metaFile := m.objectFile(bucket, object)
		for {
			objMeta, err := m.decodeObjMeta(metaFile)

			select {
			case <-ctx.Done():
				return
			case c <- Stream{objMeta, err}:
				if objMeta.NextBlob == "" {
					return
				}
				metaFile = m.blobFile(objMeta.NextBlob)
			}

		}

	}()
	return c
}

// WriteMetaStream writes a stream of metadata to disk, links them, and returns the first blob
func (m *filesystemMeta) WriteMetaStream(cb func() (*metatypes.Metadata, error), bucket, object string) (ObjectMeta, error) {
	// any changes to this function need to be mirrored in the filesystem tlogger
	var totalSize int64
	var modTime int64
	var previousPart ObjectMeta
	var firstPart ObjectMeta
	var objMeta ObjectMeta
	counter := 0

	for {
		metaData, err := cb()
		if err == io.EOF {
			break
		} else if err != nil {
			return ObjectMeta{}, err
		}

		totalSize += metaData.Size
		modTime = metaData.LastWriteEpoch
		objMeta = ObjectMeta{
			Metadata: *metaData,
			Filename: uuid.NewV4().String(),
		}

		// if this is not the first iteration set the NextBlob on the previous blob and save it if it is not the first blob
		if counter > 0 {
			previousPart.NextBlob = objMeta.Filename
			if counter == 1 {
				// update the first part
				firstPart = previousPart
			} else {
				if err := m.WriteObjMeta(&previousPart); err != nil {
					return ObjectMeta{}, err
				}
			}
		} else { // if this is the first iteration, mark the first blob
			firstPart = objMeta
		}
		previousPart = objMeta
		counter++
	}

	// write the meta of the last received metadata
	if err := m.WriteObjMeta(&objMeta); err != nil {
		return ObjectMeta{}, err
	}

	firstPart.ObjectSize = totalSize
	firstPart.ObjectModTime = modTime
	firstPart.ObjectUserMeta = firstPart.UserDefined

	// update the the first meta part with the size and mod time
	if err := m.WriteObjMeta(&firstPart); err != nil {
		return ObjectMeta{}, err
	}

	return firstPart, nil
}

// StreamObjectMeta streams an object metadata blobs through a channel
func (m *filesystemMeta) StreamBlobs(ctx context.Context) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)

		f, err := os.Open(m.blobDir)
		if err != nil {
			log.Fatal(err)
			return
		}

		dirs, err := f.Readdir(-1)
		f.Close()
		if err != nil {
			log.Fatal(err)
			return
		}

		for _, dir := range dirs {
			if !dir.IsDir() {
				continue
			}
			blobDir, err := os.Open(filepath.Join(m.blobDir, dir.Name()))
			if err != nil {
				log.Fatal(err)
				return
			}
			blobs, err := blobDir.Readdir(-1)
			blobDir.Close()
			if err != nil {
				log.Fatal(err)
				return
			}

			for _, blob := range blobs {
				if !dir.IsDir() {
					continue
				}
				filename := m.blobFile(blob.Name())
				objMeta, err := m.decodeObjMeta(filename)
				if err != nil {
					log.WithFields(log.Fields{"file": filename}).WithError(err)
				}

				select {
				case c <- Stream{objMeta, err}:
				case <-ctx.Done():
					return
				}

			}

		}

	}()
	return c
}

// ValidUpload checks if an upload id is valid
func (m *filesystemMeta) ValidUpload(bucket, uploadID string) (bool, error) {
	exists, err := utils.Exists(m.uploadDirName(bucket, uploadID))
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ListMultipartUploads lists multipart uploads that are in progress
func (m *filesystemMeta) ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error) {
	files, err := ioutil.ReadDir(m.bucketUploadsDir(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return minio.ListMultipartsInfo{}, nil
		}
		return minio.ListMultipartsInfo{}, err
	}

	info := new(minio.ListMultipartsInfo)

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		f, err := os.Open(filepath.Join(m.bucketUploadsDir(bucket), file.Name(), uploadMetaFile))
		if os.IsNotExist(err) {
			return *info, minio.InvalidUploadID{UploadID: file.Name()}
		} else if err != nil {
			return *info, err
		}
		defer f.Close()

		uploadInfo := multiPartInfo{}

		if err = m.decodeData(f, &uploadInfo); err != nil {
			return minio.ListMultipartsInfo{}, err
		}
		info.Uploads = append(info.Uploads, uploadInfo.MultipartInfo)

	}
	return *info, nil
}

// StreamMultiPartsMeta streams parts metadata for a multiupload
func (m *filesystemMeta) StreamMultiPartsMeta(ctx context.Context, bucket, uploadID string) <-chan Stream {
	c := make(chan Stream)

	go func() {
		defer close(c)

		files, err := ioutil.ReadDir(m.uploadDirName(bucket, uploadID))
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"bucket":    bucket,
				"upload_id": uploadID,
				"subsystem": "disk",
			}).Error("failed to get multipart upload")
			c <- Stream{Error: err}
			return
		}

		// read-decode each file
		for _, file := range files {
			if file.Name() == uploadMetaFile {
				continue
			}

			objMeta, err := m.decodeObjMeta(m.partFileName(bucket, uploadID, file.Name()))

			select {
			case c <- Stream{objMeta, err}:
				continue
			case <-ctx.Done():
				return
			}
		}

	}()
	return c
}

// NewMultipartUpload initializes a new multipart upload
func (m *filesystemMeta) NewMultipartUpload(bucket, object string, opts minio.ObjectOptions) (string, error) {
	// create upload ID
	uploadID := uuid.NewV4().String()

	info := multiPartInfo{
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

	return uploadID, m.encodeData(f, &info)
}

// ListPartInfo lists multipart upload parts
func (m *filesystemMeta) ListUploadParts(bucket, uploadID string) ([]minio.PartInfo, error) {
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
			ETag:         metaObj.UserDefined[ETagKey],
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

func (m *filesystemMeta) blobFile(fileID string) string {
	return filepath.Join(m.blobDir, fileID[0:2], fileID)
}

func (m *filesystemMeta) objectFile(bucket, object string) string {
	return filepath.Join(m.objDir, bucket, object)
}

func (m *filesystemMeta) bucketFileName(bucket string) string {
	return filepath.Join(m.bucketDir, bucket)
}

func (m *filesystemMeta) bucketObjectsDir(bucket string) string {
	return filepath.Join(m.objDir, bucket)
}

func (m *filesystemMeta) bucketUploadsDir(bucket string) string {
	return filepath.Join(m.uploadDir, bucket)
}

func (m *filesystemMeta) uploadDirName(bucket, uploadID string) string {
	return filepath.Join(m.bucketUploadsDir(bucket), uploadID)
}

func (m *filesystemMeta) partFileName(bucket, uploadID, partID string) string {
	return filepath.Join(m.uploadDir, bucket, uploadID, partID)
}

func (m *filesystemMeta) initialize() error {
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

	if m.key != "" {
		c, err := aes.NewCipher([]byte(m.key))
		if err != nil {
			return err
		}
		gcm, err := cipher.NewGCM(c)
		if err != nil {
			return err
		}

		m.gcm = gcm
	}

	return nil
}

func (m *filesystemMeta) encodeData(w io.Writer, object interface{}) error {

	if m.key != "" {
		nonce := make([]byte, m.gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}
		var buffer bytes.Buffer
		if err := gob.NewEncoder(&buffer).Encode(object); err != nil {
			return err
		}
		_, err := w.Write(m.gcm.Seal(nonce, nonce, buffer.Bytes(), nil))
		return err

	}

	return gob.NewEncoder(w).Encode(object)
}

func (m *filesystemMeta) decodeData(r io.Reader, object interface{}) error {

	if m.key != "" {
		var cipheredData []byte
		if _, err := r.Read(cipheredData); err != nil {
			return err
		}

		nonceSize := m.gcm.NonceSize()
		nonce, cipheredData := cipheredData[:nonceSize], cipheredData[nonceSize:]
		data, err := m.gcm.Open(nil, nonce, cipheredData, nil)
		if err != nil {
			return err
		}

		var buffer bytes.Buffer
		if _, err := buffer.Write(data); err != nil {
			return err
		}
		return gob.NewDecoder(&buffer).Decode(object)
	}

	return gob.NewDecoder(r).Decode(object)
}

func (m *filesystemMeta) saveBucket(bkt *Bucket, exists bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !exists {
		flags = flags | os.O_EXCL
	}

	f, err := os.OpenFile(m.bucketFileName(bkt.Name), flags, filePerm)
	if os.IsExist(err) {
		return minio.BucketAlreadyExists{}
	} else if os.IsNotExist(err) {
		if err := os.MkdirAll(m.bucketDir, dirPerm); err != nil {
			return err
		}
		return m.saveBucket(bkt, exists)
	} else if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    bkt.Name,
		}).Error("failed to save bucket")
		return err
	}

	defer f.Close()
	return m.encodeData(f, bkt)
}

func (m *filesystemMeta) getBucket(name string) (*Bucket, error) {
	f, err := os.Open(m.bucketFileName(name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, minio.BucketNotFound{
				Bucket: name,
			}
		}
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Error("failed to get bucket")

		return nil, err
	}
	defer f.Close()

	var bkt Bucket
	return &bkt, m.decodeData(f, &bkt)
}

// createBlob saves the metadata to a file under /blobs and returns the file id
func (m *filesystemMeta) createBlob(metaData *metatypes.Metadata, multiUpload bool) (ObjectMeta, error) {
	objMeta := ObjectMeta{
		Metadata: *metaData,
		Filename: uuid.NewV4().String(),
	}

	if !multiUpload {
		objMeta.ObjectSize = metaData.Size

		objMeta.ObjectUserMeta = metaData.UserDefined
		objMeta.ObjectModTime = metaData.LastWriteEpoch
	}
	return objMeta, m.WriteObjMeta(&objMeta)
}

func (m *filesystemMeta) scan(ctx context.Context, bucket, prefix, after string, delimited bool, maxKeys int, result *minio.ListObjectsV2Info) (string, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"prefix":    prefix,
		"after":     after,
		"delimited": delimited,
		"maxKeys":   maxKeys,
	}).Debug("scan bucket")

	root := filepath.Join(m.objDir, bucket)
	os.MkdirAll(root, 0755)

	var prefixed string
	if delimited {
		//so prefixed should be a directory
		prefixed = filepath.Join(root, prefix)
	} else {
		//otherwise we search the root
		prefixed = root
	}

	var last string

	var stat os.FileInfo
	var err error
	for {
		stat, err = os.Stat(prefixed)
		if os.IsNotExist(err) {
			prefixed = filepath.Dir(prefixed)
			continue
		} else if err != nil {
			return "", err
		}
		if len(prefixed) < len(root) {
			return "", fmt.Errorf("corrupt bucket tree") // this should never happen
		}
		break
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

func (m *filesystemMeta) decodeObjMeta(file string) (ObjectMeta, error) {
	// open file
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return ObjectMeta{}, minio.ObjectNotFound{}
	} else if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			// "key":       string(key),
		}).Error("failed to get metadata")
		return ObjectMeta{}, err
	}

	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return ObjectMeta{}, err
	}
	if !stat.IsDir() {
		objMeta := new(ObjectMeta)
		objMeta.Metadata = *new(metatypes.Metadata)
		return *objMeta, m.decodeData(f, objMeta)
	}
	epoch := stat.ModTime().UnixNano()

	return ObjectMeta{
		ObjectSize:    fileMetaDirSize,
		ObjectModTime: epoch,
	}, nil

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

	info.UserDefined = md.ObjectUserMeta

	return info
}
