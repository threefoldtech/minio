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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/pkg/errors"
	"github.com/satori/uuid"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"

	log "github.com/sirupsen/logrus"
)

const (
	uploadMetaFile = ".meta"
)

// multiPartInfo represents info/metadata of a multipart upload
type multiPartInfo struct {
	minio.MultipartInfo
	Metadata map[string]string
}

// filesystemMeta implements the Manager interface
type metaManager struct {
	key string
	gcm cipher.AEAD

	store Store
}

func (m *metaManager) Close() error {
	return m.store.Close()
}

// CreateBucket creates bucket given its name
func (m *metaManager) CreateBucket(name string) error {
	b := &Bucket{
		Name:    name,
		Created: time.Now(),
		Policy:  defaultPolicy,
	}

	// creates the actual bucket
	return m.saveBucket(b, false)
}

// DeleteBucket deletes a bucket given its name
func (m *metaManager) DeleteBucket(name string) error {
	isErr := func(err error) bool {
		if os.IsNotExist(err) {
			return false
		} else if err != nil {
			return true
		}
		return false
	}

	if err := m.store.Del(NewPath(BucketCollection, "", name)); isErr(err) {
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Error("failed to delete bucket")
		return err
	}

	if err := m.store.Del(NewPath(ObjectCollection, name, "")); isErr(err) {
		//we only log a warning if we failed to delete bucket objects
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Warning("failed to delete bucket objects")
		return err
	}

	if err := m.store.Del(NewPath(UploadCollection, name, "")); isErr(err) {
		//we only log a warning if we failed to delete bucket objects
		log.WithError(err).WithFields(log.Fields{
			"subsystem": "disk",
			"bucket":    name,
		}).Warning("failed to delete bucket uploads")
		return err
	}
	return nil
}

func (m *metaManager) IsBucketEmpty(name string) (bool, error) {
	entries, err := m.store.List(NewPath(ObjectCollection, name, ""))
	if err != nil {
		return false, err
	}

	return len(entries) == 0, nil
}

// ListBuckets lists all buckets
func (m *metaManager) ListBuckets() (map[string]*Bucket, error) {

	paths, err := m.store.List(NewPath(BucketCollection, "", ""))
	if err != nil {
		return nil, err
	}
	buckets := make(map[string]*Bucket)

	for _, path := range paths {
		bkt, err := m.getBucket(path.Name)
		if err != nil {
			return nil, err
		}

		buckets[bkt.Name] = bkt
	}

	return buckets, nil
}

// GetBucket returns a Bucket given its name
func (m *metaManager) GetBucket(name string) (*Bucket, error) {
	return m.getBucket(name)
}

// SetBucketPolicy changes bucket policy
func (m *metaManager) SetBucketPolicy(name string, policy *policy.Policy) error {
	bkt, err := m.getBucket(name)
	if err != nil {
		return err
	}

	bkt.Policy = *policy
	return m.saveBucket(bkt, true)
}

func (m *metaManager) Mkdir(bucket, object string) error {
	return m.store.Set(NewPath(ObjectCollection, filepath.Join(bucket, object), ""), nil)
}

// PutObject creates metadata for an object
func (m *metaManager) PutObject(metaData *metatypes.Metadata, bucket, object string) (minio.ObjectInfo, error) {
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
func (m *metaManager) PutObjectPart(objMeta ObjectMeta, bucket, uploadID string, partID int) (minio.PartInfo, error) {
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
func (m *metaManager) DeleteBlob(blob string) error {

	if len(blob) < 2 {
		//invalid blob name, skip
		return nil
	}

	path := m.blobPath(blob)
	return m.store.Del(path)
}

// DeleteUpload deletes the temporary multipart upload dir
func (m *metaManager) DeleteUpload(bucket, uploadID string) error {
	return m.store.Del(FromPath(UploadCollection, bucket, uploadID))
}

// DeleteObject deletes an object file from a bucket
func (m *metaManager) DeleteObject(bucket, object string) error {
	//TODO: if last file is deleted from a directory the directory
	// need to be delted as well.
	return m.store.Del(FromPath(ObjectCollection, bucket, object))
}

// LinkObject creates a symlink from the object file under /objects to the first metadata blob file
func (m *metaManager) LinkObject(bucket, object, fileID string) error {
	link := FromPath(ObjectCollection, bucket, object)

	record, err := m.store.Get(link)
	if os.IsNotExist(err) {
		return m.store.Link(
			link,
			m.blobPath(fileID),
		)
	} else if err != nil {
		return errors.Wrapf(err, "failed to check object '%s'", link)
	}

	if record.Path.IsDir() {
		return minio.ObjectExistsAsDirectory{Bucket: bucket, Object: object}
	}

	return m.store.Link(
		link,
		m.blobPath(fileID),
	)
}

// LinkPart links a multipart upload part to a metadata blob file
func (m *metaManager) LinkPart(bucket, uploadID, partID, fileID string) error {
	return m.store.Link(
		NewPath(UploadCollection, filepath.Join(bucket, uploadID), partID),
		m.blobPath(fileID),
	)
}

// ListObjects lists objects in a bucket
func (m *metaManager) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
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
func (m *metaManager) ListObjectsV2(ctx context.Context, bucket, prefix,
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

func (m *metaManager) WriteObjMeta(obj *ObjectMeta) error {
	if len(obj.Filename) < 2 {
		return fmt.Errorf("invalid metadata object filename")
	}

	path := m.blobPath(obj.Filename)
	data, err := m.encode(obj)
	if err != nil {
		return err
	}

	return m.store.Set(path, data)
}

// CompleteMultipartUpload completes a multipart upload by linking all metadata blobs
func (m *metaManager) CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (minio.ObjectInfo, error) {

	var totalSize int64
	var modTime int64
	var previousPart ObjectMeta
	var firstPart ObjectMeta
	firstPart.Filename = "00000000-0000-0000-0000-000000000000"

	// use upload parts to set NextBlob in all blobs metaData
	for ix, part := range parts {
		partPath := NewPath(
			UploadCollection,
			filepath.Join(bucket, uploadID),
			fmt.Sprint(part.PartNumber),
		)

		metaObj, err := m.getObjectMeta(partPath)

		if os.IsNotExist(err) {
			return minio.ObjectInfo{}, minio.InvalidPart{}
		} else if err != nil {
			return minio.ObjectInfo{}, err
		}

		if ix == 0 {
			firstPart = metaObj
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
				blob, err = m.getObjectMeta(m.blobPath(blob.NextBlob))
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

	metaRec, err := m.store.Get(NewPath(UploadCollection, filepath.Join(bucket, uploadID), uploadMetaFile))
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	uploadInfo := multiPartInfo{}
	err = m.decode(metaRec.Data, &uploadInfo)
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
func (m *metaManager) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	md, err := m.getObjectMeta(FromPath(ObjectCollection, bucket, object))
	if os.IsNotExist(err) {
		return minio.ObjectInfo{}, minio.ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return minio.ObjectInfo{}, err
	}
	return CreateObjectInfo(bucket, object, &md), nil
}

// GetObjectInfo returns info about a bucket object
func (m *metaManager) GetObjectMeta(bucket, object string) (ObjectMeta, error) {
	return m.getObjectMeta(FromPath(ObjectCollection, bucket, object))
}

func (m *metaManager) getObjectMeta(path Path) (ObjectMeta, error) {
	record, err := m.store.Get(path)
	if err != nil {
		return ObjectMeta{}, err
	}

	if record.Path.IsDir() || len(record.Data) == 0 {
		// empty data is used for directories.

		return ObjectMeta{
			IsDir:         true,
			ObjectSize:    fileMetaDirSize,
			ObjectModTime: record.Time.UnixNano(),
		}, nil
	}

	var meta ObjectMeta
	if err := m.decode(record.Data, &meta); err != nil {
		return meta, err
	}

	return meta, nil
}

// StreamObjectMeta streams an object metadata blobs through a channel
func (m *metaManager) StreamObjectMeta(ctx context.Context, bucket, object string) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)
		metaFile := FromPath(ObjectCollection, bucket, object)
		for {
			objMeta, err := m.getObjectMeta(metaFile)
			if os.IsNotExist(err) {
				return
			} else if err != nil {
				switch err.(type) {
				case minio.ObjectNotFound:
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case c <- Stream{objMeta, err}:
				if objMeta.NextBlob == "" {
					return
				}
				metaFile = m.blobPath(objMeta.NextBlob)
			}
		}
	}()

	return c
}

// WriteMetaStream writes a stream of metadata to disk, links them, and returns the first blob
func (m *metaManager) WriteMetaStream(cb func() (*metatypes.Metadata, error), bucket, object string) (ObjectMeta, error) {
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

// StreamObjectMeta streams all blobs through a channel
func (m *metaManager) StreamBlobs(ctx context.Context) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)

		dirs, err := m.store.List(FromPath(BlobCollection))
		if err != nil {
			log.Fatal(err)
			return
		}

		for _, dir := range dirs {
			if !dir.IsDir() {
				continue
			}

			blobs, err := m.store.List(dir)
			if err != nil {
				log.WithFields(log.Fields{"path": dir}).WithError(err)
			}

			for _, blob := range blobs {
				if blob.IsDir() {
					continue
				}
				objMeta, err := m.getObjectMeta(blob)
				if err != nil {
					log.WithFields(log.Fields{"file": blob}).WithError(err)
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
func (m *metaManager) ValidUpload(bucket, uploadID string) (bool, error) {
	exists, err := m.store.Exists(FromPath(UploadCollection, bucket, uploadID))
	if err != nil {
		return false, err
	}

	return exists, nil
}

// ListMultipartUploads lists multipart uploads that are in progress
func (m *metaManager) ListMultipartUploads(bucket string) (minio.ListMultipartsInfo, error) {
	paths, err := m.store.List(FromPath(UploadCollection, bucket))
	if os.IsNotExist(err) {
		return minio.ListMultipartsInfo{}, nil
	} else if err != nil {
		return minio.ListMultipartsInfo{}, err
	}

	var info minio.ListMultipartsInfo

	for _, path := range paths {
		if !path.IsDir() {
			continue
		}
		record, err := m.store.Get(path.Join(uploadMetaFile))
		if os.IsNotExist(err) {
			return info, minio.InvalidUploadID{UploadID: path.Prefix}
		} else if err != nil {
			return info, err
		}

		var uploadInfo multiPartInfo

		if err = m.decode(record.Data, &uploadInfo); err != nil {
			return minio.ListMultipartsInfo{}, err
		}

		info.Uploads = append(info.Uploads, uploadInfo.MultipartInfo)

	}
	return info, nil
}

// StreamMultiPartsMeta streams parts metadata for a multiupload
func (m *metaManager) StreamMultiPartsMeta(ctx context.Context, bucket, uploadID string) <-chan Stream {
	c := make(chan Stream)

	go func() {
		defer close(c)

		files, err := m.store.List(FromPath(UploadCollection, bucket, uploadID))
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
			if file.Name == uploadMetaFile {
				continue
			}

			objMeta, err := m.getObjectMeta(file)

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
func (m *metaManager) NewMultipartUpload(bucket, object, uploadID string, meta map[string]string) error {
	// create upload ID
	info := multiPartInfo{
		MultipartInfo: minio.MultipartInfo{
			UploadID:  uploadID,
			Object:    object,
			Initiated: time.Now(),
		},
		Metadata: meta,
	}

	path := FromPath(UploadCollection, bucket, uploadID, uploadMetaFile)
	data, err := m.encode(info)
	if err != nil {
		return err
	}

	return m.store.Set(path, data)
}

// ListUploadParts lists multipart upload parts
func (m *metaManager) ListUploadParts(bucket, uploadID string) ([]minio.PartInfo, error) {
	files, err := m.store.List(FromPath(UploadCollection, bucket, uploadID))
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
		if file.Name == uploadMetaFile {
			continue
		}

		metaObj, err := m.getObjectMeta(file)

		if err != nil {
			return nil, err
		}

		partID, err := strconv.ParseInt(file.Name, 0, 64)
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

func (m *metaManager) blobPath(fileID string) Path {
	return NewPath(BlobCollection, fileID[0:2], fileID)
}

func (m *metaManager) initialize() error {
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

func (m *metaManager) encode(object interface{}) ([]byte, error) {
	var buffer bytes.Buffer

	if err := gob.NewEncoder(&buffer).Encode(object); err != nil {
		return nil, err
	}

	if len(m.key) != 0 {
		nonce := make([]byte, m.gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return nil, err
		}

		return m.gcm.Seal(nonce, nonce, buffer.Bytes(), nil), nil
	}

	return buffer.Bytes(), nil
}

func (m *metaManager) decode(data []byte, object interface{}) error {
	var buffer bytes.Buffer
	if len(m.key) != 0 {
		nonceSize := m.gcm.NonceSize()
		nonce, cipheredData := data[:nonceSize], data[nonceSize:]
		data, err := m.gcm.Open(nil, nonce, cipheredData, nil)
		if err != nil {
			return err
		}

		if _, err := buffer.Write(data); err != nil {
			return err
		}
	} else {
		if _, err := buffer.Write(data); err != nil {
			return err
		}
	}

	return gob.NewDecoder(&buffer).Decode(object)
}

func (m *metaManager) encodeData(w io.Writer, object interface{}) error {

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

func (m *metaManager) decodeData(r io.Reader, object interface{}) error {

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

func (m *metaManager) saveBucket(bkt *Bucket, exists bool) error {
	path := NewPath(BucketCollection, "", bkt.Name)

	if !exists {
		bucketExists, err := m.store.Exists(path)
		if err != nil {
			return err
		}

		if bucketExists {
			return minio.BucketAlreadyExists{}
		}
	}

	data, err := m.encode(bkt)
	if err != nil {
		return err
	}

	return m.store.Set(path, data)
}

func (m *metaManager) getBucket(name string) (*Bucket, error) {
	if len(name) == 0 {
		return nil, minio.BucketNotFound{Bucket: name}
	}

	path := NewPath(BucketCollection, "", name)
	data, err := m.store.Get(path)
	if os.IsNotExist(err) {
		return nil, minio.BucketNotFound{
			Bucket: name,
		}
	} else if err != nil {
		return nil, err
	}

	var bkt Bucket
	return &bkt, m.decode(data.Data, &bkt)
}

// createBlob saves the metadata to a file under /blobs and returns the file id
func (m *metaManager) createBlob(metaData *metatypes.Metadata, multiUpload bool) (ObjectMeta, error) {
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

func (m *metaManager) isNotExist(err error) bool {
	if err == nil {
		return false
	} else if os.IsNotExist(err) {
		return true
	} else if strings.HasSuffix(err.Error(), "not a directory") {
		return true
	}

	return false
}

func (m *metaManager) scanDelimited(ctx context.Context, bucket, prefix, after string, maxKeys int, result *minio.ListObjectsV2Info) (string, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"directory": prefix,
		"after":     after,
		"maxKeys":   maxKeys,
	}).Debug("scan delimited bucket")

	var paths []Path
	var err error
	scan := FromPath(ObjectCollection, bucket, prefix)
	if prefix == "" || strings.HasSuffix(prefix, "/") {
		// this is a bucket scan or a folder scan
		paths, err = m.store.Scan(scan, after, maxKeys, ScanModeDelimited)
		if err != nil {
			return after, err
		}
	} else {
		paths = []Path{
			scan,
		}
	}

	for _, path := range paths {
		after = path.Relative()

		relative, err := filepath.Rel(bucket, path.Relative())
		if err != nil {
			return "", err
		}

		if path.IsDir() {
			result.Prefixes = append(result.Prefixes, relative+"/")
			continue
		}

		md, err := m.getObjectMeta(path)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return after, err
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, relative, &md))
	}

	if len(paths) == maxKeys {
		return after, errMaxKeyReached
	}

	return after, nil
}

func (m *metaManager) scanFlat(ctx context.Context, bucket, prefix, after string, maxKeys int, result *minio.ListObjectsV2Info) (string, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"directory": prefix,
		"after":     after,
		"maxKeys":   maxKeys,
	}).Debug("scan flat bucket")

	scan := FromPath(ObjectCollection, bucket, prefix)
	paths, err := m.store.Scan(scan, after, maxKeys, ScanModeRecursive)
	if err != nil {
		return after, err
	}

	for _, path := range paths {
		after = path.Name
		relative, err := filepath.Rel(bucket, path.Relative())
		if err != nil {
			return "", err
		}

		if path.IsDir() {
			result.Objects = append(result.Objects, minio.ObjectInfo{
				Bucket: bucket,
				Name:   relative + "/",
				ETag:   relative,
				IsDir:  true,
			})
			continue
		}

		md, err := m.getObjectMeta(path)
		if err != nil {
			return after, err
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, relative, &md))
	}

	if len(paths) == maxKeys {
		return after, errMaxKeyReached
	}

	return after, nil
}

func (m *metaManager) scan(ctx context.Context, bucket, prefix, after string, delimited bool, maxKeys int, result *minio.ListObjectsV2Info) (string, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"prefix":    prefix,
		"after":     after,
		"delimited": delimited,
		"maxKeys":   maxKeys,
	}).Debug("scan bucket")

	if delimited {
		return m.scanDelimited(ctx, bucket, prefix, after, maxKeys, result)
	}

	return m.scanFlat(ctx, bucket, prefix, after, maxKeys, result)

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
		IsDir:           md.IsDir,
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
