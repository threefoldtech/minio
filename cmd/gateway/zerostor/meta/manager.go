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

	"github.com/google/uuid"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/pkg/errors"
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

	paths, err := m.store.List(DirPath(BucketCollection))
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

// EnsureObject makes sure that this object exists in the metastore
// otherwise creates a new one
func (m *metaManager) ObjectEnsure(bucket, object string) (Object, error) {
	path := FilePath(ObjectCollection, bucket, object)
	record, err := m.store.Get(path)
	if os.IsNotExist(err) {
		obj := Object{
			ID: uuid.New().String(),
		}
		data, err := m.encode(obj)
		if err != nil {
			return obj, err
		}
		return obj, m.store.Set(path, data)
	} else if err != nil {
		return Object{}, err
	}

	if record.Path.IsDir() {
		return Object{}, minio.ObjectExistsAsDirectory{Bucket: bucket, Object: object}
	}

	var obj Object
	err = m.decode(record.Data, &obj)
	return obj, err
}

func (m *metaManager) Mkdir(bucket, object string) error {
	return m.store.Set(NewPath(ObjectCollection, filepath.Join(bucket, object), ""), nil)
}

// PutObjectPart creates metadata for an object upload part
func (m *metaManager) UploadPutPart(bucket, uploadID string, partID int, meta string) error {
	return m.store.Link(
		FilePath(UploadCollection, bucket, uploadID, fmt.Sprint(partID)),
		FilePath(BlobCollection, meta),
	)
}

// UploadDeletePart this one does not only delete the link to the part meta
// it also deletes all blobs in that part.
func (m *metaManager) UploadDeletePart(bucket, uploadID string, partID int) error {
	path := FilePath(UploadCollection, bucket, uploadID, fmt.Sprint(partID))
	for {
		meta, err := m.getBlob(path)
		if err != nil {
			return err
		}
		blob := FilePath(BlobCollection, meta.Filename)
		if err := m.store.Del(blob); err != nil {
			log.WithError(err).WithField("path", blob.String()).Error("failed to delete blob")
		}
		if len(meta.NextBlob) == 0 {
			break
		}
		path = FilePath(BlobCollection, meta.NextBlob)
	}

	return nil
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
func (m *metaManager) UploadDelete(bucket, uploadID string) error {
	return m.store.Del(DirPath(UploadCollection, bucket, uploadID))
}

// DeleteObject deletes an object file from a bucket
func (m *metaManager) DeleteObject(bucket, object string) error {
	//TODO: if last file is deleted from a directory the directory
	// need to be delted as well.
	return m.store.Del(FilePath(ObjectCollection, bucket, object))
}

// LinkObject creates a symlink from the object file under /objects to the first metadata blob file
func (m *metaManager) LinkObject(bucket, object, fileID string) error {
	link := FilePath(ObjectCollection, bucket, object)

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

	result.ContinuationToken = continuationToken
	token, err := m.scan(ctx, bucket, prefix, continuationToken, len(delimiter) != 0, maxKeys, &result)
	if err == errMaxKeyReached {
		result.IsTruncated = true
		result.NextContinuationToken = token
		err = nil
	}

	return result, err
}

func (m *metaManager) SetBlob(obj *Metadata) error {
	if len(obj.Filename) == 0 {
		return fmt.Errorf("invalid metadata object filename")
	}

	path := FilePath(BlobCollection, obj.Filename)
	data, err := m.encode(obj)
	if err != nil {
		return err
	}

	return m.store.Set(path, data)
}

// CompleteMultipartUpload completes a multipart upload by linking all metadata blobs
func (m *metaManager) UploadComplete(bucket, _object, uploadID string, parts []minio.CompletePart) (Metadata, error) {

	var totalSize int64
	var head *Metadata
	var tail *Metadata

	if len(parts) == 0 {
		return Metadata{}, fmt.Errorf("invalid parts count")
	}

	// use upload parts to set NextBlob in all blobs metaData
	for ix, part := range parts {
		partPath := FilePath(
			UploadCollection,
			bucket, uploadID,
			fmt.Sprint(part.PartNumber),
		)
		var err error
		next, err := m.getBlob(partPath)

		if os.IsNotExist(err) {
			return Metadata{}, minio.InvalidPart{}
		} else if err != nil {
			return Metadata{}, err
		}

		if ix == 0 {
			head = next
		}

		totalSize += next.Size

		if tail != nil {
			// if tail has a value, we need to chain it to this blob
			tail.NextBlob = next.Filename
			if err := m.SetBlob(tail); err != nil {
				return Metadata{}, err
			}
		}

		// forward to new tail
		tail = next
		for len(tail.NextBlob) > 0 {
			path := FilePath(BlobCollection, tail.NextBlob)
			tail, err = m.getBlob(path)
			if err != nil {
				return Metadata{}, err
			}
		}
	}

	// verification (debug)
	test := head
	for {
		if len(test.NextBlob) == 0 {
			break
		}

		var err error
		path := FilePath(BlobCollection, test.NextBlob)
		test, err = m.getBlob(path)
		if err != nil {
			return Metadata{}, err
		}
	}

	metaRec, err := m.store.Get(FilePath(UploadCollection, bucket, uploadID, uploadMetaFile))
	if err != nil {
		return Metadata{}, err
	}

	var uploadInfo minio.MultipartInfo
	err = m.decode(metaRec.Data, &uploadInfo)
	if err != nil {
		return Metadata{}, err
	}

	// put the object info in the first blob and save it
	head.ObjectUserMeta = uploadInfo.UserDefined
	head.ObjectSize = totalSize
	if err := m.SetBlob(head); err != nil {
		return Metadata{}, err
	}

	return *head, nil
}

// GetObjectInfo returns info about a bucket object
func (m *metaManager) GetObjectInfo(bucket, object string) (minio.ObjectInfo, error) {
	md, err := m.getMeta(FilePath(ObjectCollection, bucket, object))
	if os.IsNotExist(err) {
		return minio.ObjectInfo{}, minio.ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return minio.ObjectInfo{}, err
	}
	return CreateObjectInfo(bucket, object, &md), nil
}

// GetObjectInfo returns info about a bucket object
func (m *metaManager) GetObjectMeta(bucket, object string) (Metadata, error) {
	return m.getMeta(FilePath(ObjectCollection, bucket, object))
}

func (m *metaManager) getObjectVersion(objectID, version string) (Metadata, error) {
	path := FilePath(VersionCollection, objectID, version)
	record, err := m.store.Get(path)
	if err != nil {
		return Metadata{}, err
	}

	var meta Metadata
	if err := m.decode(record.Data, &meta); err != nil {
		return meta, err
	}

	return meta, nil
}

// getMeta returns only the first metadata part. This can
// be only a part of the full object meta, or the entire object
// meta based on object size. To get ALL meta parts that makes
// an object you need to use the GetMetaStream method.
// DEPRECATED
func (m *metaManager) getMeta(path Path) (Metadata, error) {
	record, err := m.store.Get(path)
	if err != nil {
		return Metadata{}, err
	}

	if record.Path.IsDir() || len(record.Data) == 0 {
		// empty data is used for directories.

		return Metadata{
			IsDir:         true,
			ObjectSize:    fileMetaDirSize,
			ObjectModTime: record.Time.UnixNano(),
		}, nil
	}

	var obj Object
	if err := m.decode(record.Data, &obj); err != nil {
		return Metadata{}, err
	}

	return m.getObjectVersion(obj.ID, obj.Version)
}

func (m *metaManager) ObjectGet(bucket, object string) (Object, error) {
	path := FilePath(ObjectCollection, bucket, object)
	record, err := m.store.Get(path)
	if err != nil {
		return Object{}, err
	}

	if record.Path.IsDir() || len(record.Data) == 0 {
		// empty data is used for directories.

		return Object{}, os.ErrNotExist
	}

	var obj Object
	if err := m.decode(record.Data, &obj); err != nil {
		return obj, err
	}

	return obj, err
}

// getBlob returns only the first metadata part. This can
// be only a part of the full object meta, or the entire object
// meta based on object size. To get ALL meta parts that makes
// an object you need to use the GetMetaStream method.
func (m *metaManager) getBlob(path Path) (*Metadata, error) {
	record, err := m.store.Get(path)
	if err != nil {
		return nil, err
	}

	var obj Metadata
	if err := m.decode(record.Data, &obj); err != nil {
		return nil, err
	}

	return &obj, nil
}

// GetMetaStream streams an object metadata blobs through a channel
func (m *metaManager) GetMetaStream(ctx context.Context, bucket, object string) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)
		obj, err := m.ObjectGet(bucket, object)
		if err != nil {

			c <- Stream{Error: err}
			return
		}

		path := FilePath(VersionCollection, obj.ID, obj.Version)

		for {
			objMeta, err := m.getBlob(path)
			if err != nil {
				c <- Stream{Error: err}
				return
			}

			select {
			case <-ctx.Done():
				return
			case c <- Stream{*objMeta, err}:
				if objMeta.NextBlob == "" {
					return
				}
				path = FilePath(BlobCollection, objMeta.NextBlob)
			}
		}
	}()

	return c
}

// WriteMetaStream writes a stream of metadata to disk, links them, and returns the first blob
func (m *metaManager) WriteMetaStream(cb func() (*metatypes.Metadata, error)) (Metadata, error) {
	// any changes to this function need to be mirrored in the filesystem tlogger
	var totalSize int64
	var modTime int64
	var head *Metadata
	var currentMeta *Metadata
	counter := 0

	current := uuid.New().String()
	next := uuid.New().String()

	for {
		metaData, err := cb()
		if err == io.EOF {
			break
		} else if err != nil {
			return Metadata{}, err
		}

		totalSize += metaData.Size
		modTime = metaData.LastWriteEpoch
		currentMeta = new(Metadata)
		currentMeta.Metadata = *metaData
		currentMeta.Filename = current
		currentMeta.NextBlob = next

		if err := m.SetBlob(currentMeta); err != nil {
			return Metadata{}, err
		}

		current = next
		next = uuid.New().String()

		if counter == 0 {
			head = currentMeta
		}

		if err == io.EOF {
			break
		}

		counter++
	}

	// terminate current blob
	// this stream actually has multiple objects
	// it means we need to terminate the end part
	currentMeta.NextBlob = ""

	if currentMeta != head {
		// write the meta of the last received metadata
		if err := m.SetBlob(currentMeta); err != nil {
			return Metadata{}, err
		}
	}

	head.ObjectSize = totalSize
	head.ObjectModTime = modTime
	head.ObjectUserMeta = head.UserDefined

	// update the the first meta part with the size and mod time
	if err := m.SetBlob(head); err != nil {
		return Metadata{}, err
	}

	return *head, nil
}

func (m *metaManager) CreateVersion(objectID string, meta string) (string, error) {
	// this will store a new version under the object ID
	version := fmt.Sprintf("%x", time.Now().UnixNano())
	link := FilePath(VersionCollection, objectID, version)

	return version, m.store.Link(link, FilePath(BlobCollection, meta))
}

func (m *metaManager) ObjectSet(bucket, object string, obj Object) error {
	path := FilePath(ObjectCollection, bucket, object)
	data, err := m.encode(obj)
	if err != nil {
		return err
	}

	return m.store.Set(path, data)
}

// StreamObjectMeta streams all blobs through a channel
func (m *metaManager) StreamBlobs(ctx context.Context) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)

		dirs, err := m.store.List(FilePath(BlobCollection))
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
				objMeta, err := m.getMeta(blob)
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

// UploadExists checks if an upload id is valid
func (m *metaManager) UploadExists(bucket, uploadID string) (bool, error) {
	exists, err := m.store.Exists(FilePath(UploadCollection, bucket, uploadID))
	if err != nil {
		return false, err
	}

	return exists, nil
}

// ListMultipartUploads lists multipart uploads that are in progress
func (m *metaManager) UploadList(bucket string) (minio.ListMultipartsInfo, error) {
	paths, err := m.store.List(DirPath(UploadCollection, bucket))
	if os.IsNotExist(err) {
		return minio.ListMultipartsInfo{}, nil
	} else if err != nil {
		return minio.ListMultipartsInfo{}, err
	}

	log.WithFields(log.Fields{
		"paths":  paths,
		"bucket": bucket,
	}).Debug("all upload paths")

	var info minio.ListMultipartsInfo

	for _, path := range paths {
		if path.IsDir() {
			continue
		}

		if path.Base() != uploadMetaFile {
			continue
		}

		uploadID := filepath.Base(path.Prefix)
		record, err := m.store.Get(path)
		if os.IsNotExist(err) {
			return info, minio.InvalidUploadID{UploadID: uploadID}
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

		files, err := m.store.List(FilePath(UploadCollection, bucket, uploadID))
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

			objMeta, err := m.getMeta(file)

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
func (m *metaManager) UploadCreate(bucket, object string, meta map[string]string) (string, error) {
	uploadID := uuid.New().String()
	// create upload ID
	info := minio.MultipartInfo{
		UploadID:    uploadID,
		Object:      object,
		Initiated:   time.Now(),
		UserDefined: meta,
	}

	path := FilePath(UploadCollection, bucket, uploadID, uploadMetaFile)
	data, err := m.encode(info)
	if err != nil {
		return uploadID, err
	}

	return uploadID, m.store.Set(path, data)
}

func (m *metaManager) UploadGet(bucket, uploadID string) (info minio.MultipartInfo, err error) {
	path := FilePath(UploadCollection, bucket, uploadID, uploadMetaFile)
	record, err := m.store.Get(path)
	if err != nil {
		return info, err
	}

	if err := m.decode(record.Data, &info); err != nil {
		return info, err
	}

	return info, nil
}

// ListUploadParts lists multipart upload parts
func (m *metaManager) UploadListParts(bucket, uploadID string) ([]minio.PartInfo, error) {
	files, err := m.store.List(FilePath(UploadCollection, bucket, uploadID))
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

		metaObj, err := m.getBlob(file)

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
func (m *metaManager) createBlob(metaData *metatypes.Metadata, multiUpload bool) (Metadata, error) {
	objMeta := Metadata{
		Metadata: *metaData,
		Filename: uuid.New().String(),
	}

	if !multiUpload {
		objMeta.ObjectSize = metaData.Size

		objMeta.ObjectUserMeta = metaData.UserDefined
		objMeta.ObjectModTime = metaData.LastWriteEpoch
	}
	return objMeta, m.SetBlob(&objMeta)
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

func (m *metaManager) scanDelimited(ctx context.Context, bucket, prefix string, after []byte, maxKeys int, result *minio.ListObjectsV2Info) ([]byte, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"directory": prefix,
		"after":     after,
		"maxKeys":   maxKeys,
	}).Debug("scan delimited bucket")

	scan := FilePath(ObjectCollection, bucket, prefix)
	results, err := m.store.Scan(scan, after, maxKeys, ScanModeDelimited)
	if err != nil {
		return after, err
	}

	for _, path := range results.Results {
		relative, err := filepath.Rel(bucket, path.Relative())
		if err != nil {
			return nil, err
		}

		if path.IsDir() {
			result.Prefixes = append(result.Prefixes, relative+"/")
			continue
		}

		md, err := m.getMeta(path)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return after, err
		}

		result.Objects = append(result.Objects, CreateObjectInfo(bucket, relative, &md))
	}

	if results.Truncated {
		return results.After, errMaxKeyReached
	}

	return results.After, nil
}

func (m *metaManager) scan(ctx context.Context, bucket, prefix, after string, delimited bool, maxKeys int, result *minio.ListObjectsV2Info) (string, error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"prefix":    prefix,
		"after":     after,
		"delimited": delimited,
		"maxKeys":   maxKeys,
	}).Debug("scan bucket")

	if !delimited {
		return "", minio.NotImplemented{}
	}

	marker, err := base64.URLEncoding.DecodeString(after)
	if err != nil {
		return "", errors.Wrap(err, "failed to process the 'after' delimeter")
	}

	marker, err = m.scanDelimited(ctx, bucket, prefix, marker, maxKeys, result)
	if err != nil {
		return "", err
	}

	return base64.URLEncoding.EncodeToString(marker), nil
}

// CreateObjectInfo creates minio ObjectInfo from 0-stor metadata
func CreateObjectInfo(bucket, object string, md *Metadata) minio.ObjectInfo {
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
		ModTime:         EpochToTimestamp(md.ObjectModTime),
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
