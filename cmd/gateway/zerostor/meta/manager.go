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
	uploadMetaFile     = ".meta"
	currentVersionFile = ".current"
)

type versionInfo struct {
	ID        string
	Meta      string
	Timestamp int64
}

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

func (m *metaManager) BucketCreate(name string) error {
	b := &Bucket{
		Name:    name,
		Created: time.Now(),
		Policy:  defaultPolicy,
	}

	// creates the actual bucket
	return m.saveBucket(b, false)
}

// BucketDelete deletes a bucket given its name
func (m *metaManager) BucketDelete(name string) error {
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

func (m *metaManager) BucketIsEmpty(name string) (bool, error) {
	entries, err := m.store.List(DirPath(ObjectCollection, name))
	if err != nil {
		return false, err
	}

	return len(entries) == 0, nil
}

// ListBuckets lists all buckets
func (m *metaManager) BucketsList() (map[string]*Bucket, error) {

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

// BucketGet returns a Bucket given its name
func (m *metaManager) BucketGet(name string) (*Bucket, error) {
	return m.getBucket(name)
}

// SetBucketPolicy changes bucket policy
func (m *metaManager) BucketSetPolicy(name string, policy *policy.Policy) error {
	bkt, err := m.getBucket(name)
	if err != nil {
		return err
	}

	bkt.Policy = *policy
	return m.saveBucket(bkt, true)
}

// ObjectDel creates a new version that points to a delete marker
func (m *metaManager) ObjectDelete(bucket, object string) error {
	id, err := m.ObjectGet(bucket, object)
	if err != nil {
		return err
	}

	_, err = m.ObjectSet(id, "") // create a new empty version
	return err
}

// EnsureObject makes sure that this object exists in the metastore
// otherwise creates a new one
func (m *metaManager) ObjectEnsure(bucket, object string) (ObjectID, error) {
	path := FilePath(ObjectCollection, bucket, object)
	record, err := m.store.Get(path)
	if os.IsNotExist(err) {
		id := ObjectID(uuid.New().String())

		return id, m.store.Set(path, []byte(id))
	} else if err != nil {
		return "", err
	}

	if record.Path.IsDir() {
		return "", minio.ObjectExistsAsDirectory{Bucket: bucket, Object: object}
	}

	return ObjectID(record.Data), err
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
		result.NextMarker = resultV2.NextContinuationToken
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

func (m *metaManager) BlobSet(obj *Metadata) error {
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

func (m *metaManager) BlobDel(obj *Metadata) error {
	if len(obj.Filename) == 0 {
		return fmt.Errorf("invalid metadata object filename")
	}

	path := FilePath(BlobCollection, obj.Filename)
	return m.store.Del(path)
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
			if err := m.BlobSet(tail); err != nil {
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
	if err := m.BlobSet(head); err != nil {
		return Metadata{}, err
	}

	return *head, nil
}

// GetObjectInfo returns info about a bucket object
func (m *metaManager) ObjectGetInfo(bucket, object, version string) (info minio.ObjectInfo, err error) {
	//path := FilePath(ObjectCollection, bucket, object)
	directory, id, err := m.objectGet(bucket, object)
	if os.IsNotExist(err) {
		return info, minio.ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return info, err
	}

	info.Bucket = bucket
	info.Name = object

	if directory {
		info.IsDir = true
		return
	}

	ver, err := m.getObjectVersion(id, version)
	if err != nil {
		return info, err
	}

	info.VersionID = ver.ID

	if len(ver.Meta) == 0 {
		info.DeleteMarker = true
		info.ModTime = EpochToTimestamp(ver.Timestamp)
		return
	}

	md, err := m.getMetadata(FilePath(BlobCollection, ver.Meta))
	if err != nil {
		return info, err
	}

	fillObjectInfo(&info, &md)

	return info, nil
}

func (m *metaManager) ObjectList(ctx context.Context, bucket, prefix, after string) (<-chan ObjectListResult, error) {
	ch := make(chan ObjectListResult)

	push := func(o ObjectListResult) bool {
		select {
		case ch <- o:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(ch)

		if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
			// the prefix does not end with a "/" but it still
			// can be a directory. hence we get the object info
			info, err := m.ObjectGetInfo(bucket, prefix, "")
			if err != nil {
				push(ObjectListResult{Error: err})
				return
			}

			push(ObjectListResult{Info: info})
			return
		}

		path := FilePath(ObjectCollection, bucket, prefix)

		child, cancel := context.WithCancel(ctx)
		defer cancel()

		results, err := m.store.Scan(child, path, ScanModeDelimited)
		if err != nil {
			push(ObjectListResult{Error: err})
			return
		}

		for scan := range results {
			if scan.Error != nil {
				push(ObjectListResult{Error: err})
				return
			}

			path := scan.Path
			name, _ := filepath.Rel(bucket, path.Relative())
			if len(after) > 0 {
				if after == name {
					after = ""
				}

				continue
			}

			info, err := m.ObjectGetInfo(bucket, name, "")
			if err != nil {
				push(ObjectListResult{Error: err})
				return
			}

			if !push(ObjectListResult{Info: info}) {
				return
			}
		}
	}()

	return ch, nil
}

func (m *metaManager) ObjectGetObjectVersions(id ObjectID) ([]string, error) {
	path := DirPath(VersionCollection, string(id))
	results, err := m.store.List(path)
	if err != nil {
		return nil, err
	}

	versions := make([]string, 0, len(results))
	for _, result := range results {
		name := result.Base()
		if name == currentVersionFile {
			continue
		}

		versions = append(versions, name)
	}

	return versions, nil
}

func (m *metaManager) ObjectDeleteVersion(bucket, object, version string) error {
	id, err := m.ObjectGet(bucket, object)
	if err != nil {
		return err
	}

	versions, err := m.ObjectGetObjectVersions(id)
	if err != nil {
		return err
	}

	var match string
	for _, ver := range versions {
		if ver == version {
			match = version
			break
		}
	}

	if len(match) == 0 {
		return minio.VersionNotFound{Bucket: bucket, Object: object, VersionID: version}
	}

	if len(versions) == 1 {
		// we deleting the only remaining version
		// perminantely delete the object
		if err := m.store.Del(FilePath(ObjectCollection, bucket, object)); err != nil {
			return err
		}

		return m.store.Del(DirPath(VersionCollection, string(id)))
	}

	info, err := m.getObjectVersion(id, version)
	if err != nil {
		return err
	}

	path := FilePath(VersionCollection, string(id), info.ID)
	current, err := m.getObjectVersion(id, "")
	if current.ID != info.ID {
		// we are NOT deleting the current version
		// so we can simply delete this version object from the
		// store.
		return m.store.Del(path)
	}

	// otherwise we are deleting the LAST (current) version, in that case
	// we need to find the next suitable one.
	candidates := make([]versionInfo, 0, len(versions)-1)
	for _, ver := range versions {
		if ver == current.ID {
			continue
		}

		info, err := m.getObjectVersion(id, ver)
		if err != nil {
			return err
		}

		candidates = append(candidates, info)
	}

	sort.Slice(candidates, func(i, j int) bool {
		// sorting in reverse so the bigger is at index 0
		return candidates[i].Timestamp > candidates[j].Timestamp
	})

	// set current version to the one with the highest timestamp
	if err := m.objectSetCurrentVersion(id, candidates[0].ID); err != nil {
		return err
	}

	// finally delete the given version
	return m.store.Del(path)
}

func (m *metaManager) getMetadata(path Path) (Metadata, error) {
	var meta Metadata
	record, err := m.store.Get(path)
	if err != nil {
		return meta, err
	}

	if err := m.decode(record.Data, &meta); err != nil {
		return meta, err
	}

	return meta, nil
}

func (m *metaManager) ObjectGet(bucket, object string) (ObjectID, error) {
	directory, id, err := m.objectGet(bucket, object)
	if err != nil {
		return id, err
	}

	if directory {
		// empty data is used for directories.
		return "", minio.ObjectExistsAsDirectory{}
	}

	return id, nil
}

func (m *metaManager) objectGet(bucket, object string) (bool, ObjectID, error) {
	path := FilePath(ObjectCollection, bucket, object)
	record, err := m.store.Get(path)
	if err != nil {
		return false, "", err
	}

	if record.Path.IsDir() {
		// empty data is used for directories.
		return true, "", nil
	}

	return false, ObjectID(record.Data), nil
}

func (m *metaManager) ObjectSet(id ObjectID, meta string) (string, error) {
	version := versionInfo{
		ID:        uuid.New().String(),
		Meta:      meta,
		Timestamp: time.Now().UnixNano(),
	}

	path := FilePath(VersionCollection, string(id), version.ID)
	data, err := m.encode(version)
	if err != nil {
		return version.ID, err
	}

	if err := m.store.Set(path, data); err != nil {
		return version.ID, err
	}

	// now link current version to this version
	return version.ID, m.objectSetCurrentVersion(id, version.ID)
}

func (m *metaManager) objectSetCurrentVersion(id ObjectID, version string) error {
	path := FilePath(VersionCollection, string(id), version)
	current := FilePath(VersionCollection, string(id), currentVersionFile)

	// now link current version to this version
	return m.store.Link(current, path)
}

func (m *metaManager) getObjectVersion(id ObjectID, version string) (versionInfo, error) {
	if len(version) == 0 {
		version = currentVersionFile
	}

	path := FilePath(VersionCollection, string(id), version)
	record, err := m.store.Get(path)
	if err == os.ErrNotExist {
		return versionInfo{ID: version}, nil
	} else if err != nil {
		return versionInfo{}, err
	}

	var ver versionInfo
	if err := m.decode(record.Data, &ver); err != nil {
		return ver, err
	}

	return ver, nil
}

// getBlob returns only the first metadata part. This can
// be only a part of the full object meta, or the entire object
// meta based on object size. To get ALL meta parts that makes
// an object you need to use the MetaGetStream method.
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
func (m *metaManager) MetaGetStream(ctx context.Context, bucket, object, version string) <-chan Stream {
	c := make(chan Stream)
	go func() {
		defer close(c)
		errored := func(err error) {
			c <- Stream{Error: err}
		}
		id, err := m.ObjectGet(bucket, object)
		if err != nil {
			errored(err)
			return
		}

		ver, err := m.getObjectVersion(id, version)
		if err != nil {
			errored(err)
			return
		}

		if len(ver.Meta) == 0 {
			// a delete marker
			return
		}

		path := FilePath(BlobCollection, ver.Meta)

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
func (m *metaManager) MetaWriteStream(cb func() (*metatypes.Metadata, error)) (Metadata, error) {
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

		if err := m.BlobSet(currentMeta); err != nil {
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
		if err := m.BlobSet(currentMeta); err != nil {
			return Metadata{}, err
		}
	}

	head.ObjectSize = totalSize
	head.ObjectModTime = modTime
	head.ObjectUserMeta = head.UserDefined

	// update the the first meta part with the size and mod time
	if err := m.BlobSet(head); err != nil {
		return Metadata{}, err
	}

	return *head, nil
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
				objMeta, err := m.getMetadata(blob)
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
	if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
		// the prefix does not end with a "/" but it still
		// can be a directory. hence we get the object info
		info, err := m.ObjectGetInfo(bucket, prefix, "")
		if err != nil {
			return nil, err
		}

		if info.IsDir {
			result.Prefixes = append(result.Prefixes, prefix+"/")
		} else {
			result.Objects = append(result.Objects, info)
		}

		return nil, nil
	}

	child, cancel := context.WithCancel(ctx)
	defer cancel()

	//info := m.ObjectGetInfo(bucket, prefix)
	results, err := m.store.Scan(child, scan, ScanModeDelimited)
	if err != nil {
		return after, err
	}

	for scan := range results {
		if scan.Error != nil {
			return nil, err
		}
		path := scan.Path
		name, err := filepath.Rel(bucket, path.Relative())
		if err != nil {
			return nil, err
		}

		if path.IsDir() {
			result.Prefixes = append(result.Prefixes, name+"/")
			continue
		}

		info, err := m.ObjectGetInfo(bucket, name, "")
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"bucket": bucket,
				"object": name,
			}).Error("failed to get object info")
			continue
		}

		if info.DeleteMarker {
			continue
		}

		result.Objects = append(result.Objects, info)
	}

	return nil, nil
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

// fillObjectInfo creates minio ObjectInfo from 0-stor metadata
func fillObjectInfo(info *minio.ObjectInfo, md *Metadata) {
	const stdStorageClass = "STANDARD"
	info.StorageClass = stdStorageClass

	etag := getUserMetadataValue(ETagKey, md.ObjectUserMeta)
	if etag == "" {
		etag = info.Name
	}

	if class, ok := md.ObjectUserMeta[amzStorageClass]; ok {
		info.StorageClass = class
	}

	info.Size = md.ObjectSize
	info.ModTime = EpochToTimestamp(md.ObjectModTime)
	info.ETag = etag
	info.IsDir = md.IsDir
	info.ContentType = getUserMetadataValue(contentTypeKey, md.ObjectUserMeta)
	info.ContentEncoding = getUserMetadataValue(contentEncodingKey, md.ObjectUserMeta)

	delete(md.ObjectUserMeta, contentTypeKey)
	delete(md.ObjectUserMeta, contentEncodingKey)
	delete(md.ObjectUserMeta, amzStorageClass)
	delete(md.ObjectUserMeta, ETagKey)

	info.UserDefined = md.ObjectUserMeta
}
