// +build linux

package zerostor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"sort"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/minio/minio/cmd/gateway/zerostor/meta/badger"
	"github.com/minio/minio/pkg/hash"
	"github.com/pkg/errors"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline/crypto"
	"github.com/threefoldtech/0-stor/client/datastor/zerodb"
	"github.com/threefoldtech/0-stor/client/metastor"
	"github.com/threefoldtech/0-stor/client/processing"
)

func TestZstorToObjectError(t *testing.T) {
	var (
		nonZstorErr = fmt.Errorf("Non zerostor error")
	)
	const (
		bucketName = "bucket"
		objectName = "object"
	)

	testCases := []struct {
		actualErr   error
		expectedErr error
		bucket      string
		object      string
	}{
		{nil, nil, "", ""},
		{
			errors.WithStack(nonZstorErr),
			nonZstorErr,
			"", "",
		},
		{
			errors.WithStack(metastor.ErrNotFound),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
		{
			errors.WithStack(datastor.ErrMissingKey),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
		{
			errors.WithStack(datastor.ErrMissingData),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
		{
			errors.WithStack(datastor.ErrKeyNotFound),
			minio.ObjectNotFound{Bucket: bucketName, Object: objectName},
			bucketName,
			objectName,
		},
	}

	for i, tc := range testCases {
		err := zstorToObjectErr(tc.actualErr, Operation("test"), tc.bucket, tc.object)
		if err == nil {
			if tc.expectedErr != nil {
				t.Errorf("Test %d: Expected nil, got %v", i, err)
			}
		} else if err.Error() != tc.expectedErr.Error() {
			t.Errorf("Test %d: Expected error %v, got %v", i, tc.expectedErr, err)
		}
	}
}

func TestGatewayObjectRoundTrip(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
		object    = "object"
		dataLen   = 4096
	)
	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// initialize data fixture
	var (
		etag            = minio.GenETag()
		contentType     = "application/json"
		contentEncoding = "gzip"
		data            = make([]byte, dataLen)
		val1            = "val1"
		key1            = "key1"
	)
	userMeta := map[string]string{
		"Content-Type":     contentType,
		"Content-Encoding": contentEncoding,
		key1:               val1,
		meta.ETagKey:       etag,
	}
	rand.Read(data)

	opts := minio.ObjectOptions{UserDefined: userMeta}
	// upload object

	bytesReader := bytes.NewReader(data)
	hashReader, err := hash.NewReader(bytesReader, bytesReader.Size(), "", "", bytesReader.Size(), true)
	if err != nil {
		t.Fatalf("failed to create hash reader = %v", err)

	}
	reader := minio.NewPutObjReader(hashReader, nil, nil)
	_, err = zo.PutObject(ctx, bucket, object, reader, opts)
	if err != nil {
		t.Fatalf("failed to put object = %v", err)
	}

	// get & check object data
	checkObject(ctx, t, zo, bucket, object, data)

	// check object info
	info, err := zo.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("failed to get object info: %v", err)
	}
	if info.ETag != etag {
		t.Fatalf("invalid etag value: %v, expected: %v", info.ETag, etag)
	}
	if info.ContentType != contentType {
		t.Fatalf("invalid content type: %v, expected: %v", info.ContentType, contentType)
	}
	if info.ContentEncoding != contentEncoding {
		t.Fatalf("invalid content encoding: %v, expected: %v", info.ContentEncoding, contentEncoding)
	}

	if len(info.UserDefined) != 1 {
		t.Fatalf("invalid number of user defined metadata: %v, expected: %v", len(info.UserDefined), 1)
	}

	if info.UserDefined[key1] != val1 {
		t.Fatalf("invalid meta value of %v: %v, expected: %v", key1, info.UserDefined[key1], val1)
	}

	// copy object - get & check
	destBucket := "destBucket"

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	metaMgr.CreateBucket(destBucket)

	_, err = zo.CopyObject(ctx, bucket, object, destBucket, object, info, opts, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	checkObject(ctx, t, zo, destBucket, object, data)

	// delete object
	_, err = zo.DeleteObject(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("failed to delete object:%v", err)
	}

	// make sure the object is not exist anymore
	err = zo.GetObject(ctx, bucket, object, 0, -1, bytes.NewBuffer(nil), etag, opts)
	if err == nil {
		t.Fatalf("unexpected error=nil when getting non existed object")
	}
}

func TestGatewayListObject(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
		dataLen   = 4096
		numObject = 10
	)
	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		data = make([]byte, dataLen)
	)
	err = zo.MakeBucketWithLocation(ctx, bucket, minio.BucketOptions{})
	if err != nil {
		t.Fatalf("create bucket `%v`: %v", bucket, err)
	}

	// initialize data fixture
	rand.Read(data)

	// list objects, must be empty
	listResults, err := zo.ListObjects(ctx, bucket, "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(listResults.Objects) != 0 {
		t.Fatalf("objects should be empty, got: %v objects", len(listResults.Objects))
	}

	// upload objects
	for i := 0; i < numObject; i++ {
		object := fmt.Sprintf("object_%v", i)

		bytesReader := bytes.NewReader(data)
		hashReader, err := hash.NewReader(bytesReader, bytesReader.Size(), "", "", bytesReader.Size(), true)
		if err != nil {
			t.Fatalf("failed to create hash reader = %v", err)

		}
		reader := minio.NewPutObjReader(hashReader, nil, nil)
		_, err = zo.PutObject(ctx, bucket, object, reader, minio.ObjectOptions{})
		if err != nil {
			t.Fatalf("failed to put object = %v", err)
		}
	}

	// list objects, must have all uploaded objects
	listResults, err = zo.ListObjects(ctx, bucket, "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(listResults.Objects) != numObject {
		t.Fatalf("invalid objects leng: %v, expected: %v", len(listResults.Objects), numObject)
	}

}

// Test Deleting non existent object.
// it shouldn't return error
func TestDeleteNotExistObject(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "buket"
		object    = "object"
	)
	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// make sure the object not exist
	err = zo.GetObject(ctx, bucket, object, 0, -1, bytes.NewBuffer(nil), "", minio.ObjectOptions{})
	if err == nil {
		t.Fatalf("unexpected error=nil when getting non existed object")
	}

	// delete object
	_, err = zo.DeleteObject(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("deleting non existent object should not return error, got: %v", err)
	}
}

func TestGatewayBucketRoundTrip(t *testing.T) {
	const (
		namespace = "ns"
	)
	var (
		buckets = []string{"bkt_1", "bkt_2", "bkt_3"}
	)

	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create buckets
	for _, bkt := range buckets {
		err = zo.MakeBucketWithLocation(ctx, bkt, minio.BucketOptions{})
		if err != nil {
			t.Fatalf("create bucket `%v`: %v", bkt, err)
		}
	}

	// list buckets
	{
		bucketsInfo, err := zo.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("list buckets: %v", err)
		}
		var listed []string
		for _, bi := range bucketsInfo {
			listed = append(listed, bi.Name)
		}
		if err := compareStringArrs(buckets, listed); err != nil {
			t.Fatalf("invalid ListBuckets result: %v", err)
		}
	}

	// get bucket info
	{
		info, err := zo.GetBucketInfo(ctx, buckets[0])
		if err != nil {
			t.Fatalf("GetBucket: %v", err)
		}
		if info.Name != buckets[0] {
			t.Fatalf("invalid bucket name : %v, expected: %v", info.Name, buckets[0])
		}
	}

	// delete bucket
	if err := zo.DeleteBucket(ctx, buckets[0], false); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	// make sure bucket is not exist anymore
	if _, err := zo.GetBucketInfo(ctx, buckets[0]); err == nil {
		t.Fatalf("expected to get error")
	}
}

func TestGatewayBucketPolicy(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
	)

	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = zo.MakeBucketWithLocation(ctx, bucket, minio.BucketOptions{})
	if err != nil {
		t.Fatalf("create bucket `%v`: %v", bucket, err)
	}

	// by default, BucketPolicy==None
	{
		_, err = zo.GetBucketPolicy(ctx, bucket)
		switch err.(type) {
		case minio.BucketPolicyNotFound:
		default:
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
	}
	{
		//delete bucket and make sure the policy back to none
		err = zo.DeleteBucketPolicy(ctx, bucket)
		if err != nil {
			t.Fatalf("failed to DeleteBucketPolicy: %v", err)
		}

		_, err := zo.GetBucketPolicy(ctx, bucket)
		switch err.(type) {
		case minio.BucketPolicyNotFound:
		default:
			t.Fatalf("failed to GetBucketPolicy:%v", err)
		}
	}
}

func TestMultipartUploadComplete(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
		object    = "object"
		dataLen   = 10 * 1024 * 1024
		numPart   = 2
	)

	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = zo.MakeBucketWithLocation(ctx, bucket, minio.BucketOptions{})
	if err != nil {
		t.Fatalf("create bucket `%v`: %v", bucket, err)
	}

	// init the data we want to upload
	var (
		data    = make([]byte, dataLen)
		partLen = dataLen / numPart
	)
	rand.Read(data)

	// Create Upload
	uploadID, err := zo.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload failed: %v", err)
	}

	// Upload each part
	var uploadParts []minio.PartInfo
	for i := 0; i < numPart; i++ {
		var part minio.PartInfo
		bytesReader := bytes.NewReader(data[i*partLen : (i*partLen)+partLen])
		hashReader, err := hash.NewReader(bytesReader, bytesReader.Size(), "", "", bytesReader.Size(), true)
		if err != nil {
			t.Fatalf("failed to create hash reader = %v", err)

		}
		rd := minio.NewPutObjReader(hashReader, nil, nil)
		part, err = zo.PutObjectPart(ctx, bucket, object, uploadID, i, rd, minio.ObjectOptions{})
		if err != nil {
			t.Fatalf("failed to PutObjectPart %v, err: %v", i, err)
		}
		uploadParts = append(uploadParts, part)
	}

	// Complete the upload
	var completeParts []minio.CompletePart
	for _, part := range uploadParts {
		completeParts = append(completeParts, minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	_, err = zo.CompleteMultipartUpload(ctx, bucket, object, uploadID, completeParts, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("failed to CompleteMultipartUpload:%v", err)
	}

	// check the uploaded object
	checkObject(ctx, t, zo, bucket, object, data)

	// ListMultipartUploads must return empty after the upload being completed
	listUpload, err := zo.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	// check uploadID of the listed multipart uploads
	if len(listUpload.Uploads) != 0 {
		t.Fatalf("invalid num uploads after complete: %v, expected: 0", len(listUpload.Uploads))
	}

}

func TestMultipartUploadListAbort(t *testing.T) {
	const (
		namespace  = "ns"
		bucket     = "bucket"
		dataLen    = 1000
		numPart    = 100
		numUploads = 10
	)

	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = zo.MakeBucketWithLocation(ctx, bucket, minio.BucketOptions{})
	if err != nil {
		t.Fatalf("create bucket `%v`: %v", bucket, err)
	}

	// init the data we want to upload
	var (
		datas            = make(map[string][]byte)
		partLen          = dataLen / numPart
		uploadIDs        = make(map[string]struct{})
		uploadedPartsMap = make(map[string][]minio.PartInfo)
	)

	// do upload
	for i := 0; i < numUploads; i++ {
		var (
			data     = make([]byte, dataLen)
			object   = fmt.Sprintf("object_%v", i)
			uploadID string
			part     minio.PartInfo
		)
		rand.Read(data)

		// Create Upload
		uploadID, err = zo.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{})
		if err != nil {
			t.Fatalf("NewMultipartUpload failed: %v", err)
		}
		uploadIDs[uploadID] = struct{}{}
		datas[uploadID] = data

		// Upload each part
		var parts []minio.PartInfo
		for i := 0; i < numPart; i++ {
			bytesReader := bytes.NewReader(data[i*partLen : (i*partLen)+partLen])
			hashReader, err := hash.NewReader(bytesReader, bytesReader.Size(), "", "", bytesReader.Size(), true)
			if err != nil {
				t.Fatalf("failed to create hash reader = %v", err)

			}
			rd := minio.NewPutObjReader(hashReader, nil, nil)
			part, err = zo.PutObjectPart(ctx, bucket, object, uploadID, i, rd, minio.ObjectOptions{})
			if err != nil {
				t.Fatalf("failed to PutObjectPart %v, err: %v", i, err)
			}
			parts = append(parts, part)
		}
		uploadedPartsMap[uploadID] = parts
	}

	// ListMultipartUpload
	uploads, err := zo.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	// check uploadID of the listed multipart uploads
	if len(uploads.Uploads) != numUploads {
		t.Fatalf("invalid num uploads: %v, expected: %v", len(uploads.Uploads), numUploads)
	}
	for i, upload := range uploads.Uploads {
		if _, ok := uploadIDs[upload.UploadID]; !ok {
			t.Fatalf("Invalid uploadID of part %v: %v", i, upload.UploadID)
		}
	}

	// check object parts
	for _, upload := range uploads.Uploads {
		var listPartsResult minio.ListPartsInfo
		listPartsResult, err = zo.ListObjectParts(ctx, bucket, upload.Object, upload.UploadID, 0, 1000, minio.ObjectOptions{})
		if err != nil {
			t.Fatalf("Failed to ListObjectPart of upload ID:%v, err: %v", upload.UploadID, err)
		}

		listedParts := listPartsResult.Parts
		// check the parts
		uploadedParts := uploadedPartsMap[upload.UploadID]
		if len(uploadedParts) != len(listedParts) {
			t.Fatalf("invalid number of parts of upload ID `%v`: %v, expected: %v", upload.UploadID, len(uploadedParts), len(listedParts))
		}

		for i, part := range uploadedParts {
			if part.PartNumber != listedParts[i].PartNumber {
				t.Fatalf("invalid part number of uploadID `%v`: %v, expected: %v",
					upload.UploadID, listedParts[i].PartNumber, part.PartNumber)
			}
			if part.ETag != listedParts[i].ETag {
				t.Fatalf("invalid etag of uploadID `%v`: %v, expected: %v",
					upload.UploadID, listedParts[i].ETag, part.ETag)
			}
		}
	}

	// AbortMultipartUpload
	for _, upload := range uploads.Uploads {
		err = zo.AbortMultipartUpload(ctx, bucket, upload.Object, upload.UploadID)
		if err != nil {
			t.Fatalf("failed to AbortMultipartUpload uploadID `%v`: %v", upload.UploadID, err)
		}
	}

	// ListMultipartUploads must return empty after all uploads being aborted
	uploadsAfterAbort, err := zo.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	// check uploadID of the listed multipart uploads
	if len(uploadsAfterAbort.Uploads) != 0 {
		t.Fatalf("invalid num uploads after abort: %v, expected: 0", len(uploadsAfterAbort.Uploads))
	}

}

// TestMultipartUploadComplete test multipart upload
// using CopyObjectPart API
func TestMultipartUploadCopyComplete(t *testing.T) {
	const (
		namespace = "ns"
		bucket    = "bucket"
		object    = "object"
		dataLen   = 10 * 1024 * 1024
		numPart   = 2
	)

	zo, cleanup, err := newZstorGateway(t, namespace)
	if err != nil {
		t.Fatalf("failed to create gateway:%v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = zo.MakeBucketWithLocation(ctx, bucket, minio.BucketOptions{})
	if err != nil {
		t.Fatalf("create bucket `%v`: %v", bucket, err)
	}

	// init the data we want to upload
	var (
		data        = make([]byte, dataLen)
		partLen     = dataLen / numPart
		objectsInfo []minio.ObjectInfo
		info        minio.ObjectInfo
	)
	rand.Read(data)

	// upload the parts using PutObject
	for i := 0; i < numPart; i++ {
		bytesReader := bytes.NewReader(data[i*partLen : (i*partLen)+partLen])
		hashReader, err := hash.NewReader(bytesReader, bytesReader.Size(), "", "", bytesReader.Size(), true)
		if err != nil {
			t.Fatalf("failed to create hash reader = %v", err)

		}
		rd := minio.NewPutObjReader(hashReader, nil, nil)
		objectPart := fmt.Sprintf("object_%v", i)
		info, err = zo.PutObject(ctx, bucket, objectPart, rd, minio.ObjectOptions{})
		if err != nil {
			t.Fatalf("failed to PutObjectPart %v, err: %v", i, err)
		}
		objectsInfo = append(objectsInfo, info)
	}

	// Create Upload
	uploadID, err := zo.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload failed: %v", err)
	}

	// CopyPart
	var (
		uploadedParts []minio.PartInfo
		part          minio.PartInfo
	)
	for i, info := range objectsInfo {
		part, err = zo.CopyObjectPart(ctx, bucket, info.Name, bucket, object, uploadID, i, 0, 0, info, minio.ObjectOptions{}, minio.ObjectOptions{})
		if err != nil {
			t.Fatalf("CopyObjectPart failed: %v", err)
		}
		uploadedParts = append(uploadedParts, part)
	}

	// Complete the upload
	var completeParts []minio.CompletePart
	for _, part := range uploadedParts {
		completeParts = append(completeParts, minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	_, err = zo.CompleteMultipartUpload(ctx, bucket, object, uploadID, completeParts, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("failed to CompleteMultipartUpload:%v", err)
	}

	// check the uploaded object
	checkObject(ctx, t, zo, bucket, object, data)

}

func checkObject(ctx context.Context, t *testing.T, gateway minio.ObjectLayer, bucket, object string, expected []byte) {
	buf := bytes.NewBuffer(nil)

	// get object info
	info, err := gateway.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("failed to getObjectInfo(%v,%v): %v", bucket, object, err)
	}
	if info.Bucket != bucket {
		t.Fatalf("invalid bucket: %v, expected: %v", info.Bucket, bucket)
	}
	if info.Name != object {
		t.Fatalf("invalid object name: %v, expected: %v", info.Name, object)
	}
	if int(info.Size) != len(expected) {
		t.Fatalf("invalid object info size: %v, expected: %v", info.Size, len(expected))
	}

	// check object content
	err = gateway.GetObject(ctx, bucket, object, 0, int64(len(expected)), buf, "", minio.ObjectOptions{})
	if err != nil {
		t.Fatalf("failed to GetObject: %v", err)
	}
	if len(expected) != buf.Len() {
		t.Fatalf("GetObject give invalida data length: %v, expected: %v", buf.Len(), len(expected))
	}
	if !bytes.Equal(expected, buf.Bytes()) {
		t.Fatalf("GetObject produce unexpected result")
	}

}

func newZstorGateway(t *testing.T, namespace string) (*zerostorObjects, func(), error) {
	// creates 0-stor wrapper
	zstorManager, cfg, cleanupZstor := newTestZsManager(t, namespace)
	cfg.DataStor.Pipeline.Distribution.DataShardCount = 1
	var conf config.Config
	conf.Config = cfg

	zo := &zerostorObjects{
		manager:     zstorManager,
		cfg:         conf,
		maxFileSize: maxFileSizeFromConfig(conf),
	}

	cleanups := func() {
		cleanupZstor()

	}
	return zo, cleanups, nil
}

func compareStringArrs(arr1, arr2 []string) error {
	if len(arr1) != len(arr2) {
		return fmt.Errorf("different length : %v and %v", len(arr1), len(arr2))
	}
	sort.Strings(arr1)
	sort.Strings(arr2)

	for i, elem := range arr1 {
		if elem != arr2[i] {
			return fmt.Errorf("elem %v different : `%v` and `%v`", i, elem, arr2[i])
		}
	}
	return nil
}

type testConfigMgr struct {
	client Client
	mgr    *metaManagerWrapper
	mux    sync.RWMutex
}

var _ ConfigManager = (*testConfigMgr)(nil)

func (c *testConfigMgr) GetClient() Client {
	c.mux.RLock()
	return c.client
}

func (c *testConfigMgr) GetMeta() meta.Manager {
	c.mux.RLock()
	return c.mgr
}
func (c *testConfigMgr) Reload(cfg config.Config) error {
	return nil
}
func (c *testConfigMgr) Close() error {
	return nil
}

func newTestConfigMgr(dir string, pipe pipeline.Pipeline) (ConfigManager, error) {

	// store, err := meta.NewFilesystemStore(dir)
	store, err := badger.NewBadgerSimpleStore(dir)
	if err != nil {
		return nil, err
	}

	metaMgr := meta.NewMetaManager(store, "")

	cm := &testConfigMgr{}

	m := metaManagerWrapper{
		Manager: metaMgr,
		release: func() { cm.mux.RUnlock() },
	}

	cl := storClientWrapper{
		Client:  client.NewClient(nil, pipe),
		release: func() { cm.mux.RUnlock() },
	}

	cm.mgr = &m
	cm.client = &cl

	return cm, nil
}

func testCreatePipeline(cfg config.Config) (pipeline.Pipeline, error) {
	cluster, err := zerodb.NewCluster(cfg.DataStor.Shards, cfg.Password, cfg.Namespace, nil, datastor.DefaultSpreadingType)
	if err != nil {
		return nil, err
	}

	jobs := cfg.Jobs
	if jobs == 0 {
		jobs = runtime.NumCPU()
	}

	// create data pipeline, using our datastor cluster
	return pipeline.NewPipeline(cfg.DataStor.Pipeline, cluster, jobs)

}

func TestBigStreamZDB(t *testing.T) {
	t.Skip()
	/*
		this test requires some manual setup to prepare
		some real working zdbs on the configured addresses.
	*/
	tr, err := os.Create("/tmp/trace.out")
	if err != nil {
		t.Fatal(err)
	}

	defer tr.Close()
	trace.Start(tr)
	defer func() {
		fmt.Println("stopping trace")
		trace.Stop()
	}()

	pipe, err := testCreatePipeline(config.Config{
		Jobs: 2,
		Config: client.Config{
			Password:  "password",
			Namespace: "shard",
			DataStor: client.DataStorConfig{
				Shards: []datastor.ShardConfig{
					{Address: "127.0.0.1:1201"},
					// {Address: "127.0.0.1:1202"},
					// {Address: "127.0.0.1:1203"},
				},
				Pipeline: pipeline.Config{
					BlockSize: 4 * 1024 * 1024,
					Compression: pipeline.CompressionConfig{
						Type: processing.CompressionTypeSnappy,
					},
					Hashing: pipeline.HashingConfig{
						Type: crypto.HashTypeBlake2b256,
					},
					Distribution: pipeline.ObjectDistributionConfig{
						DataShardCount:   1,
						ParityShardCount: 0,
					},
				},
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	metaDir := filepath.Join(home, ".minio-test")
	os.RemoveAll(metaDir)
	os.MkdirAll(metaDir, 0766)
	manager, err := newTestConfigMgr(metaDir, pipe)
	if err != nil {
		t.Fatal(err)
	}

	obj := &zerostorObjects{
		manager:     manager,
		maxFileSize: 1 * 1024 * 1024 * 1024,
	}

	ctx := context.Background()
	bkt := "test"
	if err := obj.MakeBucketWithLocation(ctx, bkt, minio.BucketOptions{}); err != nil {
		panic(err)
	}

	input, err := NewRandSource(200 * 1024 * 1024)

	if err != nil {
		log.Fatal(err)
	}
	// reader := strings.NewReader("hello world")
	var size int64 = 10 * 1024 * 1024 * 1024
	limited := io.LimitReader(input, size)
	reader := &statReader{Reader: limited}
	var bs int64 = 512 * 1024
	count := size / bs
	for i := int64(0); i < count; i++ {
		_, err = obj.putObject(
			ctx,
			bkt,
			fmt.Sprintf("random-%d", i),
			bs,
			io.LimitReader(reader, bs),
			minio.ObjectOptions{},
			fmt.Sprintf("part-%d", i),
		)

		if err != nil {
			t.Fatal(err)
		}
	}

	stats, err := os.Create("/tmp/minio.stat.zdb.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer stats.Close()
	reader.Dump(stats)
}

type statPoint struct {
	ts   int64
	size int
	buf  int
}

type statReader struct {
	io.Reader
	stats []statPoint
}

func (r *statReader) Read(p []byte) (int, error) {
	ts := time.Now()
	n, err := r.Reader.Read(p)
	r.stats = append(r.stats, statPoint{
		ts:   ts.UnixNano(),
		buf:  len(p),
		size: n,
	})

	return n, err
}

func (r *statReader) Dump(w io.Writer) {
	io.WriteString(w, "sample,timestamp,size\n")
	for i, pt := range r.stats {
		io.WriteString(w, fmt.Sprintf("%d,%d,%d\n", i, pt.ts, pt.size))
	}
}

func TestRandSource(t *testing.T) {
	src, err := NewRandSource(1024)
	if err != nil {
		t.Fatal(err)
	}

	small := make([]byte, 512)
	read, err := src.Read(small)
	if err != nil {
		t.Fatal(err)
	}

	if read != 512 {
		t.Error("read is ", read)
	}

	if len(small) != read {
		t.Error("read bytes is not equal to buffer size")
	}

	big := make([]byte, 1024*3)
	read, err = src.Read(big)
	if err != nil {
		t.Fatal(err)
	}

	if read != 1024*3 {
		t.Error("read is not ", 3*1024)
	}

	if len(big) != read {
		t.Error("read bytes is not equal to buffer size")
	}

	//t.Log(big)
}

type randSource struct {
	buf []byte
}

func NewRandSource(buf int) (io.Reader, error) {
	b := make([]byte, buf)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}

	return &randSource{b}, nil
}

func (r *randSource) Read(out []byte) (int, error) {
	size := len(out)
	out = out[:0]

	appendOnly := func(dst []byte, src []byte, n int) []byte {
		if len(src) < n {
			n = len(src)
		}
		return append(dst, src[:n]...)
	}

	for len(out) < size {
		p := rand.Intn(len(r.buf))

		for _, src := range [][]byte{r.buf[p:], r.buf[:p]} {
			toAdd := size - len(out)
			if toAdd == 0 {
				break
			}
			out = appendOnly(out, src, toAdd)
		}
	}

	return len(out), nil
}
