// +build linux

package zerostor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/satori/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/metastor"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"

	"github.com/minio/minio/pkg/auth"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
)

const (
	zerostorBackend         = "zerostor"
	minioZstorConfigFileVar = "MINIO_ZEROSTOR_CONFIG_FILE"
	minioZstorMetaDirVar    = "MINIO_ZEROSTOR_META_DIR"
	minioZstorMetaPrivKey   = "MINIO_ZEROSTOR_META_PRIVKEY"
	minioZstorDebug         = "MINIO_ZEROSTOR_DEBUG"
	defaultNamespaceMaxSize = 10e14   // default max size =  1PB
	metaMaxSize             = 7340032 // max size allowed for meta (7M)
)

var (
	debugFlag = false
)

func init() {
	const zerostorGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Access key of 0-stor storage.
     MINIO_SECRET_KEY: Secret key of 0-stor storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  UPDATE:
     MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".

  ` + minioZstorConfigFileVar + `  Zerostor config file(default : $MINIO_CONFIG_DIR/zerostor.yaml)
  ` + minioZstorMetaDirVar + `     Zerostor metadata directory(default : $MINIO_CONFIG_DIR/zerostor_meta)
  ` + minioZstorMetaPrivKey + ` Zerostor metadata private key(default : ""). Metadata won't be encrypted if the key is not provided
  ` + minioZstorDebug + `        Zerostor debug flag. Set to "1" to enable debugging (default : 0)

EXAMPLES:
  1. Start minio gateway server for 0-stor Storage backend.
      $ export MINIO_ACCESS_KEY=zerostoraccountname
      $ export MINIO_SECRET_KEY=zerostoraccountkey
      $ {{.HelpName}}

`
	const zerostorRepairTemplate = `NAME:
{{.HelpName}} - {{.Usage}}

USAGE:
{{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
{{range .VisibleFlags}}{{.}}
{{end}}{{end}}

ENVIRONMENT VARIABLES:
` + minioZstorConfigFileVar + `  Zerostor config file(default : $MINIO_CONFIG_DIR/zerostor.yaml)
` + minioZstorMetaDirVar + `     Zerostor metadata directory(default : $MINIO_CONFIG_DIR/zerostor_meta)
` + minioZstorMetaPrivKey + ` Zerostor metadata private key(default : ""). Metadata won't be encrypted if the key is not provided
` + minioZstorDebug + `        Zerostor debug flag. Set to "1" to enable debugging (default : 0)

`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               zerostorBackend,
		Usage:              "zero-os 0-stor.",
		Action:             zerostorGatewayMain,
		CustomHelpTemplate: zerostorGatewayTemplate,
	})

	debugFlag = os.Getenv("MINIO_ZEROSTOR_DEBUG") == "1"
}

// Handler for 'minio gateway zerostor' command line.
func zerostorGatewayMain(ctx *cli.Context) {
	// config file
	confFile := os.Getenv(minioZstorConfigFileVar)
	if confFile == "" {
		confFile = filepath.Join(ctx.String("config-dir"), "zerostor.yaml")
	}

	if os.Getenv(minioZstorDebug) == "1" {
		log.SetLevel(log.DebugLevel)
	}

	// meta dir
	metaDir := os.Getenv(minioZstorMetaDirVar)
	if metaDir == "" {
		metaDir = filepath.Join(ctx.String("config-dir"), "zerostor_meta")
	}
	log.Info("Meta directory: ", metaDir)
	minio.StartGateway(ctx, &Zerostor{
		confFile:    confFile,
		metaDir:     metaDir,
		metaPrivKey: os.Getenv(minioZstorMetaPrivKey),
	})
}

// Zerostor implements minio.Gateway interface
type Zerostor struct {
	confFile    string
	metaDir     string
	metaPrivKey string
}

// Name implements minio.Gateway.Name interface
func (z *Zerostor) Name() string {
	return zerostorBackend
}

// Production implements minio.Gateway.Production interface
func (z *Zerostor) Production() bool {
	return false
}

// NewGatewayLayer implements minio.Gateway.NewGatewayLayer interface
func (z *Zerostor) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	// check options
	log.Println("zerostor config file = ", z.confFile)
	log.Println("debugging flag: ", debugFlag)
	log.Println("metadata encrypted: ", z.metaPrivKey != "")

	cfg, err := config.Load(z.confFile)
	if err != nil {
		return nil, err
	}
	// creates 0-stor  wrapper
	zsManager, err := NewConfigManager(cfg, z.metaDir, z.metaPrivKey)
	if err != nil {
		log.Error("failed to creates zstor client: ", err.Error())
		return nil, err
	}

	zo := &zerostorObjects{
		manager:     zsManager,
		cfg:         cfg,
		maxFileSize: maxFileSizeFromConfig(cfg),
	}

	healer := NewHealerAPI(cfg.Minio.Healer.Listen, zsManager, zo.isReadOnly)
	go zo.handleConfigReload(z.confFile)
	go healer.Start()

	return zo, nil
}

type zerostorObjects struct {
	minio.GatewayUnsupported
	manager     ConfigManager
	cfg         config.Config
	maxFileSize int64
}

func (zo *zerostorObjects) isReadOnly() bool {
	return zo.cfg.Minio.Master != nil
}

func (zo *zerostorObjects) handleConfigReload(confFile string) {
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGHUP)

	for {
		<-sigCh
		log.Println("Got SIGHUP:reload the config")
		cfg, err := config.Load(confFile)
		if err != nil {
			log.Println("Failed to reload the config file")
			continue
		}
		zo.cfg = cfg
		zo.maxFileSize = maxFileSizeFromConfig(cfg)
		zo.manager.Reload(cfg)
	}
}

// GetBucketInfo implements minio.ObjectLayer.GetBucketInfo interface
func (zo *zerostorObjects) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
	}).Debug("GetBucketInfo")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	bkt, err := metaMgr.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketInfo"), bucket)
		return bucketInfo, err
	}

	bucketInfo.Name = bucket
	bucketInfo.Created = bkt.Created
	return bucketInfo, err
}

// DeleteBucket implements minio.ObjectLayer.DeleteBucket interface
func (zo *zerostorObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) (err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
	}).Debug("DeleteBucket")

	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}
	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	empty, err := metaMgr.IsBucketEmpty(bucket)
	if err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucket"), bucket)
	}

	if !empty {
		return minio.BucketNotEmpty{}
	}

	err = metaMgr.DeleteBucket(bucket)
	return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucket"), bucket)

}

// ListBuckets implements minio.ObjectLayer.ListBuckets interface
func (zo *zerostorObjects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	log.Debug("ListBuckets")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	var buckets []minio.BucketInfo
	bucketsList, err := metaMgr.ListBuckets()
	if err != nil {
		return nil, zstorToObjectErr(errors.WithStack(err), Operation("ListBuckets"))

	}

	for _, bkt := range bucketsList {
		buckets = append(buckets, minio.BucketInfo{
			Name:    bkt.Name,
			Created: bkt.Created,
		})
	}
	return buckets, nil
}

// MakeBucketWithLocation implements minio.ObjectLayer.MakeBucketWithLocation interface

func (zo *zerostorObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	log.WithFields(log.Fields{
		"bucket":   bucket,
		"location": opts.Location,
	}).Debug("MakeBucketWithLocation")

	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	err := metaMgr.CreateBucket(bucket)
	return zstorToObjectErr(errors.WithStack(err), Operation("MakeBucketWithLocation"), bucket)
}

// GetBucketPolicy implements minio.ObjectLayer.GetBucketPolicy interface
func (zo *zerostorObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
	}).Debug("GetBucketPolicy")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	bkt, err := metaMgr.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketPolicy"), bucket)
		return nil, err
	}

	if len(bkt.Policy.Statements) == 0 {
		return nil, minio.BucketPolicyNotFound{Bucket: bucket}
	}

	return &bkt.Policy, nil
}

// SetBucketPolicy implements minio.ObjectLayer.SetBucketPolicy
func (zo *zerostorObjects) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	log.WithFields(log.Fields{
		"bucket": bucket,
		"policy": policy,
	}).Debug("SetBucketPolicy")

	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}
	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	err := metaMgr.SetBucketPolicy(bucket, policy)
	return zstorToObjectErr(errors.WithStack(err), Operation("SetBucketPolicy"), bucket)

}

func (zo *zerostorObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	log.WithFields(log.Fields{
		"bucket": bucket,
	}).Debug("DeleteBucketPolicy")

	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}
	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	err := metaMgr.SetBucketPolicy(bucket, &policy.Policy{Version: policy.DefaultVersion})
	return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucketPolicy"), bucket)
}

func (zo *zerostorObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (results []minio.DeletedObject, errors []error) {
	log.WithFields(log.Fields{
		"bucket":  bucket,
		"objects": objects,
	}).Debug("DeleteObjects")

	for _, object := range objects {
		info, err := zo.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if err != nil {
			errors = append(errors, err)
		}

		results = append(results, minio.DeletedObject{
			ObjectName: info.Name,
			VersionID:  info.VersionID,
		})
	}

	return
}

func (zo *zerostorObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
		"object": object,
	}).Debug("DeleteObject")

	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}

	zstor := zo.manager.GetClient()
	defer zstor.Close()

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	md, err := metaMgr.GetObjectMeta(bucket, object)
	if os.IsNotExist(err) {
		return info, nil
	}

	info = meta.CreateObjectInfo(bucket, object, &md)

	if err != nil {
		return info, zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
	}

	c := metaMgr.StreamObjectMeta(ctx, bucket, object)

	for r := range c {
		if r.Error != nil {
			return info, zstorToObjectErr(errors.WithStack(r.Error), Operation("DeleteObject"), bucket, object)
		}
		if err := zstor.Delete(r.Obj.Metadata); err != nil && err.Error() != "no chunks given to delete" {
			return info, zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
		}
		if err := metaMgr.DeleteBlob(r.Obj.Filename); err != nil {
			return info, zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
		}
		if r.Obj.NextBlob != "" {
			if err := metaMgr.LinkObject(bucket, object, r.Obj.NextBlob); err != nil {
				return info, zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
			}
		}
	}

	err = metaMgr.DeleteObject(bucket, object)
	return info, zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
}

func (zo *zerostorObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	log.WithFields(log.Fields{
		"src-bucket":  srcBucket,
		"src-object":  srcObject,
		"dest-bucket": destBucket,
		"dest-object": destObject,
		"src-info":    srcInfo,
		"src-opts":    srcOpts,
		"dst-opts":    dstOpts,
	}).Debug("CopyObject")

	if zo.isReadOnly() {
		return objInfo, ErrReadOnlyZeroStor
	}

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	srcObjMeta, err := metaMgr.GetObjectMeta(srcBucket, srcObject)
	newMeta := make(map[string]string)
	for k, v := range srcObjMeta.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			continue
		}
		newMeta[k] = v
	}

	for k, v := range srcInfo.UserDefined {
		newMeta[strings.ToLower(k)] = v
	}

	storRd, storWr := io.Pipe()
	defer storRd.Close()
	getCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		storWr.CloseWithError(zo.GetObject(getCtx, srcBucket, srcObject, 0, srcInfo.Size, storWr, "", srcOpts))
	}()

	hashReader, err := hash.NewReader(storRd, srcInfo.Size, "", "", srcInfo.Size, true)
	if err != nil {
		return objInfo, zstorToObjectErr(errors.WithStack(err), Operation("CopyObject"), destBucket, destObject)
	}

	reader := minio.NewPutObjReader(hashReader, nil, nil)

	objMeta, err := zo.putObject(ctx, destBucket, destObject, reader.Size(), reader.Reader, minio.ObjectOptions{UserDefined: newMeta}, "")
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("CopyObject"), destBucket, destObject)
		return objInfo, err
	}

	if err := metaMgr.LinkObject(destBucket, destObject, objMeta.Filename); err != nil {
		return objInfo, err
	}

	return zo.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
}

func (zo *zerostorObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
		"object": object,
	}).Debug("GetObjectNInfo")

	var objInfo minio.ObjectInfo
	objInfo, err = zo.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := zo.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, minio.ObjectOptions{}, pipeCloser)
}

func (zo *zerostorObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64,
	writer io.Writer, etag string, opts minio.ObjectOptions) error {
	log.WithFields(log.Fields{
		"bucket":      bucket,
		"object":      object,
		"startOffset": startOffset,
		"length":      length,
	}).Debug("GetObject")

	if length < 0 && length != -1 {
		return minio.ErrorRespToObjectError(minio.InvalidRange{}, bucket, object)
	}

	if length == 0 {
		return nil
	}

	zstor := zo.manager.GetClient()
	defer zstor.Close()

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	objMeta, err := metaMgr.GetObjectInfo(bucket, object)
	if err != nil {
		return err
	}
	if length == -1 {
		length = objMeta.Size
	}

	c := metaMgr.StreamObjectMeta(ctx, bucket, object)
	for r := range c {
		if r.Error != nil {
			return r.Error
		}

		// we haven't reached the correct part to start from
		if startOffset > r.Obj.Size {
			startOffset -= r.Obj.Size
			continue
		}

		readLength := length
		if readLength > r.Obj.Size {
			readLength = r.Obj.Size
			length -= readLength
		}

		if err := zstor.Read(&r.Obj.Metadata, writer, startOffset, readLength); err != nil {
			return err
		}
		startOffset = 0
		if length == 0 {
			break
		}
	}
	return nil
}

func (zo *zerostorObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
		"object": object,
	}).Debug("GetObjectInfo")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	objInfo, err = metaMgr.GetObjectInfo(bucket, object)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("GetObjectInfo"), bucket, object)
	}

	return objInfo, err
}

func (zo *zerostorObjects) GetBucketObjectLockConfig(context.Context, string) (*objectlock.Config, error) {
	return nil, minio.BucketObjectLockConfigNotFound{}

}

func (zo *zerostorObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string,
	maxKeys int) (result minio.ListObjectsInfo, err error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxKeys":   maxKeys,
	}).Debug("ListObjects")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	_, err = metaMgr.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketInfo"), bucket)
		return result, err
	}

	// get objects
	result, err = metaMgr.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("ListObjects"), bucket)
		return
	}
	return result, nil
}

// ListObjectsV2 implementation
func (zo *zerostorObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	log.WithFields(log.Fields{
		"bucket":            bucket,
		"prefix":            prefix,
		"continuationToken": continuationToken,
		"delimiter":         delimiter,
		"maxKeys":           maxKeys,
		"fetchOwner":        fetchOwner,
		"startAfter":        startAfter,
	}).Debug("ListObjectsV2")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	_, err = metaMgr.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketInfo"), bucket)
		return result, err
	}

	// get objects
	result, err = metaMgr.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("ListObjectsV2"), bucket)
		return
	}
	return result, nil
}

// PutObject implements ObjectLayer.PutObject
func (zo *zerostorObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"metadata": opts.UserDefined,
	}).Debug("PutObject")

	if zo.isReadOnly() {
		err = ErrReadOnlyZeroStor
		return
	}

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	if strings.HasSuffix(object, "/") && data.Reader.Size() == 0 {
		return minio.ObjectInfo{
			Bucket: bucket,
			Name:   object,
			IsDir:  true,
		}, metaMgr.Mkdir(bucket, object)
	}

	info, err := metaMgr.GetObjectMeta(bucket, object)
	if err != nil && !os.IsNotExist(err) {
		return objInfo, err
	}

	if info.IsDir {
		return objInfo, minio.ObjectExistsAsDirectory{Bucket: bucket, Object: object}
	}

	objMeta, err := zo.putObject(ctx, bucket, object, data.Size(), data.Reader, opts, "")
	if err != nil {
		return objInfo, err
	}
	if err := metaMgr.LinkObject(bucket, object, objMeta.Filename); err != nil {
		return objInfo, err
	}

	return meta.CreateObjectInfo(bucket, object, &objMeta), nil
}

func (zo *zerostorObjects) putObject(ctx context.Context, bucket, object string, size int64, reader io.Reader, opts minio.ObjectOptions, partID string) (objMeta meta.ObjectMeta, err error) {
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if _, exists := opts.UserDefined[meta.ETagKey]; !exists {
		opts.UserDefined[meta.ETagKey] = minio.GenETag()
	}

	zstor := zo.manager.GetClient()
	defer zstor.Close()

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	part := 0
	var readSize int64
	finished := false
	objectSize := size

	objMeta, err = metaMgr.WriteMetaStream(
		func() (*metatypes.Metadata, error) {
			if finished {
				return &metatypes.Metadata{}, io.EOF
			}

			metaData, err := zstor.Write(bucket, object+partID+strconv.Itoa(part), &io.LimitedReader{R: reader, N: zo.maxFileSize}, opts.UserDefined)
			if err != nil {
				err = zstorToObjectErr(errors.WithStack(err), Operation("PutObject"), bucket, object)
				return metaData, err
			}
			part++
			readSize += metaData.Size

			if readSize >= objectSize {
				finished = true
			}
			return metaData, nil
		}, bucket, object)

	return objMeta, err
}

// NewMultipartUpload implements minio.ObjectLayer.NewMultipartUpload
func (zo *zerostorObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
		"object": object,
	}).Debug("NewMultipartUpload")

	if zo.isReadOnly() {
		return uploadID, ErrReadOnlyZeroStor
	}

	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if _, exists := opts.UserDefined[meta.ETagKey]; !exists {
		opts.UserDefined[meta.ETagKey] = minio.GenETag()
	}
	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	info, err := metaMgr.GetObjectMeta(bucket, object)
	if err != nil && !os.IsNotExist(err) {
		return uploadID, err
	}

	if info.IsDir {
		return uploadID, minio.ObjectExistsAsDirectory{Bucket: bucket, Object: object}
	}

	uploadID = uuid.NewV4().String()
	err = metaMgr.NewMultipartUpload(bucket, object, uploadID, opts.UserDefined)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("NewMultipartUpload"), bucket, object)
	}
	return uploadID, err
}

// PutObjectPart implements minio.ObjectLayer.PutObjectPart
func (zo *zerostorObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
		"partID":   partID,
	}).Debug("PutObjectPart")

	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}

	if data.Size() > zo.maxFileSize {
		return info, fmt.Errorf("Multipart uploads with parts larger than %v are not supported", zo.maxFileSize)
	}

	return zo.putObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}

func (zo *zerostorObjects) putObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	if exists, err := metaMgr.ValidUpload(bucket, uploadID); err != nil {
		return info, err
	} else if !exists {
		return info, minio.InvalidUploadID{UploadID: uploadID}
	}

	objMeta, err := zo.putObject(ctx, bucket, object, data.Size(), data, opts, strconv.Itoa(partID))
	if err != nil {
		return info, err
	}

	info, err = metaMgr.PutObjectPart(objMeta, bucket, uploadID, partID)
	if err != nil {
		log.Printf("PutObjectPart bucket:%v, object:%v, part:%v, failed: %v\n", bucket, object, partID, err)
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObjectPart"), bucket, object)
	}
	return info, err
}

// CopyObjectPart implements ObjectLayer.CopyObjectPart
func (zo *zerostorObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (info minio.PartInfo, err error) {
	log.WithFields(log.Fields{
		"src-bucket":  srcBucket,
		"src-object":  srcObject,
		"dest-bucket": destBucket,
		"dest-object": destObject,
		"upload-id":   uploadID,
		"src-info":    srcInfo,
		"src-opts":    srcOpts,
		"dst-opts":    dstOpts,
	}).Debug("CopyObjectPart")

	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	srcObjMeta, err := metaMgr.GetObjectMeta(srcBucket, srcObject)
	newMeta := make(map[string]string)
	for k, v := range srcObjMeta.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			continue
		}
		newMeta[k] = v
	}

	for k, v := range srcInfo.UserDefined {
		newMeta[strings.ToLower(k)] = v
	}

	storRd, storWr := io.Pipe()
	defer storRd.Close()
	getCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if length <= 0 {
		length = srcInfo.Size
	}

	go func() {
		storWr.CloseWithError(zo.GetObject(getCtx, srcBucket, srcObject, startOffset, length, storWr, "", srcOpts))
	}()

	hashReader, err := hash.NewReader(storRd, length-startOffset, "", "", length-startOffset, true)
	if err != nil {
		return info, err
	}
	reader := minio.NewPutObjReader(hashReader, nil, nil)
	return zo.putObjectPart(ctx, destBucket, destObject, uploadID, partID, reader, minio.ObjectOptions{UserDefined: newMeta})
}

// CompleteMultipartUpload implements minio.ObjectLayer.CompleteMultipartUpload
func (zo *zerostorObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string,
	parts []minio.CompletePart, options minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
		"parts":    parts,
	}).Debug("CompleteMultipartUpload")

	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	if exists, err := metaMgr.ValidUpload(bucket, uploadID); err != nil {
		return info, err
	} else if !exists {
		return info, minio.InvalidUploadID{UploadID: uploadID}
	}

	info, err = metaMgr.CompleteMultipartUpload(bucket, object, uploadID, parts)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("CompleteMultipartUpload"), bucket, object)
	}
	return info, err
}

// AbortMultipartUpload implements minio.ObjectLayer.AbortMultipartUpload
func (zo *zerostorObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (err error) {
	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
	}).Debug("AbortMultipartUpload")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	if exists, err := metaMgr.ValidUpload(bucket, uploadID); err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	} else if !exists {
		return minio.InvalidUploadID{UploadID: uploadID}
	}

	zstor := zo.manager.GetClient()
	defer zstor.Close()

	c := metaMgr.StreamMultiPartsMeta(ctx, bucket, uploadID)
	for r := range c {
		if r.Error != nil {
			err = r.Error
			break
		}
		if err = zstor.Delete(r.Obj.Metadata); err != nil {
			break
		}
		metaMgr.DeleteBlob(r.Obj.Filename)
	}
	if err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	}

	if err = metaMgr.DeleteUpload(bucket, uploadID); err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	}

	err = metaMgr.DeleteObject(bucket, object)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	}
	return err
}

// ListMultipartUploads implements ObjectLayer.ListMultipartUploads
// Note: because of lack of docs and example in production ready gateway,
// we don't respect : prefix, keyMarker, uploadIDMarker, delimiter, and maxUploads
func (zo *zerostorObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
	}).Debug("ListMultipartUploads")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	result, err = metaMgr.ListMultipartUploads(bucket)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("ListMultipartUploads"), bucket)
	}
	return result, err
}

// ListObjectParts implements ObjectLayer.ListObjectParts
func (zo *zerostorObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	log.WithFields(log.Fields{
		"bucket":    bucket,
		"object":    object,
		"upload-id": uploadID,
	}).Debug("ListObjectParts")

	metaMgr := zo.manager.GetMeta()
	defer metaMgr.Close()

	if exists, err := metaMgr.ValidUpload(bucket, uploadID); err != nil {
		return result, zstorToObjectErr(err, Operation("ListObjectParts"), bucket, object)
	} else if !exists {
		return result, minio.InvalidUploadID{UploadID: uploadID}
	}

	parts, err := metaMgr.ListUploadParts(bucket, uploadID)
	if err != nil {
		return result, zstorToObjectErr(err, Operation("ListObjectParts"), bucket, object)
	}
	return minio.ListPartsInfo{
		Bucket:           bucket,
		Object:           object,
		UploadID:         uploadID,
		MaxParts:         len(parts),
		PartNumberMarker: partNumberMarker,
		Parts:            parts,
	}, nil
}

// Shutdown implements ObjectLayer.Shutdown
func (zo *zerostorObjects) Shutdown(ctx context.Context) error {
	log.Debug("Shutdown")
	return zo.manager.Close()
}

// StorageInfo implements ObjectLayer.StorageInfo
func (zo *zerostorObjects) StorageInfo(ctx context.Context, local bool) (minio.StorageInfo, []error) {
	log.Debug("StorageInfo")

	offline := madmin.BackendDisks{}
	online := madmin.BackendDisks{}

	// iterate all shards, get info from each of it
	// returns immediately once we got an answer
	var disks []madmin.Disk
	var errors []error
	for _, shard := range zo.cfg.DataStor.Shards {
		u, t, err := zo.shardUsage(zo.cfg.Namespace, zo.cfg.Password, shard)
		if err != nil {
			offline[shard.Address] = 0
			errors = append(errors, err)
			log.WithError(err).WithField("shard", shard).Error("failed to get shard info")
			continue
		}

		disks = append(disks, madmin.Disk{
			UsedSpace:      u,
			TotalSpace:     t,
			AvailableSpace: t - u,
			DrivePath:      zo.cfg.Namespace,
		})
		online[shard.Address] = 1
	}

	info := minio.StorageInfo{
		Disks: disks,
	}

	info.Backend.Type = minio.BackendErasure
	info.Backend.OnlineDisks = online
	info.Backend.OfflineDisks = offline
	info.Backend.StandardSCData = zo.cfg.DataStor.Pipeline.Distribution.DataShardCount
	info.Backend.StandardSCParity = zo.cfg.DataStor.Pipeline.Distribution.ParityShardCount
	return info, errors
}

func (zo *zerostorObjects) shardUsage(ns, password string, shard datastor.ShardConfig) (used uint64, total uint64, err error) {
	// get conn
	conn, err := redis.Dial("tcp", shard.Address, redis.DialConnectTimeout(2*time.Second))
	if err != nil {
		return
	}
	defer conn.Close()

	or := func(a, b string) string {
		if len(a) == 0 {
			return b
		}

		return a
	}

	// request the info
	nsinfo, err := redis.String(conn.Do("NSINFO", or(shard.Namespace, ns)))
	if err != nil {
		return
	}
	total, used, err = parseNsInfo(nsinfo)
	return
}

func (zo *zerostorObjects) IsCompressionSupported() bool {
	return false
}

func (zo *zerostorObjects) IsListenBucketSupported() bool {
	return false
}

func (zo *zerostorObjects) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	log.Warnf("not implemented yet")
	return madmin.HealResultItem{}, nil

}

//Operation an alias type for zstorObObjectErr to just make sure it's different from params
type Operation string

// convert 0-stor error to minio error
func zstorToObjectErr(err error, op Operation, params ...string) error {
	if err == nil {
		return nil
	}

	var (
		bucket string
		object string
	)

	if len(params) >= 1 {
		bucket = params[0]
	}

	if len(params) >= 2 {
		object = params[1]
	}

	cause := errors.Cause(err)

	switch cause {
	case metastor.ErrNotFound, datastor.ErrMissingKey, datastor.ErrMissingData, datastor.ErrKeyNotFound, minio.ObjectNotFound{}:
		cause = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	log.WithError(cause).WithFields(
		log.Fields{
			"bucket":    bucket,
			"object":    object,
			"operation": op,
		},
	).Error("operation failed")

	return cause
}

func parseNsInfo(nsinfo string) (total, used uint64, err error) {
	// parse the info
	for _, line := range strings.Split(nsinfo, "\n") {
		elems := strings.Split(line, ":")
		if len(elems) != 2 {
			continue
		}
		val := strings.TrimSpace(elems[1])
		switch strings.TrimSpace(elems[0]) {
		case "data_size_bytes":
			used, err = strconv.ParseUint(val, 10, 64)
		case "data_limits_bytes":
			total, err = strconv.ParseUint(val, 10, 64)
		}
		if err != nil {
			return total, used, err
		}
	}
	if total == 0 {
		total = defaultNamespaceMaxSize
	}

	return total, used, err
}

func maxFileSizeFromConfig(cfg config.Config) int64 {
	// max size of meta without chunks. This includes the following attributes:
	// Namespace, Key, Size, StorageSize, CreateEpoch, LastWriteEpoch, ChunkSize, PreviousKey and NextKey.
	// any change to the metatypes.MetaData or relevant 0-stor implementation, requires an update in this value
	metaWithoutChunks := 96
	// we will also add full file name + 2k bytes for custom user metadata
	metaWithoutChunks += 255 + (2 * 1024)

	// max size of metatypes.Object. This includes the fields: Key and ShardID
	objectSize := 26
	// max number of objects in each chunk
	objectCount := cfg.DataStor.Pipeline.Distribution.DataShardCount + cfg.DataStor.Pipeline.Distribution.ParityShardCount

	// max size of each chunk. This includes the fields: Size, Object[], Hash
	chunkSize := 8 + 32 + (objectCount * objectSize)

	// total metadata size = metaWithoutChunks + (chunkSize * chunkCount)

	// and chunkCount = filesize/blocksize
	// we use this to figure out the maximum filesize that can be stored in 0-stor without the metadata exceeding metaMaxSize

	maxFileSize := ((metaMaxSize - metaWithoutChunks) / chunkSize) * cfg.DataStor.Pipeline.BlockSize

	return int64(maxFileSize)

}
