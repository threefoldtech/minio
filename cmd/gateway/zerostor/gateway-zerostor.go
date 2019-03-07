package zerostor

import (
	"context"
	goerrors "errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/metastor"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
)

const (
	zerostorBackend         = "zerostor"
	zerostorRepairBackend   = "zerostor-repair"
	minioZstorConfigFileVar = "MINIO_ZEROSTOR_CONFIG_FILE"
	minioZstorMetaDirVar    = "MINIO_ZEROSTOR_META_DIR"
	minioZstorMetaPrivKey   = "MINIO_ZEROSTOR_META_PRIVKEY"
	minioZstorDebug         = "MINIO_ZEROSTOR_DEBUG"
)

var (
	errBucketNotFound = goerrors.New("bucket not found")
	errBucketExists   = goerrors.New("bucket already exists")
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

	// minio.RegisterGatewayCommand(cli.Command{
	// 	Name:               zerostorRepairBackend,
	// 	Usage:              "checks all objects in the store and repair if necessary",
	// 	Action:             zerostorRepairMain,
	// 	CustomHelpTemplate: zerostorRepairTemplate,
	// })

	debugFlag = os.Getenv("MINIO_ZEROSTOR_DEBUG") == "1"
}

func setupZosLogging() {
	log.SetFormatter(&ZOSLogFormatter{
		Default: &log.TextFormatter{},
		Error:   &log.JSONFormatter{},
	})
}

// Handler for 'minio gateway zerostor' command line.
func zerostorGatewayMain(ctx *cli.Context) {
	setupZosLogging()

	// config file
	confFile := os.Getenv(minioZstorConfigFileVar)
	if confFile == "" {
		confFile = filepath.Join(ctx.String("config-dir"), "zerostor.yaml")
	}

	// meta dir
	metaDir := os.Getenv(minioZstorMetaDirVar)
	if metaDir == "" {
		metaDir = filepath.Join(ctx.String("config-dir"), "zerostor_meta")
	}

	minio.StartGateway(ctx, &Zerostor{
		confFile:    confFile,
		metaDir:     metaDir,
		metaPrivKey: os.Getenv(minioZstorMetaPrivKey),
	})
}

// func zerostorRepairMain(ctx *cli.Context) {
// 	setupZosLogging()
// 	// config file
// 	confFile := os.Getenv(minioZstorConfigFileVar)
// 	if confFile == "" {
// 		confFile = filepath.Join(ctx.String("config-dir"), "zerostor.yaml")
// 	}

// 	// meta dir
// 	metaDir := os.Getenv(minioZstorMetaDirVar)
// 	if metaDir == "" {
// 		metaDir = filepath.Join(ctx.String("config-dir"), "zerostor_meta")
// 	}

// 	if err := repair.CheckAndRepair(confFile, metaDir, os.Getenv(minioZstorMetaPrivKey)); err != nil {
// 		log.Println("check and repair failed:", err)
// 		os.Exit(1)
// 	}
// }

// Zerostor implements minio.Gateway interface
type Zerostor struct {
	confFile    string
	metaDir     string
	metaPrivKey string
	zo          *zerostorObjects
	cluster     datastor.Cluster
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
	zsManager := zsClientManager{}

	zstor, cluster, err := createClient(cfg)
	if err != nil {
		log.Println("failed to creates zstor client: ", err.Error())
		return nil, err
	}

	zsClient := zsClient{
		zstor,
		&zsManager.mux,
	}
	zsManager.zstorClient = &zsClient
	zsManager.Cluster = cluster
	metaManager, err := meta.InitializeMetaManager(z.metaDir)
	if err != nil {
		log.Println("failed to create meta manager: ", err.Error())
		return nil, err
	}

	zo := &zerostorObjects{
		zsManager: &zsManager,
		cfg:       cfg,
		meta:      metaManager,
	}

	go zo.handleConfigReload(z.confFile)

	return zo, nil
}

type zerostorObjects struct {
	//minio.GatewayUnsupported
	zsManager *zsClientManager
	cfg       config.Config
	meta      meta.Manager
}

func (zo *zerostorObjects) isReadOnly() bool {
	return zo.cfg.Minio.Master != nil
}

func (zo *zerostorObjects) handleConfigReload(confFile string) {
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGHUP)

	go func() error {
		for {
			<-sigCh
			log.Println("Got SIGHUP:reload the config")
			cfg, _ := config.Load(confFile)
			// @todo
			// if err != nil {
			// 	return nil, err
			// }
			zo.cfg = cfg
			zo.zsManager.Open(cfg)
		}
	}()
}

func (zo *zerostorObjects) shardHealth(shard datastor.Shard) error {
	key, err := shard.CreateObject([]byte("test write"))
	if err != nil {
		return err
	}

	return shard.DeleteObject(key)
}

func (zo *zerostorObjects) healthReporter() {
	for {
		<-time.After(10 * time.Minute)
		log.Debug("checking cluster health")

		for iter := zo.zsManager.Cluster.GetRandomShardIterator(nil); iter.Next(); {
			shard := iter.Shard()
			err := zo.shardHealth(shard)
			if err != nil {
				log.WithFields(log.Fields{
					"shard": shard.Identifier(),
				}).WithError(err).Error("error while checking shard health")
			} else {
				log.WithFields(log.Fields{
					"shard": shard.Identifier(),
				}).Error("shard state is okay")
			}
		}
	}
}

// GetBucketInfo implements minio.ObjectLayer.GetBucketInfo interface
func (zo *zerostorObjects) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bkt, err := zo.meta.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketInfo"), bucket)
		return bucketInfo, err
	}

	bucketInfo.Name = bucket
	bucketInfo.Created = bkt.Created
	return bucketInfo, err
}

// DeleteBucket implements minio.ObjectLayer.DeleteBucket interface
func (zo *zerostorObjects) DeleteBucket(ctx context.Context, bucket string) (err error) {
	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}

	result, err := zo.meta.ListObjects(ctx, bucket, "", "", "/", 10000)
	if err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucket"), bucket)
	}

	if len(result.Objects) > 0 {
		return minio.BucketNotEmpty{}
	}

	err = zo.meta.DeleteBucket(bucket)
	return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucket"), bucket)

}

// ListBuckets implements minio.ObjectLayer.ListBuckets interface
func (zo *zerostorObjects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	var buckets []minio.BucketInfo

	bucketsList, err := zo.meta.ListBuckets()
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
func (zo *zerostorObjects) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}

	log.Debugf("MakeBucketWithLocation bucket=%v, location=%v\n", bucket, location)
	err := zo.meta.CreateBucket(bucket)
	return zstorToObjectErr(errors.WithStack(err), Operation("MakeBucketWithLocation"), bucket)
}

// GetBucketPolicy implements minio.ObjectLayer.GetBucketPolicy interface
func (zo *zerostorObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	bkt, err := zo.meta.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketInfo"), bucket)
		return nil, err
	}
	return &bkt.Policy, nil
}

// SetBucketPolicy implements minio.ObjectLayer.SetBucketPolicy
func (zo *zerostorObjects) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}

	err := zo.meta.SetBucketPolicy(bucket, policy)
	return zstorToObjectErr(errors.WithStack(err), Operation("SetBucketPolicy"), bucket)

}

func (zo *zerostorObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}

	err := zo.meta.SetBucketPolicy(bucket, &policy.Policy{Version: policy.DefaultVersion})
	return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucketPolicy"), bucket)
}

func (zo *zerostorObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	zstor := zo.zsManager.Get()
	defer zstor.Close()

	c := zo.meta.StreamObjectMeta(ctx, bucket, object)

	for r := range c {
		if r.Error != nil {
			return zstorToObjectErr(errors.WithStack(r.Error), Operation("DeleteObject"), bucket, object)
		}
		if err := zstor.Delete(*r.Obj.Metadata); err != nil {
			return zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
		}
		if err := zo.meta.DeleteBlob(r.Obj.Filename); err != nil {
			return zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
		}
		if r.Obj.NextBlob != "" {
			if err := zo.meta.SetObjectLink(bucket, object, r.Obj.NextBlob); err != nil {
				return zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
			}
		}
	}

	err := zo.meta.DeleteObjectFile(bucket, object)
	return zstorToObjectErr(errors.WithStack(err), Operation("DeleteObject"), bucket, object)
}

func (zo *zerostorObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if zo.isReadOnly() {
		return objInfo, ErrReadOnlyZeroStor
	}

	log.WithFields(log.Fields{
		"src-bucket":  srcBucket,
		"src-object":  srcObject,
		"dest-bucket": destBucket,
		"dest-object": destObject,
		"src-info":    srcInfo,
	}).Debug("CopyObject")

	storRd, storWr := io.Pipe()
	defer storRd.Close()

	go func() {
		defer storWr.Close()
		zo.GetObject(ctx, srcBucket, srcObject, 0, srcInfo.Size, storWr, "", srcOpts)
	}()

	hashReader, err := hash.NewReader(storRd, srcInfo.Size, "", "", srcInfo.Size)
	if err != nil {
		return objInfo, zstorToObjectErr(errors.WithStack(err), Operation("CopyObject"), destBucket, destObject)
	}
	reader := minio.NewPutObjReader(hashReader, nil, nil)
	objInfo, err = zo.putObject(ctx, destBucket, destObject, reader, dstOpts)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("CopyObject"), destBucket, destObject)
	}
	return objInfo, err
}

func (zo *zerostorObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = zo.GetObjectInfo(ctx, bucket, object, opts)
	println("size", objInfo.Size)
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
	return minio.NewGetObjectReaderFromReader(pr, objInfo, pipeCloser), nil
}

func (zo *zerostorObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64,
	writer io.Writer, etag string, opts minio.ObjectOptions) error {

	zstor := zo.zsManager.Get()
	defer zstor.Close()

	objMeta, err := zo.meta.GetObjectInfo(bucket, object)
	if err != nil {
		return err
	}
	var streamAll bool
	var accSize int64
	var offset int64

	remaingLength := length
	firstPart := true

	if startOffset == 0 && (length <= 0 || length == objMeta.Size) {
		streamAll = false
	}

	c := zo.meta.StreamObjectMeta(ctx, bucket, object)
	for r := range c {
		if r.Error != nil {
			return r.Error
		}

		if streamAll {
			if err := zstor.Read(r.Obj.Metadata, writer, 0, 0); err != nil {
				return err
			}
		} else {

			// we haven't reached the correct part to start from
			if startOffset > accSize+r.Obj.Size {
				continue
			} else {

				if firstPart {
					offset = startOffset - accSize
					firstPart = false

				} else {
					offset = 0
				}

				readLength := r.Obj.Metadata.Size - offset
				if remaingLength < readLength {
					readLength = remaingLength
				}

				if err := zstor.Read(r.Obj.Metadata, writer, offset, readLength); err != nil {
					return err
				}
				remaingLength -= readLength
				if remaingLength == 0 {
					break
				}
			}
			accSize += r.Obj.Size

		}

	}
	return nil
}

func (zo *zerostorObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	log.WithFields(log.Fields{
		"bucket": bucket,
		"object": object,
	}).Debug("GetObjectInfo")

	objInfo, err = zo.meta.GetObjectInfo(bucket, object)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("GetObjectInfo"), bucket, object)
	}

	return objInfo, err
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

	// get objects
	result, err = zo.meta.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
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
	}).Debug("ListObjects")

	// get objects
	result, err = zo.meta.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("ListObjectsV2"), bucket)
		return
	}
	return result, nil
}

// PutObject implements ObjectLayer.PutObject
func (zo *zerostorObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if zo.isReadOnly() {
		err = ErrReadOnlyZeroStor
		return
	}

	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"metadata": opts.UserDefined,
	}).Debug("PutObject")

	return zo.putObject(ctx, bucket, object, data, opts)
}

func (zo *zerostorObjects) putObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if _, exists := opts.UserDefined[meta.ETagKey]; !exists {
		opts.UserDefined[meta.ETagKey] = minio.GenETag()
	}

	zstor := zo.zsManager.Get()
	defer zstor.Close()

	metaData, err := zstor.Write(bucket, object, data.Reader, opts.UserDefined)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObject"), bucket, object)
		return objInfo, err
	}

	objInfo, err = zo.meta.PutObject(metaData, bucket, object)

	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObject"), bucket, object)
	}
	return objInfo, err

}

// NewMultipartUpload implements minio.ObjectLayer.NewMultipartUpload
func (zo *zerostorObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	if zo.isReadOnly() {
		return uploadID, ErrReadOnlyZeroStor
	}
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if _, exists := opts.UserDefined[meta.ETagKey]; !exists {
		opts.UserDefined[meta.ETagKey] = minio.GenETag()
	}

	uploadID, err = zo.meta.NewMultipartUpload(bucket, object, opts)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("NewMultipartUpload"), bucket, object)
	}
	return uploadID, err
}

// PutObjectPart implements minio.ObjectLayer.PutObjectPart
func (zo *zerostorObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}

	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
		"partID":   partID,
	}).Debug("PutObjectPart")

	return zo.putObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}

func (zo *zerostorObjects) putObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	if exists, err := zo.meta.ValidUpload(bucket, uploadID); err != nil {
		return info, err
	} else if !exists {
		return info, minio.InvalidUploadID{UploadID: uploadID}
	}
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if _, exists := opts.UserDefined[meta.ETagKey]; !exists {
		opts.UserDefined[meta.ETagKey] = minio.GenETag()
	}

	zstor := zo.zsManager.Get()
	defer zstor.Close()

	metaData, err := zstor.Write(bucket, object+strconv.Itoa(partID), data.Reader, opts.UserDefined)
	if err != nil {
		log.Printf("PutObjectPart bucket:%v, object:%v, part:%v, failed: %v\n", bucket, object, partID, err)
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObjectPart"), bucket, object)
		return info, err
	}

	info, err = zo.meta.PutObjectPart(metaData, bucket, uploadID, partID)
	if err != nil {
		log.Printf("PutObjectPart bucket:%v, object:%v, part:%v, failed: %v\n", bucket, object, partID, err)
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObjectPart"), bucket, object)
	}
	return info, err
}

// CopyObjectPart implements ObjectLayer.CopyObjectPart
func (zo *zerostorObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (info minio.PartInfo, err error) {
	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}
	println("copy object part")
	storRd, storWr := io.Pipe()
	defer storRd.Close()

	go func() {
		defer storWr.Close()
		zo.GetObject(ctx, srcBucket, srcObject, startOffset, length, storWr, "", srcOpts)
	}()

	if length <= 0 {
		length = srcInfo.Size
	}

	hashReader, err := hash.NewReader(storRd, length-startOffset, "", "", length-startOffset)
	if err != nil {
		return info, err
	}
	reader := minio.NewPutObjReader(hashReader, nil, nil)
	return zo.putObjectPart(ctx, destBucket, destObject, uploadID, partID, reader, dstOpts)
}

// CompleteMultipartUpload implements minio.ObjectLayer.CompleteMultipartUpload
func (zo *zerostorObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string,
	parts []minio.CompletePart, options minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	if zo.isReadOnly() {
		return info, ErrReadOnlyZeroStor
	}

	log.WithFields(log.Fields{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
		"parts":    parts,
	}).Debug("CompleteMultipartUpload")

	if exists, err := zo.meta.ValidUpload(bucket, uploadID); err != nil {
		return info, err
	} else if !exists {
		return info, minio.InvalidUploadID{UploadID: uploadID}
	}

	info, err = zo.meta.CompleteMultipartUpload(bucket, object, uploadID, parts)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("CompleteMultipartUpload"), bucket, object)
	}
	return info, err
}

// AbortMultipartUpload implements minio.ObjectLayer.AbortMultipartUpload
func (zo *zerostorObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	if exists, err := zo.meta.ValidUpload(bucket, uploadID); err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	} else if !exists {
		return minio.InvalidUploadID{UploadID: uploadID}
	}

	zstor := zo.zsManager.Get()
	defer zstor.Close()

	c := zo.meta.StreamPartsMeta(ctx, bucket, uploadID)
	for r := range c {
		if r.Error != nil {
			err = r.Error
			break
		}
		if err = zstor.Delete(*r.Obj.Metadata); err != nil {
			break
		}
		zo.meta.DeleteBlob(r.Obj.Filename)
	}
	if err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	}

	if err = zo.meta.DeleteUploadDir(bucket, uploadID); err != nil {
		return zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	}

	err = zo.meta.DeleteObjectFile(bucket, object)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("AbortMultipartUpload"), bucket, object)
	}
	return err
}

// ListMultipartUploads implements ObjectLayer.ListMultipartUploads
// Note: because of lack of docs and example in production ready gateway,
// we don't respect : prefix, keyMarker, uploadIDMarker, delimiter, and maxUploads
func (zo *zerostorObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	result, err = zo.meta.ListMultipartUploads(bucket)
	if err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("ListMultipartUploads"), bucket)
	}
	return result, err
}

// ListObjectParts implements ObjectLayer.ListObjectParts
func (zo *zerostorObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	if exists, err := zo.meta.ValidUpload(bucket, uploadID); err != nil {
		return result, zstorToObjectErr(err, Operation("ListObjectParts"), bucket, object)
	} else if !exists {
		return result, minio.InvalidUploadID{UploadID: uploadID}
	}

	parts, err := zo.meta.ListPartsInfo(bucket, uploadID)
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
	return zo.zsManager.Close()
}

// StorageInfo implements ObjectLayer.StorageInfo
func (zo *zerostorObjects) StorageInfo(ctx context.Context) (info minio.StorageInfo) {
	return minio.StorageInfo{}
}

func (zo *zerostorObjects) IsCompressionSupported() bool {
	return false
}

func (zo *zerostorObjects) IsListenBucketSupported() bool {
	return false
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

	log.WithError(err).WithFields(
		log.Fields{
			"bucket":    bucket,
			"object":    object,
			"operation": op,
		},
	).Error("operation failed")

	cause := errors.Cause(err)

	switch cause {
	case metastor.ErrNotFound, datastor.ErrMissingKey, datastor.ErrMissingData, datastor.ErrKeyNotFound:
		cause = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	case errBucketNotFound:
		cause = minio.BucketNotFound{
			Bucket: bucket,
		}
	case errBucketExists:
		cause = minio.BucketExists{
			Bucket: bucket,
		}
	}

	return cause
}
