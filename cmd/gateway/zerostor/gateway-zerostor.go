package zerostor

import (
	"context"
	goerrors "errors"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/metastor"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"

	"github.com/minio/minio/pkg/auth"
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
// 	// setupZosLogging()
// 	// // config file
// 	// confFile := os.Getenv(minioZstorConfigFileVar)
// 	// if confFile == "" {
// 	// 	confFile = filepath.Join(ctx.String("config-dir"), "zerostor.yaml")
// 	// }

// 	// // meta dir
// 	// metaDir := os.Getenv(minioZstorMetaDirVar)
// 	// if metaDir == "" {
// 	// 	metaDir = filepath.Join(ctx.String("config-dir"), "zerostor_meta")
// 	// }

// 	// if err := repair.CheckAndRepair(confFile, metaDir, os.Getenv(minioZstorMetaPrivKey)); err != nil {
// 	// 	log.Println("check and repair failed:", err)
// 	// 	os.Exit(1)
// 	// }
// }

// Zerostor implements minio.Gateway interface
type Zerostor struct {
	confFile    string
	metaDir     string
	metaPrivKey string
	zo          *zerostorObjects
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

	zstor, err := createClient(cfg)
	if err != nil {
		log.Println("failed to creates zstor client: ", err.Error())
		return nil, err
	}

	zsClient := zsClient{
		zstor,
		&zsManager.mux,
	}
	zsManager.zstorClient = &zsClient
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

// func (zo *zerostorObjects) shardHealth(shard datastor.Shard) error {
// 	key, err := shard.CreateObject([]byte("test write"))
// 	if err != nil {
// 		return err
// 	}

// 	return shard.DeleteObject(key)
// }

// func (zo *zerostorObjects) healthReporter() {
// 	for {
// 		<-time.After(10 * time.Minute)
// 		log.Debug("checking cluster health")

// 		store := zo.getZstor()
// 		for iter := store.cluster.GetRandomShardIterator(nil); iter.Next(); {
// 			shard := iter.Shard()
// 			err := zo.shardHealth(shard)
// 			if err != nil {
// 				log.WithFields(log.Fields{
// 					"shard": shard.Identifier(),
// 				}).WithError(err).Error("error while checking shard health")
// 			} else {
// 				log.WithFields(log.Fields{
// 					"shard": shard.Identifier(),
// 				}).Error("shard state is okay")
// 			}
// 		}
// 	}
// }

func (zo *zerostorObjects) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bkt, err := zo.meta.GetBucket(bucket)
	if err != nil {
		err = zstorToObjectErr(err, Operation("GetBucketInfo"), bucket)
		return
	}

	bucketInfo.Name = bucket
	bucketInfo.Created = bkt.Created
	return
}

func (zo *zerostorObjects) DeleteBucket(ctx context.Context, bucket string) error {
	if zo.isReadOnly() {
		return ErrReadOnlyZeroStor
	}

	err := zo.meta.DeleteBucket(bucket)
	return zstorToObjectErr(errors.WithStack(err), Operation("DeleteBucket"), bucket)

}

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
	return nil
}

func (zo *zerostorObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, nil
}

func (zo *zerostorObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64,
	writer io.Writer, etag string, opts minio.ObjectOptions) error {
	return nil
}

func (zo *zerostorObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {

	return minio.ObjectInfo{}, nil
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
	opts.UserDefined[meta.ETagKey] = minio.GenETag()
	zstor := zo.zsManager.Get()
	defer zstor.Close()

	metaData, err := zstor.Write(bucket, object, data.Reader, opts.UserDefined)
	objMeta := meta.ObjectMeta{
		Metadata:   metaData,
		NextPart:   "",
		ObjectSize: metaData.Size,
	}
	partID, err := zo.meta.SetChunk(&objMeta)
	if err != nil {
		log.Printf("PutObject bucket:%v, object:%v, failed: %v\n", bucket, object, err)
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObject"), bucket, object)
		return
	}

	if err = zo.meta.SetObjectLink(bucket, object, partID); err != nil {
		err = zstorToObjectErr(errors.WithStack(err), Operation("PutObject"), bucket, object)
		return
	}

	objInfo = meta.CreateObjectInfo(bucket, object, &objMeta)
	return

}

// NewMultipartUpload implements minio.ObjectLayer.NewMultipartUpload
func (zo *zerostorObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	return "", nil
}

// PutObjectPart implements minio.ObjectLayer.PutObjectPart
func (zo *zerostorObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	return minio.PartInfo{}, nil
}

func (zo *zerostorObjects) putObjectPart(ctx context.Context, bucket, object, uploadID, etag string, partID int, rd io.Reader) (info minio.PartInfo, err error) {
	return minio.PartInfo{}, nil
}

// CopyObjectPart implements ObjectLayer.CopyObjectPart
func (zo *zerostorObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return minio.PartInfo{}, nil
}

// CompleteMultipartUpload implements minio.ObjectLayer.CompleteMultipartUpload
func (zo *zerostorObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string,
	parts []minio.CompletePart, options minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, nil
}

// AbortMultipartUpload implements minio.ObjectLayer.AbortMultipartUpload
func (zo *zerostorObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	return nil
}

// ListMultipartUploads implements ObjectLayer.ListMultipartUploads
// Note: because of lack of docs and example in production ready gateway,
// we don't respect : prefix, keyMarker, uploadIDMarker, delimiter, and maxUploads
func (zo *zerostorObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	return minio.ListMultipartsInfo{}, nil
}

// ListObjectParts implements ObjectLayer.ListObjectParts
func (zo *zerostorObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	return minio.ListPartsInfo{}, nil
}

// Shutdown implements ObjectLayer.Shutdown
func (zo *zerostorObjects) Shutdown(ctx context.Context) error {
	return nil
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
