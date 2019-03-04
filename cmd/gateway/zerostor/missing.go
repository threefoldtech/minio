package zerostor

import (
	"context"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/madmin"
)

//ReloadFormat implementation
func (zo *zerostorObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	return minio.NotImplemented{}
}

//HealBucket implementation
func (zo *zerostorObjects) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

//HealFormat implementation
func (zo *zerostorObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

//HealObject implementation
func (zo *zerostorObjects) HealObject(ctx context.Context, bucket, object string, dryRun, remove bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

//ListBucketsHeal implementation
func (zo *zerostorObjects) ListBucketsHeal(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	return nil, minio.NotImplemented{}
}

//ListObjectsHeal implementation
func (zo *zerostorObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	return minio.ListObjectsInfo{}, minio.NotImplemented{}
}

//IsNotificationSupported implementation
func (zo *zerostorObjects) IsNotificationSupported() bool {
	return false
}

//IsEncryptionSupported implementation
func (zo *zerostorObjects) IsEncryptionSupported() bool {
	return false
}
