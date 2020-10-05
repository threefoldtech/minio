package repair

import (
	"context"
	"fmt"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline/storage"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"
)

//Status of the heal process
type Status interface {
	Error() error
	String() string
}

//BlobStatus heal status of a blob
type BlobStatus struct {
	Status   storage.CheckStatus
	Repaired bool

	blob string
	err  error
}

func (s *BlobStatus) Error() error {
	return s.err
}

func (s *BlobStatus) String() string {
	str := fmt.Sprintf("blob: %s (status: %s, repaired: %v)", s.blob, s.Status, s.Repaired)
	if s.err != nil {
		str = fmt.Sprintf("%s [error: %s]", str, s.err)
	}

	return str
}

// WithError attach an error to the blob status
func (s *BlobStatus) WithError(err error) *BlobStatus {
	s.err = err
	return s
}

// WithStatus sets the status of the blob check
func (s *BlobStatus) WithStatus(status storage.CheckStatus) *BlobStatus {
	s.Status = status
	return s
}

// WithRepaired set if this blob was repaired
func (s *BlobStatus) WithRepaired(repaired bool) *BlobStatus {
	s.Repaired = repaired
	return s
}

func statusForMeta(meta meta.Metadata) *BlobStatus {
	blob := meta.Filename
	if len(blob) == 0 {
		blob = "<empty>"
	}
	return &BlobStatus{blob: blob}
}

// ObjectStatus struct
type ObjectStatus struct {
	Bucket string
	Object string

	Chunks   uint64
	Optimal  uint64
	Valid    uint64
	Invalid  uint64
	Errors   uint64
	Repaired uint64
}

// AddBlob adds a blob status to this object status
func (s *ObjectStatus) AddBlob(blob *BlobStatus) {
	s.Chunks++
	if blob.Error() != nil {
		s.Errors++
	}
	if blob.Repaired {
		s.Repaired++
	}
	switch blob.Status {
	case storage.CheckStatusInvalid:
		s.Invalid++
	case storage.CheckStatusValid:
		s.Valid++
	case storage.CheckStatusOptimal:
		s.Optimal++
	}
}

func (s *ObjectStatus) Error() error {
	if s.Errors > 0 {
		return fmt.Errorf("object has %d blobs with errors", s.Errors)
	}

	return nil
}

func (s *ObjectStatus) String() string {
	return fmt.Sprintf("object: %s/%s (blobs: %d, optimal: %d, valid: %d, invalid: %d, errors: %d, repaired: %d)",
		s.Bucket, s.Object,
		s.Chunks, s.Optimal, s.Valid, s.Invalid, s.Errors, s.Repaired)
}

// BucketStatus struct
type BucketStatus struct {
	bucket string
	err    error
}

// WithError attaches error to this bucket status instance
func (s *BucketStatus) WithError(err error) *BucketStatus {
	s.err = err
	return s
}

func (s *BucketStatus) Error() error {
	return s.err
}

func (s *BucketStatus) String() string {
	str := fmt.Sprintf("bucket: %s", s.bucket)
	if s.err != nil {
		str = fmt.Sprintf("%s [error: %s]", str, s.err)
	}
	return str
}

// Healer provide interface to check and repair objects
type Healer struct {
	manager meta.Manager
	client  *client.Client
}

// NewHealer creates a new instance of Healer
func NewHealer(manager meta.Manager, client *client.Client) *Healer {
	return &Healer{manager: manager, client: client}
}

func (h *Healer) checkBlob(ctx context.Context, cb Callback, meta meta.Metadata) *BlobStatus {
	result := statusForMeta(meta)
	if len(meta.Chunks) == 0 {
		// not a real object and is just a directory
		return result.WithStatus(storage.CheckStatusOptimal)
	}

	status, err := h.client.Check(meta.Metadata, false)
	if err != nil {
		return result.WithError(err)
	}
	result = result.WithStatus(status)

	updated, err := cb(meta.Metadata, status)
	if err != nil {
		return result.WithError(errors.Wrapf(err, "failed to apply repair on blob (%s)", meta.Filename))
	}

	if updated == nil {
		//nothing to change
		return result
	}

	meta.Metadata = *updated

	if err := h.manager.BlobSet(&meta); err != nil {
		return result.WithError(errors.Wrapf(err, "failed to update meta object for blob (%s)", meta.Filename))
	}
	// if repaired then status should be back to optimal
	result = result.WithStatus(storage.CheckStatusOptimal)
	return result.WithRepaired(true)
}

//Check check entire minio instance by going over all blob meta regardless of objects or buckets
func (h *Healer) Check(ctx context.Context, cb Callback) <-chan Status {
	ch := make(chan Status)
	go func() {
		defer close(ch)
		for blob := range h.manager.StreamBlobs(ctx) {
			status := h.checkBlob(ctx, cb, blob.Obj)
			select {
			case <-ctx.Done():
				return
			case ch <- status:
			}
		}
	}()

	return ch
}

//CheckObject check single object
func (h *Healer) CheckObject(ctx context.Context, cb Callback, bucket, object string) <-chan Status {
	blobs := h.manager.MetaGetStream(ctx, bucket, object, "")
	ch := make(chan Status)
	go func(ctx context.Context) {
		defer close(ch)
		summary := &ObjectStatus{
			Bucket: bucket,
			Object: object,
		}

		for blob := range blobs {
			status := h.checkBlob(ctx, cb, blob.Obj)
			summary.AddBlob(status)
			select {
			case <-ctx.Done():
				return
			case ch <- status:
			}

		}

		// after feeding all separate blob check and repair status
		// we send the object status
		select {
		case <-ctx.Done():
			return
		case ch <- summary:
		}
	}(ctx)

	return ch
}

// CheckBucket check entire bucket. if bucket is empty, scans the full installation
func (h *Healer) CheckBucket(ctx context.Context, cb Callback, bucket string) (<-chan Status, error) {
	list, err := h.manager.ObjectList(ctx, bucket, "", "")
	if err != nil {
		return nil, err

	}
	ch := make(chan Status)

	go func(ctx context.Context) {
		defer close(ch)
		result := &BucketStatus{bucket: bucket}

		for object := range list {
			if object.Error != nil {
				log.WithError(err).WithField("bucket", bucket).Error("error while scanning bucket")
				continue
			}

			// forward object report
			for status := range h.CheckObject(ctx, cb, bucket, object.Info.Name) {
				select {
				case <-ctx.Done():
					return
				case ch <- status:
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case ch <- result:
		}
	}(ctx)

	return ch, nil
}

// CheckBuckets check all buckets
func (h *Healer) CheckBuckets(ctx context.Context, cb Callback) (<-chan Status, error) {
	buckets, err := h.manager.BucketsList()
	if err != nil {
		return nil, err
	}

	ch := make(chan Status)
	go func() {
		defer close(ch)
		for bucket := range buckets {
			stream, err := h.CheckBucket(ctx, cb, bucket)
			if err != nil {
				select {
				case ch <- &BucketStatus{bucket: bucket, err: err}:
					continue
				case <-ctx.Done():
					return
				}
			}

			for status := range stream {
				select {
				case ch <- status:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case ch <- &BucketStatus{bucket: bucket}:
			}
		}
	}()

	return ch, nil
}

// Dryrun is a callback that does nothing.
func (h *Healer) Dryrun(meta metatypes.Metadata, status storage.CheckStatus) (*metatypes.Metadata, error) {
	return nil, nil
}

// CheckAndRepair is a callback that tries to repair the meta if neededs
func (h *Healer) CheckAndRepair(meta metatypes.Metadata, status storage.CheckStatus) (*metatypes.Metadata, error) {
	if status == storage.CheckStatusOptimal {
		// nothing to do.
		return nil, nil
	}

	return h.client.Repair(meta)
}

// Callback to the check function, where a repair can be done here, should then return new fixed meta
type Callback func(meta metatypes.Metadata, status storage.CheckStatus) (*metatypes.Metadata, error)
