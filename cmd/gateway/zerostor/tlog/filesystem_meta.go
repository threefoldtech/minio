package tlog

import (
	"context"
	"time"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	log "github.com/sirupsen/logrus"
)

func (t *fsTLogger) Set(path meta.Path, data []byte) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Store.Set(path, data); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationSet,
		path,
		data,
	}, true)

	return err
}

func (t *fsTLogger) Del(path meta.Path) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Store.Del(path); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationDel,
		path,
	}, true)

	return err
}

func (t *fsTLogger) Link(link, target meta.Path) error {
	t.recorder.Begin()
	defer t.recorder.End()

	if err := t.Store.Link(link, target); err != nil {
		return err
	}

	_, err := t.recorder.Record(Record{
		OperationLink,
		link,
		target,
	}, true)

	return err
}

//Sync syncs the backend storage with the latest records from the tlog storage
func (t *fsTLogger) Sync() error {

	return t.recorder.Play(nil, func(key []byte, rec Record) error {
		if err := rec.Play(t.Store); err != nil {
			logger := log.WithError(err).WithFields(log.Fields{
				"subsystem": "sync",
				"tlog":      t.recorder.p.address,
				"namespace": t.recorder.p.namespace,
				"action":    rec.Action(),
			})

			if _, ok := err.(Warning); ok {
				logger.Warning("failed to process tlog record")
			} else {
				logger.Error("failed to process tlog record")
			}
		}

		return t.recorder.SetState(key)
	})
}

//HealthChecker start health checker for TLogger
func (t *fsTLogger) HealthChecker(ctx context.Context) {
	for {
		select {
		case <-time.After(10 * time.Minute):
		case <-ctx.Done():
			return
		}

		if err := t.recorder.test(); err != nil {
			log.WithFields(log.Fields{
				"subsystem": "tlog",
				"tlog":      t.recorder.p.address,
				"namespace": t.recorder.p.namespace,
				"master":    false,
			}).WithError(err).Error("error while checking shard health")
		} else {
			log.WithFields(log.Fields{
				"subsystem": "tlog",
				"tlog":      t.recorder.p.address,
				"namespace": t.recorder.p.namespace,
				"master":    false,
			}).Error("tlog state is okay")
		}
	}
}
