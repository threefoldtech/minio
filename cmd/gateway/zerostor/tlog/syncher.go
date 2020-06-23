package tlog

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/minio/minio/cmd/gateway/zerostor/meta"
)

//Syncher struct
type Syncher struct {
	meta meta.Manager
	p    *Pool
	s    string
}

//NewSyncher creates a new syncher that sync tlogs from source zdb defined by address, and namespace
//to the local TLogger
func NewSyncher(address, namespace, password, stateFile string, meta meta.Manager) *Syncher {
	return &Syncher{
		meta: meta,
		p:    newZDBPool(address, namespace, password),
		s:    stateFile,
	}
}

func (s *Syncher) setState(key []byte) error {
	return ioutil.WriteFile(s.s, key, 0600)
}

func (s *Syncher) state() ([]byte, error) {
	key, err := ioutil.ReadFile(s.s)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return key, nil
}

func (s *Syncher) wait() error {
	con := s.p.Get()
	defer con.Close()

	_, err := con.Do("WAIT", "SET", 60000)
	if err != nil && err.Error() == "Timeout" {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

func (s *Syncher) signal(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)

	go func() {
		defer close(ch)
		for {
			if err := s.wait(); err != nil {
				log.Warningf("failed to wait for master zdb action: %v", err)
				select {
				case <-time.After(3 * time.Second):
				case <-ctx.Done():
					//in case context was canceled when we were waiting
					return
				}

				continue
			}

			select {
			case ch <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (s *Syncher) sync() error {
	key, err := s.state()
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"subsystem": "sync",
		"last-key":  fmt.Sprintf("%X", key),
	}).Debug("synchronizing with master")

	return s.p.play(key, func(key []byte, rec Record) error {
		if err := rec.Play(s.meta); err != nil {
			logger := log.WithError(err).WithFields(log.Fields{
				"subsystem": "sync",
				"tlog":      s.p.address,
				"namespace": s.p.namespace,
				"action":    rec.Action(),
			})

			if _, ok := err.(Warning); ok {
				logger.Warning("failed to process tlog record")
			} else {
				logger.Error("failed to process tlog record")
			}
		}

		return s.setState(key)
	})
}

//Sync starts the sync process, never return
func (s *Syncher) Sync(ctx context.Context) error {
	/*
		To avoid losing a signal, we first register to the
		signal channel. Then do an initial synching to master
		after that, we just wait for new signals, and process them
		immediately by doing a sync again
	*/

	//register to signal first
	signal := s.signal(ctx)
	//initial sync
	if err := s.sync(); err != nil {
		return err
	}

	//wait for signal
	for {
		select {
		case <-ctx.Done():
			log.Info("terminate synchronization due to cancel")
			return nil
		case <-signal:
			if err := s.sync(); err != nil {
				return err
			}
		}
	}
}
