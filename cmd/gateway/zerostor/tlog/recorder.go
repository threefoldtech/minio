package tlog

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/minio/minio/cmd/gateway/zerostor/meta"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
)

var (
	emptyKey = []byte{}
)

//Warning marks this error as of level warning
type Warning struct {
	Cause error
}

func (w Warning) Error() string {
	return w.Cause.Error()
}

//Record is a tlog record structure
type Record []interface{}

//Action returns the record action name
func (r Record) Action() string {
	if len(r) == 0 {
		panic("invalid record")
	}

	return r[0].(string)
}

//At return record entry at index (starts at one)
func (r Record) At(at int) interface{} {
	if len(r) <= at {
		panic("entry out of range")
	}

	return r[at]
}

//String returns string entry at index (starts at one)
func (r Record) String(at int) string {
	if len(r) <= at {
		panic("entry out of range")
	}

	if o := r.At(at); o != nil {
		return o.(string)
	}

	return ""
}

//Int returns int entry at index (starts at one)
func (r Record) Int(at int) int {
	if len(r) <= at {
		panic("entry out of range")
	}

	if o := r.At(at); o != nil {
		return int(o.(int64))
	}

	return 0
}

//Bytes returns byte slice entry at index (starts at one)
func (r Record) Bytes(at int) []byte {
	if len(r) <= at {
		panic("entry out of range")
	}

	if o := r.At(at); o != nil {
		return o.([]byte)
	}

	return nil
}

//Path returns path at index
func (r Record) Path(at int) meta.Path {
	if len(r) <= at {
		panic("entry out of range")
	}

	if o := r.At(at); o != nil {
		return o.(meta.Path)
	}

	return meta.Path{}
}

//JSON unmarshal field
func (r Record) JSON(at int, o interface{}) error {
	return json.Unmarshal(r.Bytes(at), o)
}

//Play plays the record on the given meta manager. Note that error is only
//returned if the underlying meta manager return an error. A panic
//is thrown if the record is corrupt
func (r Record) Play(store meta.Store) error {
	var err error

	switch r.Action() {
	case OperationSet:
		err = store.Set(r.Path(1), r.Bytes(2))
	case OperationDel:
		err = store.Del(r.Path(1))
	case OperationLink:
		err = store.Link(r.Path(1), r.Path(2))
	case OperationTest:

	}

	return err
}

type zdbRecorder struct {
	p *Pool
	s string
	m sync.Mutex
}

//NewZDB create a law level zdb connection pool
func newZDBRecorder(address, namespace, password, state string) (*zdbRecorder, error) {
	pool := &zdbRecorder{
		s: state,
		p: newZDBPool(address, namespace, password),
	}

	return pool, pool.test()
}

func (z *zdbRecorder) test() error {
	/*
		To test a connection, we make sure we actually write a Record with "test" action
		We don't just write any data, because slave minios are waiting for any write on tlog
		namespace, and if they received a "data" object that can't be interpreted they will
		fail. To avoid confusing an actual corrupt data with test data we introduced a test
		record witch is just ignored by the syncher
	*/
	key, err := z.Record(Record{OperationTest}, false)
	if err != nil {
		return err
	}

	con := z.p.Get()
	defer con.Close()

	return con.Send("DEL", key)
}

func (z *zdbRecorder) Record(record Record, setState bool) ([]byte, error) {
	con := z.p.Get()
	defer con.Close()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.Encode(record); err != nil {
		return nil, err
	}

	bytes, err := redis.Bytes(con.Do("SET", emptyKey, buf.Bytes()))
	if err != nil {
		log.WithFields(log.Fields{
			"subsystem": "tlog",
			"tlog":      z.p.address,
			"namespace": z.p.namespace,
			"master":    false,
		}).WithError(err).Error("failed to write transaction log")
		return nil, err
	}

	if setState {
		return bytes, z.SetState(bytes)
	}

	return bytes, nil
}

func (z *zdbRecorder) Begin() {
	z.m.Lock()
}

func (z *zdbRecorder) End() {
	z.m.Unlock()
}

func (z *zdbRecorder) SetState(key []byte) error {
	return ioutil.WriteFile(z.s, key, 0600)
}

func (z *zdbRecorder) state() ([]byte, error) {
	z.m.Lock()
	defer z.m.Unlock()

	key, err := ioutil.ReadFile(z.s)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return key, nil
}

func (z *zdbRecorder) Play(key []byte, cb func([]byte, Record) error) error {
	var err error
	if key == nil {
		key, err = z.state()
		if err != nil {
			return err
		}
	}

	return z.p.play(key, cb)
}
