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

//JSON unmarshal field
func (r Record) JSON(at int, o interface{}) error {
	return json.Unmarshal(r.Bytes(at), o)
}

//Play plays the record on the given meta sotre. Note that error is only
//returned if the underlying bucket or storage manager return an error. A panic
//is thrown if the record is corrupt
func (r Record) Play(meta meta.Manager) error {
	var err error
	// switch r.Action() {
	// case OperationBucketCreate:
	// 	err = bucket.Create(r.String(1))
	// 	if err == meta.ErrReservedBucketName {
	// 		err = Warning{err}
	// 	} else if _, ok := err.(minio.BucketExists); ok {
	// 		err = Warning{err}
	// 	}
	// case OperationBucketDelete:
	// 	err = bucket.Del(r.String(1))
	// case OperationBucketSetPolicy:
	// 	var pol policy.Policy
	// 	if err = r.JSON(2, &pol); err != nil {
	// 		return err
	// 	}
	// 	err = bucket.SetPolicy(r.String(1), pol)
	// case OperationObjectSet:
	// 	err = storage.Set(r.Bytes(1), r.Bytes(2), r.Bytes(3))
	// case OperationObjectDel:
	// 	err = storage.Delete(r.Bytes(1), r.Bytes(2))
	// case OperationTest:
	// default:
	// 	err = fmt.Errorf("unknown record action: %s", r.Action())
	// }

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
	key, err := z.Record(Record{OperationTest})
	if err != nil {
		return err
	}

	con := z.p.Get()
	defer con.Close()

	return con.Send("DEL", key)
}

func (z *zdbRecorder) Record(record Record) ([]byte, error) {
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
	}

	return bytes, err
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
