package tlog

import (
	"context"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
)

//Pool is a zdb pool with extra functionality to build other components
type Pool struct {
	*redis.Pool
	address   string
	namespace string
}

func newZDBPool(address, namespace, password string) *Pool {
	return &Pool{
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				con, err := redis.Dial("tcp", address)
				if err != nil {
					return nil, err
				}

				return con, con.Send("SELECT", namespace, password)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < 5*time.Second {
					return nil
				}
				_, err := c.Do("PING")
				return err
			},
			MaxActive: 20,
			Wait:      true,
		},
		address:   address,
		namespace: namespace,
	}
}

func (z *Pool) play(key []byte, cb func([]byte, Record) error) error {
	var err error
	sCon := z.Get()
	defer sCon.Close()

	gCon := z.Get()
	defer gCon.Close()

	var cursor []byte

	if key != nil {
		cursor, err = redis.Bytes(sCon.Do("KEYCUR", key))
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for sca := range z.scan(ctx, sCon, cursor) {
		if sca.Err != nil {
			return sca.Err
		}

		recData, err := redis.Bytes(gCon.Do("GET", sca.Key))
		if err != nil {
			return err
		}
		var rec Record

		if err := msgpack.Unmarshal(recData, &rec); err != nil {
			return err
		}

		if err := cb(sca.Key, rec); err != nil {
			return err
		}
	}

	return nil
}

type scan struct {
	Key       []byte
	Size      int64
	Timestamp int64
	Err       error
}

//scan is a generic zdb scan method that takes a cursor and keey scanning the given cursor until
//ctx is canceled or no more records to retrieve
func (z *Pool) scan(ctx context.Context, con redis.Conn, cursor []byte) <-chan scan {
	result := make(chan scan)

	go func() {
		defer close(result)

		push := func(rec scan) bool {
			select {
			case result <- rec:
				return true
			case <-ctx.Done():
				return false
			}
		}

		for {
			var ret []interface{}
			var err error
			var rec scan

			//var dest []Scan
			if len(cursor) == 0 {
				ret, err = redis.Values(con.Do("SCAN"))
			} else {
				ret, err = redis.Values(con.Do("SCAN", cursor))
			}

			if err != nil && err.Error() == "No more data" {
				break
			} else if err != nil {
				rec.Err = err
				push(rec)
				return
			}

			cursor = ret[0].([]byte)

			for _, obj := range ret[1].([]interface{}) {
				if line, ok := obj.([]interface{}); ok {
					rec := scan{
						Key:       line[0].([]byte),
						Size:      line[1].(int64),
						Timestamp: line[2].(int64),
					}

					if !push(rec) {
						return
					}
				}
			}
		}
	}()

	return result
}
