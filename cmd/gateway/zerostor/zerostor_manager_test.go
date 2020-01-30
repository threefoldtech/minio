package zerostor

import (
	"bytes"
	"crypto/rand"

	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/metastor/db"
	"github.com/threefoldtech/0-stor/client/metastor/metatypes"

	"testing"
)

func TestZerostorRoundTrip(t *testing.T) {
	const (
		bkt       = "bucket"
		namespace = "ns"
		object    = "myKey"
		dataLen   = 4096
	)
	var (
		data = make([]byte, dataLen)
	)
	rand.Read(data)

	zstor, cfg, cleanup := newTestZsManager(t, namespace)
	defer cleanup()

	cli := zstor.GetClient()
	defer cli.Close()

	// make sure the object does not exist yet
	buf := bytes.NewBuffer(nil)
	err := cli.Read(&metatypes.Metadata{Namespace: []byte(namespace), Key: cli.getKey(bkt, object), Chunks: []metatypes.Chunk{{Objects: []metatypes.Object{{ShardID: cfg.DataStor.Shards[0].Address}}}}}, buf, 0, dataLen)
	if err != datastor.ErrKeyNotFound {
		t.Fatalf("expect error: %v, got: %v", db.ErrNotFound, err)
	}
	buf.Reset()

	// set
	metaData, err := cli.Write(bkt, object, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	if !bytes.Equal(metaData.Key, cli.getKey(bkt, object)) {
		t.Fatalf("metadata key is not valid")
	}

	// get
	err = cli.Read(metaData, buf, 0, dataLen)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(data, buf.Bytes()) {
		t.Fatalf("data read is not valid")
	}

	// delete
	err = cli.Delete(*metaData)
	if err != nil {
		t.Fatalf("delete data failed error: %v", err)
	}

	// make sure the object is not exist anymore
	err = cli.Read(metaData, buf, 0, dataLen)
	if err != datastor.ErrKeyNotFound {
		t.Fatalf("expect error: %v, got: %v", datastor.ErrKeyNotFound, err)
	}

}
