package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/threefoldtech/0-stor/client"
	"github.com/threefoldtech/0-stor/client/datastor"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline"
	"gopkg.in/yaml.v2"
)

const (
	//EnvShards is shards config in format ns:password@ip:port
	EnvShards = "SHARDS"
	//EnvBlockSize is blocksize in bytes default to 4M (4194304 bytes)
	EnvBlockSize = "BLOCKSIZE"
	//EnvParityShards number of parity shards
	EnvParityShards = "PARITY"
	//EnvDataShards number of data shards
	EnvDataShards = "DATA"

	//EnvAccessKey minio access key
	EnvAccessKey = "ACCESS_KEY"
	//EnvSecretKey minio secret key
	EnvSecretKey = "SECRET_KEY"

	// EnvTlogServer tlog server
	EnvTlogServer = "TLOG"
	// EnvMaster master
	EnvMaster = "MASTER"
)

func env(k, d string) string {
	v := os.Getenv(k)
	if len(v) == 0 {
		return d
	}

	return v
}

func shards(s string) ([]datastor.ShardConfig, error) {
	var shards []datastor.ShardConfig
	for _, str := range strings.Split(s, ",") {
		u, err := url.Parse(fmt.Sprintf("zdb://%s", str))
		if err != nil {
			return nil, fmt.Errorf("failed to parse shard '%s': %s", str, err)
		}

		if u.User == nil {
			return nil, fmt.Errorf("failed to parse shard '%s' expected format is 'namespace:password@ip:port", str)
		}

		password, _ := u.User.Password()

		shards = append(
			shards,
			datastor.ShardConfig{
				Address:   u.Host,
				Namespace: u.User.Username(),
				Password:  password,
			},
		)
	}

	return shards, nil
}

func toTlogConfig(s string) (*config.TLog, error) {
	if s == "" {
		return nil, nil
	}
	u, err := url.Parse(fmt.Sprintf("zdb://%s", s))
	if err != nil {
		return nil, fmt.Errorf("failed to parse tlog config '%s': %s", s, err)
	}

	if u.User == nil {
		return nil, fmt.Errorf("failed to parse tlog config '%s' expected format is 'namespace:password@ip:port", s)
	}

	password, _ := u.User.Password()

	return &config.TLog{
		Address:   u.Host,
		Password:  password,
		Namespace: u.User.Username(),
	}, nil
}

// this entrypoint will build min-io config from ENV vars for zerostor
func main() {
	for _, e := range os.Environ() {
		fmt.Println(e)
	}

	shards, err := shards(env(EnvShards, ""))
	if err != nil {
		log.Fatalf("failed to parse shards: %s", err)
	}

	if len(shards) == 0 {
		log.Fatal("SHARDS is required")
	}

	blockSize, err := strconv.Atoi(env(EnvBlockSize, "4194304"))
	if err != nil {
		log.Fatal("failed to parse BLOCKSIZE expected an int", err)
	}

	dataShards, err := strconv.Atoi(env(EnvDataShards, ""))
	if err != nil {
		log.Fatal("DATA is required and must be an int")
	}
	parityShards, err := strconv.Atoi(env(EnvParityShards, ""))
	if err != nil {
		log.Fatal("PARITY is required and must be an int")
	}

	tlogConfig, err := toTlogConfig(env(EnvTlogServer, ""))
	if err != nil {
		log.Fatalf("failed to parse shards: %s", err)
	}

	masterConfig, err := toTlogConfig(env(EnvMaster, ""))
	if err != nil {
		log.Fatalf("failed to parse shards: %s", err)
	}

	dataStorConfig := client.DataStorConfig{
		Shards: shards,
		Pipeline: pipeline.Config{
			BlockSize: blockSize,
			Distribution: pipeline.ObjectDistributionConfig{
				DataShardCount:   dataShards,
				ParityShardCount: parityShards,
			},
		},
	}

	cfg := config.Config{
		Config: client.Config{
			DataStor: dataStorConfig,
		},
	}
	cfg.Minio.TLog = tlogConfig
	cfg.Minio.Master = masterConfig

	p := filepath.Join(os.TempDir(), "minio.yaml")
	f, err := os.Create(p)
	if err != nil {
		log.Fatal("failed to create config file", err)
	}

	enc := yaml.NewEncoder(f)
	if err := enc.Encode(cfg); err != nil {
		log.Fatalf("failed to write config: %s", err)
	}

	f.Close()

	os.Setenv("MINIO_ZEROSTOR_CONFIG_FILE", p)
	os.Setenv("MINIO_ACCESS_KEY", env(EnvAccessKey, ""))
	os.Setenv("MINIO_SECRET_KEY", env(EnvSecretKey, ""))
	os.Setenv("MINIO_ZEROSTOR_META_DIR", "/data")
	os.Setenv("MINIO_UPDATE", "off")

	if err := syscall.Exec("/bin/minio", []string{"minio", "gateway", "zerostor"}, os.Environ()); err != nil {
		log.Fatalf("failed to exec minio: %s", err)
	}
}
