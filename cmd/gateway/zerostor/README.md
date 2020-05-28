# Minio Gateway for 0-stor

Minio gateway for [0-stor](https://github.com/zero-os/0-stor) using [0-db](https://github.com/zero-os/0-db)
as storage server.

## Metadata

This gateway has two kind of metadata : mongodb and local filesystem.

When using local filesystem as metadata, the directory of the metadata
location is specified in `MINIO_ZEROSTOR_META_DIR` environment variable.

0-stor provide metadata using `ETCD` and `Badger`, but this gateway doesn't use them because they
don't have directory-like behavior.

Metadata will be encrypted with AES encryption if user provide private key through `MINIO_ZEROSTOR_META_PRIVKEY` environment variable.
What specific AES algorithm is used, depends on the given key size:
- 16 bytes: AES_128
- 24 bytes: AES_192
- 32 bytes: AES_256


## Bucket Management

Because 0-stor doesn't have `bucket` concept, all bucket management are handled by
the gateway, including the bucket policy.

## Config Reload

This gateway configuration could be reloaded by sending `SIGHUP` to the process.

## Running

### Build

`make`

### Check options

`minio gateway zerostor -h`

### 0-stor Gateway Configuration

Other than 0-stor client config, all configuration are done through environment variables.
We use environment variables to make it consistent with other Minio gateways.

Standard minio environment variables:
- `MINIO_ACCESS_KEY` : Your minio Access key
- `MINIO_SECRET_KEY` : Your minio Secret key

0-stor gateway environment variables:
- `MINIO_ZEROSTOR_CONFIG_FILE` :  Zerostor config file(default : $MINIO_CONFIG_DIR/zerostor.yaml). Default minio config dir is $HOME/.minio
- `MINIO_ZEROSTOR_META_DIR`     Zerostor metadata directory(default : $MINIO_CONFIG_DIR/zerostor_meta)
- `MINIO_ZEROSTOR_META_PRIVKEY` Zerostor metadata private key(default : ""). Metadata won't be encrypted if the key is not provided
- `MINIO_ZEROSTOR_DEBUG`        Zerostor debug flag. Set to "1" to enable debugging (default : 0)

Put your 0-stor config file as specified by `MINIO_ZEROSTOR_CONFIG_FILE`.

### Run 0-db server


```
./bin/zdb --mode direct --port 12345
```

### example configuration

```yaml
namespace: thedisk
password: kunci
datastor: # required
  # the address(es) of a zstordb cluster (required0)
  shards: # required
    - address: 127.0.0.1:12345
      # namespace: <namespace> overrides namespace name for this instance
      # password: <password> overrides password for this instance
  pipeline:
    block_size: 4096
    compression: # optional, snappy by default
      type: snappy # snappy is the default, other options: lz4, gzip
      mode: default # default is the default, other options: best_speed, best_compression
    encryption: # optional, disabled by default
      type: aes # aes is the default and only standard option
      private_key: ab345678901234567890123456789012
```

## Transaction log
Under `minio` you can define 2 entries that sets up transaction logging. Transaction logging us useful in case of filesystem corruption

```yaml
minio:
  tlog:
    address: <ip>:<port>
    namespace: zdb namespace
    password: zdb namespace password
  master:
    address: <ip>:<port>
    namespace: zdb namespace
    password: zdb namespace password
```

both `tlog` and `master` are optional and they work as following:

If `tlog` is defined, any write operation to minio (file upload, bucket creation/deletion, etc...) are logged to zdb instance defined as `tlog`
If `master` is defined, master must be a `tlog` of another minio instances. In that case the instance that configures this zdb as master will
duplicate all changes to the `master` zdb to it's local `meta`. If this minio has `tlog` defined, the master records will also get replicated to
the `tlog`.


### Run the gateway

`./minio gateway zerostor`

## Simple test using minio client


### install minio client

Follow this guide https://docs.minio.io/docs/minio-client-complete-guide for the guide

### Configure minio client to use our server

When we run the server, the server is going to print these lines
```
....
Command-line Access: https://docs.minio.io/docs/minio-client-quickstart-guide
   $ mc config host add myzerostor http://192.168.194.249:9000 XXX YYYYYY
....
```
See your screen, and type the `mc config ...` command printed there.

### list all buckets/namespace
```
$ mc ls myzerostor
[1970-01-01 07:00:00 WIB]     0B newbucket/
```

### upload file
```
$ mc cp minio myzerostor/newbucket
minio:                                    24.95 MB / 24.95 MB ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 100.00% 29.54 MB/s 0s
```

### check file exist
```
$ mc ls myzerostor/newbucket
[2018-02-20 11:02:01 WIB]  25MiB minio
```
### download the file
```
mc cat myzerostor/newbucket/minio > /tmp/bin-minio
```

Check the downloaded file is not corrupt
```
cmp minio /tmp/bin-minio
```

## Healer API
the gateway provide another API (beside minio api) that can be used to run a full check on all the objects, or a full bucket, or a single object.
The API provide options so you can do a check without fixing, or check and fix (default). The check or check and fix jobs can either run in the foreground
or in the background

### Config
Add `healer` section to the `minio` config like
```yaml
minio:
  healer:
    listen: 0.0.0.0:9010
```

> If not set default value for `listen` is `0.0.0.0:9010`

### Foreground jobs
A foreground job will stream back a full detailed report of each check, fix or error that happens on the system while doing the healing. It's up for the client to read this report and process, or aggregate it the way they see fit. A disconnection of the client will stop the job.
### Background jobs
A background job can run in the background, with no detailed report. instead it will keep a summary status of the process. The client can later check on the job status once in a while until it's completely done. Then they can read the summary and drop the job report later on.

### API
```
POST /repair
POST /repair/{bucket}
POST /repair/{bucket}/{object}

GET    /jobs
DELETE /jobs/{id}
```

> All `repair` endpoints accepts optional query `dry-run` and `bg`

#### Query options
- `dry-run` runs a check with no attempt to fix. This flag will make minio report the current status of the zerostore gateway.
- `bg` short for `background` as explained before, this will not produce a report instead only keep a summary of number of objects.

### Example report
running the following curl command in your shell
```
curl -XPOST "localhost:9010/repair/large"
```

Will produce some output like this

```
blob: 18dcb41b-b998-4d8f-ac61-85e02497c0f8 (status: optimal, repaired: false)
object: large/how_to_get_tft.mp4 (blobs: 1, optimal: 1, valid: 0, invalid: 0, errors: 0, repaired: 0)
blob: <empty> (status: optimal, repaired: false)
object: large/subdir/ (blobs: 1, optimal: 1, valid: 0, invalid: 0, errors: 0, repaired: 0)
blob: 1f8168e7-12e0-4824-abcf-b6060db8b06d (status: optimal, repaired: false)
blob: b3f81564-13d7-40ed-9378-d5551a3ece49 (status: optimal, repaired: false)
blob: ff84d290-79ab-49e7-b5a7-6a952979d02a (status: optimal, repaired: false)
blob: 72988083-fc81-4654-8795-32f6377e942f (status: optimal, repaired: false)
blob: dcf03c3d-0c50-4d43-8d35-5cabd8546510 (status: optimal, repaired: false)
blob: 0d2ab8cb-1aee-477a-9c38-9a0596d09df3 (status: optimal, repaired: false)
blob: 3759d5eb-9bc0-4365-a406-6369eab360b9 (status: optimal, repaired: false)
blob: bb4f72c0-fe0a-4016-81f9-e56b66da5e58 (status: optimal, repaired: false)
object: large/subdir/movie.mp4 (blobs: 8, optimal: 8, valid: 0, invalid: 0, errors: 0, repaired: 0)
bucket: large
```

#### How to read the report
Due to the streaming nature of the report, reports makes more sense if they are reading down up. Because we need to check all blobs of a single object before we can give statistics about the object. So blobs of an object comes before the object stats itself.

Also directories are "fake" entries in minio. So they are also checked, while they have one blob that actually doesn't container any data. Hence directories blobs always have `blob: <empty>` and they are always `optimal`. so you can always skip these ones. Again notice that the blob also comes before the directory object.

### Example client
Using Curl

```bash
# repairs full installation
curl -XPOST "localhost:9010/repair"

# repairs bucket in the background
curl -XPOST "localhost:9010/repair/mybucket?bg=1"

# report status of a single object
curl -XPOST "localhost:9010/repair/mybucket/path/to/object?dry-run=1"

# list jobs
curl "localhost:9010/jobs" | json_pp
```

> Note: the value passed to both `bg` and `dry-run` are not actually checked, once the argument has any value, it considered as set.


In Go. Note that this is just an example no decent error handling.
> Note: the statistics returned on the job list is only valid for Jobs of type `background`

```go
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

func main() {
	response, err := http.Post("http://localhost:9010/repair/mybucket?bg=1", "", nil)
	if err != nil {
		panic(err)
	}

	if response.StatusCode != http.StatusOK {
		panic(fmt.Errorf("invalid status code: %s", response.Status))
	}

	defer response.Body.Close()
	id, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println("Job started with id: ", string(id))

	// list jobs

	type Job struct {
		Location string `json:"location"`
		Type     string `json:"type"`
		Blobs    struct {
			Checked  uint64 `json:"checked"`
			Optimal  uint64 `json:"optimal"`
			Valid    uint64 `json:"valid"`
			Invalid  uint64 `json:"invalid"`
			Repaired uint64 `json:"repaired"`
			Errors   uint64 `json:"errors"`
		} `json:"blobs"`

		Objects struct {
			Checked uint64 `json:"checked"`
		} `json:"objects"`

		Running bool `json:"running"`
	}

	var results map[string]Job

	response, err = http.Get("http://localhost:9010/jobs")
	if err != nil {
		panic(err)
	}

	if response.StatusCode != http.StatusOK {
		panic(fmt.Errorf("invalid status code: %s", response.Status))
	}

	defer response.Body.Close()

	dec := json.NewDecoder(response.Body)

	if err := dec.Decode(&results); err != nil {
		panic(err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	enc.Encode(results)
}

```
