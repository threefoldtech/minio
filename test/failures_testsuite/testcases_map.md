# 0-db level:
## data 0-db temporary unavailable
```
    def test001_upload_stop_parity_zdb_download(self):
        """
        - upload 2M, should succeed.
        - Download file, should succeed
        - Deleted the downloaded file
        - assert md5 checksum is matching
        - Stop n zdb, n <= parity
        - Download file, should succeed
        - assert md5 checksum is matching
        - Start n zdb
        """

    def test002_stop_parity_zdb_upload_download_start(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - download file, should pass
        - assert md5 checksum is matching
        - start n zdb, should pass
        """

    def test003_stop_parity_zdb_upload_start_download(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - start n zdb, should pass
        - download file, should pass
        - assert md5 checksum is matching
        """

    def test004_stop_greater_parity_zdb_upload(self):
        """
        - Upload file, should succeed
        - Stop n+ zdb, n = parity, should succeed
        - Upload file, should fail
        - Download the uploaded file, should succeed
        - Start n+ zdb
        """
```

## Metadata 0-db temporary unavailable
```
    def test001_upload_stop_tlog_start_download(self):
        """
         - Upload file
         - stop tlog
         - Try to download, should fail
         - Start tlog
         - Download file, should succeed
        """

    def test002_stop_tlog_upload_download(self):
        """
         - Stop tlog
         - Upload file, should fail
         - Start tlog
         - Download file, should succeed
        """

```

