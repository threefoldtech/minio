from random import randint
from base_test import BaseTest


class ZDBFailures(BaseTest):
    def test001_zdb_kill(self):
        """

        test001_zdb_kill
        - upload file, should pass
        - kill zdb process  and make sure it will restart automatically.
        - download uploaded file, should pass
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('kill zdb process and make sure it will restart automatically')
        flag = self.s3.failures.zdb_kill_job()
        self.assertTrue(flag, "zdb didn't restart")

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    def test002_upload_stop_parity_zdb_download(self):
        """

        test002_upload_stop_parity_zdb_download
        - upload 1M, should succeed.
        - Download file, should succeed
        - Deleted the downloaded file
        - assert md5 checksum is matching
        - Stop n zdb, n <= parity
        - Download file, should succeed
        - assert md5 checksum is matching
        - Start n zdb
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('Stop {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_stop_service(count=self.s3.parity)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('Start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_start_service(count=self.s3.parity)

    def test003_stop_parity_zdb_upload_download_start(self):
        """

        test003_stop_parity_zdb_upload_download_start
        - Stop n zdb, n <= parity
        - upload file, should pass
        - download file, should pass
        - assert md5 checksum is matching
        - start n zdb, should pass
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        self.logger.info('stop {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_stop_service(count=self.s3.parity)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_start_service(count=self.s3.parity)

    def test004_stop_parity_zdb_upload_start_download(self):
        """

        test004_stop_parity_zdb_upload_start_download
        - Stop n zdb, n <= parity
        - upload file, should pass
        - start n zdb, should pass
        - download file, should pass
        - assert md5 checksum is matching
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        self.logger.info('stop {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_stop_service(count=self.s3.parity)

        self.logger.info('start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_start_service(count=self.s3.parity)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    def test005_stop_greater_parity_zdb_upload(self):
        """

        test005_stop_greater_parity_zdb_upload
        - Upload file, should succeed
        - Stop n+ zdb, n = parity, should succeed
        - Upload file, should fail
        - Start n+ zdb
        - Download the uploaded file, should succeed
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        zdb_turn_down = self.s3.parity + randint(1, self.s3.shards)
        self.logger.info('stop {} zdb'.format(zdb_turn_down))
        self.s3.failures.zdb_stop_service(count=zdb_turn_down)

        self.logger.info('uploading should fail')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.logger.info('start {} zdb'.format(zdb_turn_down))
        self.s3.failures.zdb_start_service(count=zdb_turn_down)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)
