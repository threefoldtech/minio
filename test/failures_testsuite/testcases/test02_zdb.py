from random import randint
from base_test import BaseTest


class ZDBFailures(BaseTest):
    def tearDown(self):
        super().tearDown()

    def test001_zdb_kill(self):
        """
        - upload file, should pass
        - kill zdb process  and make sure it will restart automatically.
        - download uploaded file, should pass
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('kill zdb process and make sure it will restart automatically')
        flag = self.s3.failures.zdb_process_down()
        self.assertTrue(flag, "zdb didn't restart")

        self.logger.info("Download uploaded file, and check that both are same.")
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    def test002_upload_stop_parity_zdb_download(self):
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
        file_name, bucket_name, md5_before = self.s3.upload_file()
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('Stop {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_down(count=self.s3.parity)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('Start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_up(count=self.s3.parity)

    def test003_stop_parity_zdb_upload_download_start(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - download file, should pass
        - assert md5 checksum is matching
        - start n zdb, should pass
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_down(count=self.s3.parity)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info(' Start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_up(count=self.s3.parity)

    def test004_stop_parity_zdb_upload_start_download(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - start n zdb, should pass
        - download file, should pass
        - assert md5 checksum is matching
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_down(count=self.s3.parity)

        self.logger.info(' Start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_up(count=self.s3.parity)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    def test005_stop_greater_parity_zdb_upload(self):
        """
        - Upload file, should succeed
        - Stop n+ zdb, n = parity, should succeed
        - Upload file, should fail
        - Download the uploaded file, should succeed
        - Start n+ zdb
        """
        self.s3.upload_file()
        zdb_turn_down = self.s3.parity + randint(1, self.s3.shards)
        self.logger.info(' Stop {} zdb'.format(zdb_turn_down))
        self.s3.failures.zdb_down(count=zdb_turn_down)

        self.logger.info('Uploading should raise an error')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.logger.info(' Start {} zdb'.format(zdb_turn_down))
        self.s3.failures.zdb_up(count=zdb_turn_down)
