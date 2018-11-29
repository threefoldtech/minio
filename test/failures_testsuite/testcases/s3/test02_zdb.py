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
        - upload 2M, should succeed.
        - Download file, should succeed
        - assert md5 checksum is matching
        - Stop n zdb, n = parity namespaces
        - Upload a file, should fail
        - Download the old file, should succeed
        - assert md5 checksum is matching
        - Start n zdb
        """
        self.logger.info('upload file')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('download file')
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('stop {} zdb services'.format((self.s3.parity)))
        self.s3.failures.zdb_stop_service(count=self.s3.parity)

        self.logger.info('upload a file, should fail')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.logger.info('download the old file, should succeed')
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('start parity zdb services')
        self.logger.info('start {} zdb'.format((self.s3.parity)))
        self.s3.failures.zdb_start_service(count=self.s3.parity)

    def test004_stop_parity_zdb_upload_start_download(self):
        """

        test004_stop_parity_zdb_upload_start_download
        - Stop n zdb, n = parity
        - upload file, should fail
        - start n zdb, should pass
        - upload file, should succeed
        - download file, should pass
        - assert md5 checksum is matching
        """
        self.logger.info('stop {} zdb services'.format((self.s3.parity)))
        self.s3.failures.zdb_stop_service(count=self.s3.parity)

        self.logger.info('upload a file, should fail')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.logger.info('start {} zdb services'.format((self.s3.parity)))
        self.s3.failures.zdb_start_service(count=self.s3.parity)

        self.logger.info('upload file, should pass')
        file_name, bucket_name, md5_before = self.s3.upload_file()

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
        self.logger.info('upload file, should pass')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        zdb_turn_down = self.s3.parity + randint(1, self.s3.shards)
        self.logger.info('stop {} zdb services'.format(zdb_turn_down))
        self.s3.failures.zdb_stop_service(count=zdb_turn_down)

        self.logger.info('upload a file, should fail')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.logger.info('start {} zdb services'.format(zdb_turn_down))
        self.s3.failures.zdb_start_service(count=zdb_turn_down)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    def test006_upload_stopallzdb_check_start(self):
        """

        test006_upload_stopallzdb_check_start
        - Upload file, should succeed
        - stop all zdbs services
        - Upload file, should fail
        - Downalod file, should fail
        - Start all zdbs
        - Download old file, should pass
        - Upload file should pass
        - Download new file, should succeed
        """
        self.logger.info('upload file, should pass')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.namespaces = self.s3.data['namespaces']
        self.logger.info('stop {} zdb services'.format(len(self.namespaces)))
        self.s3.failures.zdb_stop_service(count=self.namespaces)

        self.logger.info('uploading should raise an error')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.logger.info('start {} zdb services'.format(self.namespaces))
        self.s3.failures.zdb_start_service(count=self.namespaces)

        self.logger.info('download old file, should pass')
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('upload file, should pass')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('download new file, should pass')
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)
