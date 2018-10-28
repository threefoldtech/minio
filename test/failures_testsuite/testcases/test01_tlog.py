from random import randint
from base_test import BaseTest
from unittest import skip
import time


class TestTlog(BaseTest):
    def setUp(self):
        super().setUp()
        if not self.s3.failures.tlog_status():
            self.s3.failures.tlog_up()

    def tearDown(self):
        super().tearDown()

    @skip('https://github.com/threefoldtech/0-templates/issues/179')
    def test001_upload_stop_tlog_start_download(self):
        """
         - Upload file
         - stop tlog
         - Try to download, should fail
         - Start tlog
         - Download file, should succeed
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.s3.failures.tlog_down()
        time.sleep(60)

        self.assertIsNone(self.download_file(file_name=self.file_name))

        self.s3.failures.tlog_up()

        for _ in range(10):
            self.logger.info('wait till tlog  be up')
            if self.s3.failures.tlog_status():
                break
            else:
                time.sleep(60)
        else:
            self.assertTrue(self.s3.failures.tlog_status())

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    @skip('https://github.com/threefoldtech/0-templates/issues/179')
    def test002_stop_tlog_upload_download(self):
        """
         - Stop tlog
         - Upload file, should fail
         - Start tlog
         - Download file, should succeed
        """
        self.s3.failures.tlog_down()
        time.sleep(60)
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()
            self.logger.info("Can't upload the file")

        self.s3.failures.tlog_up()
        for _ in range(10):
            self.logger.info('wait till tlog  be up')
            if self.s3.failures.tlog_status():
                break
            else:
                time.sleep(60)
        else:
            self.assertTrue(self.s3.failures.tlog_status())

        file_name, bucket_name, md5_before = self.s3.upload_file()

        md5_after = self.s3.download_file(file_name, bucket_name, timeout=250)
        self.assertEqual(md5_after, md5_before)

    def test003_upload_kill_tlog_download(self):
        """
         - Upload file
         - kill tlog
         - wait 60 sec, tlog should be returned
         - Download file, should succeed
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.assertFalse(self.s3.failures.kill_tlog())
        time.sleep(60)

        md5_after = self.s3.download_file(file_name, bucket_name, timeout=250)
        self.assertEqual(md5_after, md5_before)

    def test004_kill_tlog_upload_download(self):
        """
         - kill tlog
         - wait 60 sec, tlog should be returned
         - Upload file
         - Download file, should succeed
        """
        self.assertFalse(self.s3.failures.kill_tlog())
        time.sleep(60)

        file_name, bucket_name, md5_before = self.s3.upload_file()

        md5_after = self.s3.download_file(file_name, bucket_name, timeout=250)
        self.assertEqual(md5_after, md5_before)
