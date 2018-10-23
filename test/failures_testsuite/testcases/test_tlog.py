from random import randint
from base_test import BaseTest
import time


class TestTlog(BaseTest):
    def tearDown(self):
        self.s3.failures.tlog_up()
        super().tearDown()

    def test001_upload_stop_tlog_start_download(self):
        """
         - Upload file
         - stop tlog
         - Try to download, should fail
         - Start tlog
         - Download file, should succeed
        """
        self.file_name = self.upload_file()
        md5_before = self.file_name

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

        md5_after = self.download_file(file_name=self.file_name, keep_trying=True)
        self.assertEqual(md5_after, md5_before)

    def test002_stop_tlog_upload_download(self):
        """
         - Stop tlog
         - Upload file, should fail
         - Start tlog
         - Download file, should succeed
        """
        self.s3.failures.tlog_down()
        time.sleep(60)
        self.assertIsNone(self.upload_file())

        self.s3.failures.tlog_up()
        for _ in range(10):
            self.logger.info('wait till tlog  be up')
            if self.s3.failures.tlog_status():
                break
            else:
                time.sleep(60)
        else:
            self.assertTrue(self.s3.failures.tlog_status())

        self.file_name = self.upload_file()
        md5_before = self.file_name

        md5_after = self.download_file(file_name=self.file_name, keep_trying=True)
        self.assertEqual(md5_after, md5_before)

