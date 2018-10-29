from random import randint
from base_test import BaseTest
from unittest import skip
import time


class TestTlog(BaseTest):
    @skip('https://github.com/threefoldtech/0-templates/issues/179')
    def test001_upload_stop_tlog_start_download(self):
        """

        test001_upload_stop_tlog_start_download
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

    @skip('https://github.com/threefoldtech/0-templates/issues/179')
    def test002_stop_tlog_upload_download(self):
        """

        test002_stop_tlog_upload_download
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

    def test003_upload_kill_tlog_download(self):
        """

        test003_upload_kill_tlog_download
         - Upload file
         - kill tlog
         - wait 60 sec, tlog should be returned
         - Download file, should succeed
        """
        self.file_name = self.upload_file()
        md5_before = self.file_name

        self.assertFalse(self.s3.failures.kill_tlog())
        time.sleep(60)

        md5_after = self.download_file(file_name=self.file_name, keep_trying=True)
        self.assertEqual(md5_after, md5_before)

    def test004_kill_tlog_upload_download(self):
        """

        test004_kill_tlog_upload_download
         - kill tlog
         - wait 60 sec, tlog should be returned
         - Upload file
         - Download file, should succeed
        """
        self.assertFalse(self.s3.failures.kill_tlog())
        time.sleep(60)

        self.file_name = self.upload_file()
        md5_before = self.file_name

        md5_after = self.download_file(file_name=self.file_name, keep_trying=True)
        self.assertEqual(md5_after, md5_before)

    def test005_upload_tlog_die_forever_download(self):
        """

        test005_upload_tlog_die_forever_download
         - Upload file
         - Make tlog die forever
         - Wait for 60 sec.
         - Download file, should succeed
        """
        self.file_name = self.upload_file()
        md5_before = self.file_name

        self.assertFalse(self.s3.failures.tlog_die_forever())
        time.sleep(60)

        md5_after = self.download_file(file_name=self.file_name, keep_trying=True)
        self.assertEqual(md5_after, md5_before)

    def test006_tlog_die_forever_upload_download(self):
        """

         - Make tlog die forever
         - Wait for 60 sec.
         - Upload file, should succeed
         - Download file, should succeed
        """
        self.file_name = self.upload_file()
        md5_before = self.file_name

        self.assertFalse(self.s3.failures.tlog_die_forever())
        time.sleep(60)

        md5_after = self.download_file(file_name=self.file_name, keep_trying=True)
        self.assertEqual(md5_after, md5_before)

