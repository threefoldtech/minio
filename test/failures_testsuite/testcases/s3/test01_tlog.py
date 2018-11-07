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
         - Try to download, should success
         - Start tlog
         - Download file, should success
        """

        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.s3.failures.tlog_down()
        time.sleep(60)
        self.assertEqual(self.s3.download_file(file_name, bucket_name), md5_before)

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

        test002_stop_tlog_upload_download
         - Stop tlog
         - Upload file, should fail
         - Start tlog
         - Upload file, should success
         - Download file, should success
        """
        self.s3.failures.tlog_down()
        time.sleep(60)
        self.logger.info('Upload file, should fail')
        with self.assertRaises(RuntimeError):
            self.s3.upload_file()

        self.s3.failures.tlog_up()
        for _ in range(10):
            self.logger.info('wait till tlog  be up')
            if self.s3.failures.tlog_status():
                break
            else:
                time.sleep(60)
        else:
            self.assertTrue(self.s3.failures.tlog_status())

        self.logger.info('Upload file, should success')
        file_name, bucket_name, md5_before = self.s3.upload_file()
        self.logger.info('Download file, should success')
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    def test003_upload_kill_tlog_download(self):
        """

        test003_upload_kill_tlog_download
         - Upload file
         - kill tlog
         - wait 60 sec, tlog should be returned
         - Download file, should succeed
        """
        self.logger.info('Upload file')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.assertFalse(self.s3.failures.kill_tlog())
        time.sleep(60)

        self.logger.info('Download file, should succeed')
        md5_after = self.s3.download_file(file_name, bucket_name)
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

        file_name, bucket_name, md5_before = self.s3.upload_file()

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    @skip('https://github.com/threefoldtech/0-templates/issues/186')
    def test005_upload_tlog_die_forever_download(self):
        """

        test005_upload_tlog_die_forever_download
         - Upload file
         - Make tlog die forever
         - Wait for 60 sec.
         - Download file, should succeed
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()
        md5_before = self.file_name

        self.assertFalse(self.s3.failures.tlog_die_forever())
        time.sleep(60)

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

    @skip('https://github.com/threefoldtech/0-templates/issues/186')
    def test006_tlog_die_forever_upload_download(self):
        """

         - Make tlog die forever
         - Wait for 60 sec.
         - Upload file, should succeed
         - Download file, should succeed
        """
        self.assertFalse(self.s3.failures.tlog_die_forever())
        time.sleep(60)

        file_name, bucket_name, md5_before = self.s3.upload_file()
        self.assertTrue(False, "Upload file, should fail")

        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

