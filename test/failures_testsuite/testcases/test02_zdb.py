from random import randint
from base_test import BaseTest
import unittest, time

class ZDBFailures(BaseTest):    
    def tearDown(self):
        super().tearDown()

    def test001_zdb_kill(self):
        """
        - kill zdb process  and make sure it will restart automatically.
        """
        md5_before = self.upload_file()

        self.logger.info('kill zdb process and make sure it will restart automatically')
        flag = self.s3.failures.zdb_process_down()
        self.assertTrue(flag, "zdb didn't restart")

        self.logger.info("Download uploaded file, and check that both are same.")
        md5_after = self.download_file(file_name=md5_before)
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
        self.file_name = self.upload_file()
        md5_before = self.file_name

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)
        self._delete_file('tmp/{}'.format(md5_after))

        self.logger.info('Stop {} zdb'.format((self.parity)))
        self.s3.failures.zdb_down(count=self.parity)

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('Start {} zdb'.format((self.parity)))
        self.s3.failures.zdb_up(count=self.parity)

    def test003_stop_parity_zdb_upload_download_start(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - download file, should pass
        - assert md5 checksum is matching
        - start n zdb, should pass
        """
        self.file_name = self.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.parity)))
        md5_before = self.file_name
        self.s3.failures.zdb_down(count=self.parity)

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info(' Start {} zdb'.format((self.parity)))
        self.s3.failures.zdb_up(count=self.parity)

    def test004_stop_parity_zdb_upload_start_download(self):
        """
        - Stop n zdb, n <= parity
        - upload file, should pass
        - start n zdb, should pass
        - download file, should pass
        - assert md5 checksum is matching
        """
        self.file_name = self.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.parity)))
        md5_before = self.file_name
        self.s3.failures.zdb_down(count=self.parity)

        self.logger.info(' Start {} zdb'.format((self.parity)))
        self.s3.failures.zdb_up(count=self.parity)

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

    def test005_stop_greater_parity_zdb_upload(self):
        """
        - Upload file, should succeed
        - Stop n+ zdb, n = parity, should succeed
        - Upload file, should fail
        - Download the uploaded file, should succeed
        - Start n+ zdb
        """
        self.file_name = self.upload_file()
        zdb_turn_down = self.parity + randint(1, self.shards)
        self.logger.info(' Stop {} zdb'.format(zdb_turn_down))
        self.s3.failures.zdb_down(count=zdb_turn_down)

        try:
            self.upload_file()
            self.assertTrue(False, 'Uploading should raise an error')
        except:
            pass

        self.logger.info(' Start {} zdb'.format(zdb_turn_down))
        self.s3.failures.zdb_up(count=zdb_turn_down)
