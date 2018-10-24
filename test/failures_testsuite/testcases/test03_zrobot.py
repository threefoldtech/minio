from random import randint
from base_test import BaseTest
import unittest, time

class ZROBOTFailures(BaseTest):      
    def tearDown(self):
        super().tearDown()

    def test001_zrobot_kill(self):
        """
        - upload file, should pass
        - kill zrobot process on minio vm and make sure it will restart automatically.
        - download uploaded file, should pass
        """
        md5_before = self.upload_file()

        self.logger.info('kill zrobot process and make sure it will restart automatically')
        minio_node_adder = self.s3.vm_node.addr
        flag = self.s3.failures.Kill_node_robot_process(node_addr=minio_node_adder)
        self.assertTrue(flag, "zrobot didn't restart")

        self.logger.info("Download uploaded file, and check that both are same.")
        md5_after = self.download_file(file_name=md5_before,keep_trying=True)
        self.assertEqual(md5_after, md5_before)

