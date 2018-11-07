from base_test import BaseTest


class ZrobotFailures(BaseTest):
    def test001_zrobot_kill(self):
        """

        test001_zrobot_kill
        - upload file, should pass
        - kill zrobot process on minio vm and make sure it will restart automatically.
        - download uploaded file, should pass
        """
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('kill zrobot process and make sure it will restart automatically')
        minio_node_adder = self.s3.vm_node.addr
        self.logger.info('minio node adder : {}'.format(minio_node_adder))
        flag = self.s3.failures.Kill_node_robot_process(node_addr=minio_node_adder)
        self.assertTrue(flag, "zrobot didn't restart")

        self.logger.info("Download uploaded file, and check that both are same.")
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)
