from base_test import BaseTest
import unittest
from utils.failures import robot_god_token
from jumpscale import j


class TestS3Failures(BaseTest):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    @unittest.skip('blocked till bitrot pull request is done')
    def test001_bitrot(self):
        """
        - Get the minio namespaces and get the zdbs location.
        - Make sure all zdbs are up.
        - Get the extra 25% namespaces down.
        - Upload a file (F1) and get its md5sum, should succeed.
        - Corrupt all data in any zdb location.
        - Get parity zdbs down excluding the corrupted namespace.
        - Try to download F1, should give wrong md5.
        - Get parity zdbs up exculding extra_namespaces zdbs.
        - Run the bitrot protection, should succeed.
        - Get parity zdbs down again excluding corrupted namespace zdb.
        - Check that the corrupted data has been corrected, should succeed.
        - Download F1 again, should have same md5 as the uploaded file.
        - Make sure all zdbs are up.
        """
        self.logger.info('Get the minio namespaces and get the zdbs location')
        ser = self.s3.dm_robot.services.names[self.s3_service_name]
        namespaces = ser.data['data']['namespaces']
        parityshards = self.s3.service.data['data']['parityShards']
        datashards = self.s3.service.data['data']['dataShards']

        #make this in the setup if possible
        self.logger.info('Make sure all zdbs are up')
        self.s3.failures.zdb_up(count=len(namespaces))

        self.logger.info('Get the extra 25% namespaces down')
        extra_namespaces_num = len(namespaces) - parityshards - datashards
        extra_namespaces = [ns['name'] for ns in namespaces[:extra_namespaces_num - 1]]
        self.s3.failures.zdb_down(count=extra_namespaces_num, except_namespaces=namespaces[extra_namespaces_num:])

        self.logger.info('Upload a file and get its md5sum, should succeed.')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('Corrupt all data in any zdb location')
        # can make a function to corrupt shards in given namespace
        namespace = namespaces[extra_namespaces_num:][0]
        robot = j.clients.zrobot.robots[namespace['node']]
        robot = robot_god_token(robot)
        ns = robot.services.get(name=namespace['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])
        zdb_path = zdb.data['data']['path']
        # connect to the namespace node
        node_url = namespace['url'].split(':6600')[0].split('http://')[1]
        node = j.clients.zos.get('namespace_node', data={'host': node_url})
        # check the files there in zdb path
        ns_name = ns.data['data']['nsName']
        data = node.client.bash('ls {}/data/{}'.format(zdb_path, ns_name)).get().stdout
        files = data.split()
        self.assertTrue(files, "No shards were found in this namespace")
        for f in files:
            zdb_file = '{}/data/{}/{}'.format(zdb_path, ns_name, f)
            node.client.bash('dd if=/dev/urandom of={} bs=1M count=10'.format(zdb_file)).get()

        self.logger.info('Get parity zdbs down excluding the corrupted namespace')
        wrong_data_namespace = namespace['name']
        self.s3.failures.zdb_down(count=parityshards, except_namespaces=wrong_data_namespace)

        self.logger.info('Try to download F1, should give wrong md5')
        md5_after = self.s3.download(file_name, bucket_name)
        self.assertNotEqual(md5_before, md5_after, "md5 after download is not different")

        self.logger.info('Get parity zdbs up exculding extra_namespaces zdbs')
        self.s3.failures.zdb_up(parityshards, except_namespaces=extra_namespaces)

        # run the bitrot protections and then wait
        self.logger.info('Run the bitrot protection, should succeed')
        result = self.s3.minio_container.client.system('minio gateway zerostor-repair --config-dir /bin').get()

        self.logger.info('Get parity zdbs down again excluding corrupted namespace zdb')
        self.s3.failures.zdb_down(count=parityshards, except_namespaces=wrong_data_namespace)

        # download  and and assert
        self.logger.info('Download F1 again , should have same md5 as the uploaded file')
        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info('Make sure all zdbs are up')
        self.s3.failures.zdb_up(count=len(namespaces))

    def test002_kill_minio_process(self):
        """
        - kill minio process and make sure it will restart automatically.
        """
        self.logger.info('kill minio process and make sure it will restart automatically')
        flag = self.s3.failures.minio_process_down(timeout=200)
        self.assertTrue(flag, "minio didn't restart")
