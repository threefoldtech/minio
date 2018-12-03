from base_test import BaseTest
from utils.failures import robot_god_token
from jumpscale import j
from urllib3.exceptions import MaxRetryError
import time


class TestS3Failures(BaseTest):
    def setUp(self):
        super().setUp()
        ser = self.s3.dm_robot.services.names[self.s3_service_name]
        self.namespaces = ser.data['data']['namespaces']
        self.logger.info('Make sure all zdbs are up')
        self.s3.failures.zdb_start_service(count=len(self.namespaces))

    def tearDown(self):
        super().tearDown()
        self.logger.info('Make sure all zdbs are up')
        self.s3.failures.zdb_start_service(count=len(self.namespaces))

    def corrupt_namespace_data(self, namespace):
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

    def test001_bitrot(self):
        """

        test001_bitrot
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
        parityshards = self.s3.service.data['data']['parityShards']
        datashards = self.s3.service.data['data']['dataShards']

        self.logger.info('Get the extra 25% namespaces down')
        extra_namespaces_num = len(self.namespaces) - parityshards - datashards
        extra_namespaces = [ns['name'] for ns in self.namespaces[:extra_namespaces_num]]
        main_namespaces = [ns['name'] for ns in self.namespaces[extra_namespaces_num:]]
        self.s3.failures.zdb_stop_service(count=extra_namespaces_num, except_namespaces=main_namespaces)

        self.logger.info('Upload a file and get its md5sum, should succeed.')
        file_name, bucket_name, md5_before = self.s3.upload_file()

        self.logger.info('Corrupt all data in any zdb location')
        # can make a function to corrupt shards in given namespace
        namespace = self.namespaces[extra_namespaces_num:][0]
        self.corrupt_namespace_data(namespace)

        self.logger.info('Get parity zdbs down excluding the corrupted namespace')
        wrong_data_namespace = namespace['name']
        self.s3.failures.zdb_stop_service(count=parityshards, except_namespaces=wrong_data_namespace)

        self.logger.info('Try to download F1, should give wrong md5')
        with self.assertRaises(MaxRetryError):
            self.s3.download_file(file_name, bucket_name, delete_bucket=False)

        self.logger.info('Get parity zdbs up exculding extra_namespaces zdbs')
        self.s3.failures.zdb_start_service(parityshards, except_namespaces=extra_namespaces)

        self.logger.info('Run the bitrot protection, should succeed')
        for _ in range(20):
            try:
                self.s3.minio_container.system('minio gateway zerostor-repair --config-dir /bin').get()
                break
            except Exception as e:
                self.logger.warning(e)
                time.sleep(5)
        else:
            self.s3.minio_container.system('minio gateway zerostor-repair --config-dir /bin').get()

        self.logger.info('Get parity zdbs down again excluding corrupted namespace zdb')
        self.s3.failures.zdb_stop_service(count=parityshards, except_namespaces=wrong_data_namespace)

        self.logger.info('Download F1 again , should have same md5 as the uploaded file')
        md5_after = self.s3.download_file(file_name, bucket_name)
        self.assertEqual(md5_after, md5_before)

