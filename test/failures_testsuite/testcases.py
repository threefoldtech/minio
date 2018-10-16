from random import randint
from base_test import BaseTest
import unittest


class TestS3Failures(BaseTest):

    def tearDown(self):
        super().tearDown()

    def test001_upload_stop_parity_zdb_download(self):
        """
        - upload 2M
        - Stop n zdb, n <= parity
        - Downlaod 10M file, should succeed
        - assert md5 checksum is matching
        - Start n zdb
        """
        self.file_name = self.upload_file()
        self.logger.info(' Stop {} zdb'.format((self.parity)))
        md5_before = self.file_name
        self.s3.failures.zdb_down(count=self.parity)

        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

        self.logger.info(' Start {} zdb'.format((self.parity)))
        self.s3.failures.zdb_up(count=self.parity)

    def test002_stop_parity_zdb_upload_download_start(self):
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

    def test003_stop_parity_zdb_upload_start_download(self):
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

    def test004_stop_greater_parity_zdb_upload(self):
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

    @unittest.skip('blocked till bitrot pull request is done')
    def test005_bitrot(self):
        """
        - Get the minio namespaces and get the zdbs location.
        - Upload a file and get its md5 sum, should succeed.
        - Manipulate some data of the uploaded file in any zdb location that has data due to upload.
        - Run the bitrot protection, should succeed.
        - Check that the corrupted data has been corrected, should succeed.
        - Download the file, Must have same md5 as the uploaded file.
        """
        self.logger.info('Get the minio namespaces and get the zdbs location')
        ser = self.s3.dm_robot.services.names[self.s3_service_name]
        namespaces = ser.data['data']['namespaces']

        self.logger.info('Upload a file and get its md5 sum, should succeed.')
        self.logger.info('Create a file and check its md5sum.')
        self.file_name = self.upload_file()
        md5_before = self.file_name

        self.logger.info('Manipulate some data of the uploaded file in any zdb location that has data due to upload')
        for namespace in namespaces:
            robot = j.clients.zrobot.robots[namespace['node']]
            ns = robot.services.get(name=namespace['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])
            zdb_path = zdb.data['data']['path']
            # connect to the namespace node
            node_url = namespaces[0]['url'].split(':6600')[0].split('http://')[1]
            node = j.clients.zos.get('john', data={'host': node_url})
            # check the files there in zdb path
            ns_name = ns.data['data']['nsName']
            data = node.client.bash('ls {}/data/{}'.format(zdb_path, ns_name)).get().stdout
            files1 = data.split()
            if files1:
                break

        # check the added files and manpiulate them
        zdb_file = '{}/data/{}/{}'.format(zdb_path, ns_name, files1[0])
        zdb_file_md5_before = node.client.bash('md5sum {}'.format(zdb_file)).get().stdout.split()[0]
        node.client.bash('dd conv=notrunc if=/dev/urandom of={} bs=1M count=1'.format(zdb_file)).get()

        # run the bitrot protections and then wait
        self.logger.info('Run the bitrot protection, should succeed')

        self.logger.info('Check that the corrupted data has been corrected, should succeed')
        zdb_file_md5_after = node.client.bash('md5sum {}'.format(zdb_file)).get().stdout.split()[0]
        self.assertEqual(zdb_file_md5_before, zdb_file_md5_after)

        # download  and and assert
        self.logger.info('Download the file, Must have same md5 as the uploaded file')
        md5_after = self.download_file(file_name=self.file_name)
        self.assertEqual(md5_after, md5_before)

