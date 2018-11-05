from base_test import BaseTest
import requests
from requests.exceptions import ConnectionError
import time
import unittest


class TestActivePassive(BaseTest):

    def setUp(self):
        if not self.s3_active_service_name:
           self.skipTest('No s3 service for active minio is found')
        if not self.s3_active_service_name:
           self.skipTest('No s3 service for passive minio is found')
        super().setUp()
        self.s3_active = self.s3_controller.s3[self.s3_active_service_name]
        self.s3_passive = self.s3_controller.s3[self.s3_passive_service_name]

    def tearDown(self):
        super().tearDown()

    def ping_minio(self, url, timeout=200):
        start = time.time()
        while (start + timeout) > time.time():
            try:
                requests.get(url, timeout=0.2)
                end = time.time()
                duration = end - start
                return duration
            except ConnectionError:
                continue
        return False

    @unittest.skip('to do .. wait till passive minio is updated .. passive minio vm is created')
    def test001_kill_active_minio_vm(self):
        """
        - Get the active minio vm (VM1)
        - Upload file (F1) to the active minio
        - kill VM1
        - Wait till the configuration of the passive minio get updated to become the active one.
        - Make sure the passive vm is now serving the requests which becomes an active.
        - Download F1 and check on its md5sum
        - Check that a new vm has been deployed (instead of VM1) and now acting as a passive minio.
        """


        self.logger.info('Get the active minio vm (VM1)')
        vm_host = self.s3_active.vm_host
        vms = vm_host.client.kvm.list()
        for vm in vms:
            if vm['name'] == '%s_vm' % self.s3_active.dm_vm.guid:
                break
            else:
                raise Exception("can't find vm with name: %s_vm" % self.s3.dm_vm.guid)

        self.logger.info('upload file to the active minio')
        file_name, bucket_name, file_md5 = self.s3_active.upload_file()

        self.logger.info('kill VM1')
        vm_host.client.kvm.destroy(vm['uuid'])

        self.logger.info('Wait till the configuration of the passive minio get updated to become the active one')

        self.logger.info('Make sure the passive vm is now serving the requests.')
        url = self.s3_active.url['public']
        duration = self.ping_minio(url, timeout=200)
        self.assertTrue(duration, "Active minio vm didn't start")
        self.logger.info("Active minio vm took %s sec to restart" % duration)
        self.logger.info('Download the file and check on its md5sum')
        self.s3_active.download_file(file_name, bucket_name, file_md5)

        self.logger.info('Check that a new vm has been deployed (instead of VM1) and now acting as a passive minio')
        #wait till u make sure new passive vm has been created
        url = self.s3_passive.url['public']
        duration = self.ping_minio(url, timeout=200)
        self.assertTrue(duration, "New minio vm acting as a passive minio didn't start")
        self.logger.info("Active minio vm took %s sec to restart" % duration)

    @unittest.skip('Wait for redploying passive vm')
    def test002_kill_passive_minio_vm(self):
        """
        - Get the passive minio vm (VM1)
        - kill VM1
        - Check if a new VM has been redployed and functions as passive minio.
        """

        self.logger.info('Get the active minio vm (VM1)')
        vm_host = self.s3_passive.vm_host
        vms = vm_host.client.kvm.list()
        for vm in vms:
            if vm['name'] == '%s_vm' % self.s3_passive.dm_vm.guid:
                break
            else:
                raise Exception("can't find vm with name: %s_vm" % self.s3.dm_vm.guid)

        self.logger.info('kill VM1')
        vm_host.client.kvm.destroy(vm['uuid'])

        self.logger.info(' Check if a new VM has been redployed and functions as passive minio')
        # wait till passive vm get started .. to do
        url = self.s3_passive.url['public']
        duration = self.ping_minio(url, timeout=200)
        self.assertTrue(duration, "Passive minio vm didn't start")
        self.logger.info("passive minio vm took %s sec to restart" % duration)

    @unittest.skip('same as test001 + enable ssd')
    def test003_ssd_failure_active_minio_vm(self):
        """
        - Upload file (F1) to the active minio
        - Disable ssd of the active minio vdisk, should succeed
        - Wait till the configuration of the passive minio get updated to become the active one.
        - Make sure the passive vm is now serving the requests which becomes an active.
        - Download F1 and check on its md5sum
        - Check that a new vm has been deployed (instead of VM1) and now acting as a passive minio.
        - Enable the ssd.
        """

        self.logger.info('upload file to the active minio')
        file_name, bucket_name, file_md5 = self.s3_active.upload_file()

        self.logger.info('Disable ssd of the active minio vdisk, should succeed')
        flag = self.s3_active.failures.disable_minio_vdisk_ssd()
        self.assertTrue(flag, "ssd hasn't been disabled")

        self.logger.info('Wait till the configuration of the passive minio get updated to become the active one')

        self.logger.info('Make sure the passive vm is now serving the requests which becomes an active.')
        url = self.s3_active.url['public']
        duration = self.ping_minio(url, timeout=200)
        self.assertTrue(duration, "Active minio vm didn't start")
        self.logger.info("Active minio vm took %s sec to restart" % duration)

        self.logger.info('Download the file and check on its md5sum')
        self.s3_active.download_file(file_name, bucket_name, file_md5)

        self.logger.info('Check that a new vm has been deployed (instead of VM1) and now acting as a passive minio.')
        #wait till u make sure new passive vm has been created
        url = self.s3_passive.url['public']
        duration = self.ping_minio(url, timeout=200)
        self.assertTrue(duration, "New minio vm acting as a passive minio didn't start")
        self.logger.info("Active minio vm took %s sec to restart" % duration)

        self.logger.info('Enable the ssd')
        # make sure it is the same host used
        self.s3_active.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()

    @unittest.skip('same as test002 + enable ssd')
    def test004_ssd_failure_passive_minio_vm(self):
        """
        - Disable ssd of the passive minio vdisk, should succeed.
        -  Check if a new VM has been redployed and functions as passive minio.
        - Enable the ssd.
        """

        self.logger.info('Disable ssd of the passive minio vdisk, should succeed')
        flag = self.s3_passive.failures.disable_minio_vdisk_ssd()
        self.assertTrue(flag, "ssd hasn't been disabled")

        self.logger.info(' Check if a new VM has been redployed and functions as passive minio')
        # wait till passive vm get started .. to do
        url = self.s3_passive.url['public']
        duration = self.ping_minio(url, timeout=200)
        self.assertTrue(duration, "Passive minio vm didn't start")
        self.logger.info("passive minio vm took %s sec to restart" % duration)

        self.logger.info('Enable the ssd')
        # make sure it is the same host used
        self.s3_passive.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()

    @unittest.skip('skip till the flow of active and passive is done')
    def test005_active_minio_tlog_ssd_failure(self):
        """
        - Upload file (F1) to the active minio.
        - Disable ssd of the active minio tlog, should succeed.
        - Wait till the configuration of the passive minio get updated to become the active one.
        - Make sure the passive vm is now serving the requests which becomes an active.
        - Download F1  and check on its md5sum
        - Check that a new tlog namespace has been deployed and has the same info as other tlog
        - Enable the ssd.
        """

        self.logger.info('Upload file (F1) to the active minio')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()

        try:
            self.logger.info('Disable ssd of the active minio tlog, should succeed.')
            flag = False
            flag = self.s3_active.failures.disable_minio_tlog_ssd()
            self.assertTrue(flag, "ssd hasn't been disabled")

            self.logger.info('Wait till the configuration of the passive minio get updated to become the active one.')

            self.logger.info('Make sure the passive vm is now serving the requests which becomes an active.')

            self.logger.info('Download F1  and check on its md5sum')
            md5_after = self.s3_active.download_file(file_name, bucket_name)
            self.assertEqual(md5_before, md5_after)

            self.logger.info('Check that a new tlog namespace has been deployed and has the same info as other tlog')

        except:
            raise
        finally:
            if flag:
                self.logger.info('Enable the ssd')
                self.s3_passive.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()

    @unittest.skip('skip till the flow of active and passive is done')
    def test006_passive_minio_tlog_ssd_failure(self):
        """
        - Upload a file (F1) to the active minio.
        - Disable ssd of the passive minio tlog, should succeed.
        - Check that a new tlog namespace has been deployed and has the same info as other tlog.
        - Download F1 from the passive minio, should succeed
        - Enable the ssd.
        """

        self.logger.info('Upload a file (F1) to the active minio.')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()

        try:
            self.logger.info('Disable ssd of the passive minio tlog, should succeed.')
            flag = False
            flag = self.s3_passive.failures.disable_minio_tlog_ssd()
            self.assertTrue(flag, "ssd hasn't been disabled")

            self.logger.info('Check that a new tlog namespace has been deployed and has the same info as other tlog.')

            self.logger.info('Download F1 from the passive minio, should succeed ')
            md5_after = self.s3_active.download_file(file_name, bucket_name)
            self.assertEqual(md5_before, md5_after)
        except:
            raise
        finally:
            if flag:
                self.logger.info('Enable the ssd')
                self.s3_passive.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()
