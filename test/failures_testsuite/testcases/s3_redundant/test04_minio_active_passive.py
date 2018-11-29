from utils.controller import Controller
from base_test import BaseTest
from requests.exceptions import ConnectionError
import time, unittest, requests
from jumpscale import j
from subprocess import Popen, PIPE


class TestActivePassive(BaseTest):
    @classmethod
    def setUpClass(cls):
        """
        Deploy s3 redundant.

        function to deploy s3 redundant with one of pre-configured parameters.

        """
        cls.config = j.data.serializer.yaml.load('./config.yaml')
        if cls.config['s3_redundant']['deploy']:
            cls.s3_redundant_controller = Controller(cls.config)

            data = [cls.config['s3_redundant']['instance']['farm'], cls.config['s3_redundant']['instance']['size'],
                    cls.config['s3_redundant']['instance']['shards'], cls.config['s3_redundant']['instance']['parity']]

            for _ in range(5):
                cls.s3_redundant_service_name = 's3_redundant_{}'.format(str(time.time()).split('.')[0])
                cls.logger.info("s3 redundant service name : {}".format(cls.s3_redundant_service_name))

                instance = cls.s3_redundant_controller.deploy_s3_redundant(cls.s3_redundant_service_name, *data)
                try:
                    cls.logger.info("wait for deploying {} service".format(cls.s3_redundant_service_name))
                    instance.wait(die=True)
                    break
                except Exception as e:
                    cls.logger.error("There is an error while installing s3 .. we will re-install it!")
                    cls.logger.error(e)
                    cls.logger.info('uninstall {} service'.format(cls.s3_redundant_service_name))
                    s3_redundant_object = cls.s3_redundant_controller.s3_redundant[cls.s3_redundant_service_name]
                    s3_redundant_object.uninstall()
                    cls.logger.info('delete {} service'.format(cls.s3_redundant_service_name))
                    s3_redundant_object.delete()
            else:
                raise TimeoutError("can't install s3 redundant .. gonna quit!")

            cls.logger.info('wait for {} state to be okay'.format(cls.s3_redundant_service_name))
            for _ in range(10):
                cls.s3_redundant_object = cls.s3_redundant_controller.s3_redundant[cls.s3_redundant_service_name]
                state = cls.s3_redundant_object.service.state
                cls.logger.info("{} state : {}".format(cls.s3_redundant_service_name, state))
                try:
                    cls.logger.info("waiting {} state to be ok ... ".format(cls.s3_redundant_service_name))
                    state.check('actions', 'install', 'ok')
                    break
                except:
                    time.sleep(5 * 60)
                    cls.logger.info("wait for 5 mins .. then we try again!")
            else:
                state.check('actions', 'install', 'ok')

            for _ in range(10):
                try:
                    url = cls.s3_redundant_object.url
                    if 'http' in url['active_urls']['public'] and 'http' in url['active_urls']['storage'] and 'http' in \
                            url['passive_urls']['public'] and 'http' in url['passive_urls']['storage']:
                        cls.logger.info('s3s have a public and storage ip')
                        break
                    cls.logger.info('wait till s3s get the urls')
                    time.sleep(60)
                except:
                    time.sleep(60)
            else:
                raise TimeoutError("There is no ip for the s3 ... gonna quit!")
        else:
            sub = Popen('zrobot godtoken get', stdout=PIPE, stderr=PIPE, shell=True)
            out, err = sub.communicate()
            god_token = str(out).split(' ')[2]
            cls.s3_redundant_controller = Controller(cls.config, god_token)
            cls.s3_redundant_service_name = cls.config['s3_redundant']['use']['s3_redundant_service_name']
            cls.s3_redundant_object = cls.s3_redundant_controller.s3_redundant[cls.s3_redundant_service_name]

        cls.s3_active_service_name = cls.s3_redundant_object.active_s3
        cls.s3_passive_service_name = cls.s3_redundant_object.passive_s3

    def setUp(self):
        if not self.s3_active_service_name or not self.s3_active_service_name:
            self.skipTest("Please, check there is active and passive s3 minios")
        self.s3_active = self.s3_redundant_controller.s3[self.s3_active_service_name]
        self.s3_passive = self.s3_redundant_controller.s3[self.s3_passive_service_name]

    def tearDown(self):
        pass

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

    def test001_kill_active_minio_vm(self):
        """
        test001_kill_active_minio_vm
        - Get the active minio vm (VM1)
        - Upload file (F1) to the active minio
        - kill VM1
        - Check that a new vm has been deployed and acting as active vm.
        - Download F1 and check on its md5sum
        """

        self.logger.info('Get the active minio vm (VM1)')
        url = self.s3_active.url['public']
        vm_host = self.s3_active.vm_host
        vms = vm_host.client.kvm.list()
        for vm in vms:
            if vm['name'] == '%s_vm' % self.s3_active.service_vm.guid:
                break
        else:
            raise Exception("can't find vm with name: %s_vm" % self.s3_active.service_vm.guid)

        self.logger.info('upload file (F1)to the active minio')
        file_name, bucket_name, file_md5 = self.s3_active.upload_file()

        self.logger.info('kill VM1')
        vm_host.client.kvm.destroy(vm['uuid'])
        time.sleep(20)

        self.logger.info('Check that a new vm has been deployed and acting as active vm')
        duration = self.ping_minio(url, timeout=300)
        self.assertTrue(duration, "Active minio vm didn't start")
        self.logger.info("Active minio vm took %s sec to restart" % duration)

        self.logger.info('Download the file and check on its md5sum')
        self.s3_active.download_file(file_name, bucket_name, file_md5)

    def test002_kill_passive_minio_vm(self):
        """

        test002_kill_passive_minio_vm
        - Upload file (F1) to the active minio
        - Download F1 from the passive minio, should succeed
        - Get the passive minio vm (VM1)
        - kill VM1
        - Check if a new VM has been redployed and functions as passive minio.
        - Download F1 again from the passive minio, should succeed
        """

        self.logger.info('upload file to the active minio')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()
        time.sleep(10)

        self.logger.info(' Download F1 from the passive minio, should succeed')
        md5_after = self.s3_passive.download_file(file_name, bucket_name, delete_bucket=False)
        self.assertEqual(md5_after, md5_before, "md5s doesn't match")

        self.logger.info('Get the active minio vm (VM1)')
        vm_host = self.s3_passive.vm_host
        vms = vm_host.client.kvm.list()
        for vm in vms:
            if vm['name'] == '%s_vm' % self.s3_passive.service_vm.guid:
                break
        else:
            raise Exception("can't find vm with name: %s_vm" % self.s3.service_vm.guid)

        self.logger.info('kill VM1')
        vm_host.client.kvm.destroy(vm['uuid'])

        self.logger.info('Check if a new VM has been redployed and functions as passive minio')
        url = self.s3_passive.url['public']
        duration = self.ping_minio(url, timeout=300)
        self.assertTrue(duration, "Passive minio vm didn't start")
        self.logger.info("passive minio vm took %s sec to restart" % duration)

        self.logger.info('Download the file again from the passive minio, should succeed')
        md5_after2 = self.s3_passive.download_file(file_name, bucket_name, delete_bucket=False)
        self.assertEqual(md5_after2, md5_before, "md5s doesn't match")

    @unittest.skip('https://github.com/threefoldtech/minio/issues/49 ')
    def test003_ssd_failure_active_minio_vm(self):
        """
        test003_ssd_failure_active_minio_vm
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

        try:
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

            self.logger.info(
                'Check that a new vm has been deployed (instead of VM1) and now acting as a passive minio.')
            # wait till u make sure new passive vm has been created
            url = self.s3_passive.url['public']
            duration = self.ping_minio(url, timeout=200)
            self.assertTrue(duration, "New minio vm acting as a passive minio didn't start")
            self.logger.info("Active minio vm took %s sec to restart" % duration)
        except:
            raise
        finally:
            self.logger.info('Enable the ssd')
            # make sure it is the same host used
            self.s3_active.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()

    @unittest.skip('https://github.com/threefoldtech/minio/issues/49 ')
    def test004_ssd_failure_passive_minio_vm(self):
        """
        test004_ssd_failure_passive_minio_vm
        - Upload file (F1) to the active minio
        - Download F1 from the passive minio, should succeed
        - Disable ssd of the passive minio vdisk, should succeed.
        - Check if a new VM has been redployed and functions as passive minio.
        - Download F1 from the passive minio, should succeed
        - Enable the ssd.
        """

        self.logger.info('Upload a file (F1) to the active minio.')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()
        time.sleep(10)

        self.logger.info(' Download F1 from the passive minio, should succeed')
        md5_after = self.s3_passive.download_file(file_name, bucket_name, delete_bucket=False)
        self.assertEqual(md5_after, md5_before, "md5s doesn't match")

        try:
            self.logger.info('Disable ssd of the passive minio vdisk, should succeed')
            flag = False
            flag = self.s3_passive.failures.disable_minio_vdisk_ssd()
            self.assertTrue(flag, "ssd hasn't been disabled")

            self.logger.info(' Check if a new VM has been redployed and functions as passive minio')
            # wait till passive vm get started .. to do
            url = self.s3_passive.url['public']
            duration = self.ping_minio(url, timeout=200)
            self.assertTrue(duration, "Passive minio vm didn't start")
            self.logger.info("passive minio vm took %s sec to restart" % duration)

            self.logger.info("Download F1 from the passive minio, should succeed")
            md5_after2 = self.s3_passive.download_file(file_name, bucket_name)
            self.assertEqual(md5_after2, md5_before, "md5s doesn't match")
        except:
            raise
        finally:
            self.logger.info('Enable the ssd')
            # make sure it is the same host used
            self.s3_passive.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()

    @unittest.skip('https://github.com/threefoldtech/minio/issues/49 ')
    def test005_active_minio_tlog_ssd_failure(self):
        """

        test005_active_minio_tlog_ssd_failure
        - Upload file (F1) to the active minio.
        - Disable ssd of the active minio tlog, should succeed.
        - Wait till the configuration of the passive minio get updated to become the active one.
        - Make sure the passive vm is now serving the requests which becomes an active.
        - Check that a new tlog namespace has been deployed and has the same info as other tlog
        - Download F1  and check on its md5sum
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

            self.logger.info('Check that a new tlog namespace has been deployed and has the same info as other tlog')

            self.logger.info('Download F1  and check on its md5sum')
            md5_after = self.s3_active.download_file(file_name, bucket_name)
            self.assertEqual(md5_before, md5_after)
        except:
            raise
        finally:
            if flag:
                self.logger.info('Enable the ssd')
                self.s3_passive.vm_host.client.bash('echo "- - -" | tee /sys/class/scsi_host/host*/scan').get()

    @unittest.skip('https://github.com/threefoldtech/minio/issues/49 ')
    def test006_passive_minio_tlog_ssd_failure(self):
        """

        test006_passive_minio_tlog_ssd_failure
        - Upload a file (F1) to the active minio.
        - Download F1 from the passive minio, should succeed.
        - Disable ssd of the passive minio tlog, should succeed.
        - Check that a new tlog namespace has been deployed and has the same info as other tlog.
        - Download F1 from the passive minio, should succeed
        - Enable the ssd.
        """

        self.logger.info('Upload a file (F1) to the active minio.')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()
        time.sleep(10)

        self.logger.info(' Download F1 from the passive minio, should succeed')
        md5_after = self.s3_passive.download_file(file_name, bucket_name, delete_bucket=False)
        self.assertEqual(md5_after, md5_before, "md5s doesn't match")

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

    def test007_hdd_failure(self):
        """
        - Disable hdd of the data, should succeed.
        """
        parity_data_nodes=[]
        extra_namespaces = []
        file_name, bucket_name, md5_before = self.s3_active.upload_file()
        data_namespaces = self.s3_active.service.data['data']['namespaces']
        data_shards = self.s3_redundant_object.shards
        data_parity = self.s3_redundant_object.parity
        extra_namespaces_len = len(data_namespaces) -(data_shards+data_parity)
        try:
            self.logger.info('stop zdb  of extra namespace nodes, should succeed.')
            extra_namespaces = self.s3_active.failures.zdb_down(count=extra_namespaces_len)
            md5_after = self.s3_active.download_file(file_name, bucket_name)
            self.assertEqual(md5_after, md5_before)
            
            self.logger.info("Disable hhd of parity_shards namespace nodes, should succeed")
            parity_data_nodes = self.s3_active.failures.disable_datadisk_hdd(count=data_parity)
            self.assertEqual(len(parity_data_nodes), data_parity,"hdd hasn't been disabled")

            self.logger.info("Check that new parity shards namespaces created, should succeed")
            time.sleep(60)
            md5_after = self.s3_active.download_file(file_name, bucket_name)
            self.assertEqual(md5_after, md5_before)
            file_name, bucket_name, md5_before = self.s3_active.upload_file()
            new_data_namespaces = self.s3_active.service.data['data']['namespaces']
            new_data_shards = [new_shard for new_shard in new_data_namespaces if new_shard not in old_data_namespaces]
            self.assertEqual(len(new_data_shards), data_parity+len(extra_namespaces))
        except Exception as e:
            raise RuntimeError(e)
        finally:
            if extra_namespaces:
                self.s3_active.failures.zdb_up(count=len(extra_namespaces))

    def test008_reboot_datashards_node(self):
        """

        test008_reboot_datashards_node
        - Stop extra zdbs, and Upload a file (F1) to the active minio.
        - Reboot one of nodes of datashards.
        - Start extra zdbs again.
        - Check that a new namespace created instead of one on rebooted node.
        - Check that  useless namespace deleted.
        """
        old_data_namespaces = self.s3_active.service.data['data']['namespaces']

        self.logger.info(" Stop extra zdbs ")
        extra_namespaces=self.s3_active.failures.zdb_down(count=2)
        datashards_namespaces =[namespace['name']  for namespace in old_data_namespaces if namespace['name'] not in extra_namespaces]   
        
        self.logger.info('Upload a file (F1) to the active minio. ')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()
        time.sleep(10)

        self.logger.info(" Start extra zdbs again.")
        self.s3_active.failures.zdb_up(count=2)

        self.logger.info('Reboot one of nodes of datashards.')
        rebooted_nodes = self.s3_active.failures.reboot_datashards_node(except_namespaces=extra_namespaces)

        self.logger.info('Upload a file (F1) to the active minio. ')
        file_name, bucket_name, md5_before = self.s3_active.upload_file()
        time.sleep(300)

        self.logger.info("Check that a new namespace created instead of one on rebooted node.")
        new_data_namespaces = self.s3_active.update_data()['namespaces']
        diff_data_shards = [new_shard for new_shard in new_data_namespaces if new_shard not in old_data_namespaces]
        self.assertEqual(len(diff_data_shards),len(rebooted_nodes))

        self.logger.info("Check that download working correctly. ")
        self.s3_active.failures.zdb_down(count=2,except_namespaces=datashards_namespaces)
        self.s3_active.failures.zdb_down(count=2,except_namespaces=rebooted_nodes)
        md5_after = self.s3_active.download_file(file_name, bucket_name, delete_bucket=False)
        self.assertEqual(md5_after, md5_before)

