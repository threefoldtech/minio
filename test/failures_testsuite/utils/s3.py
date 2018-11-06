from jumpscale import j
import os, time, hashlib
from zerorobot.service_collection import ServiceNotFoundError
from gevent.pool import Group
from utils.reset import EnvironmentReset
from utils.failures import FailureGenenator
from urllib.parse import urlparse
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou

logger = j.logger.get('s3demo')


class S3Manager:
    def __init__(self, parent, name):
        self.failures = FailureGenenator(self)
        self.reset = EnvironmentReset(self)

        self._parent = parent
        self.name = name
        self.dm_robot = self._parent.dm_robot

        self._zt_id = self._parent.config['zerotier']['id']
        self._zt_token = self._parent.config['zerotier']['token']

        self._client_type = 'public'
        self._client = None
        self._vm_node = None
        self._vm_robot = None
        self._vm_host_robot = None
        self._vm_host = None
        self._container_client = None
        try:
            self._service = self.dm_robot.services.get(name=name)
        except ServiceNotFoundError:
            self._service = None

    @property
    def client(self):
        if self._client is None:
            url = urlparse(self.data['minioUrls'][self._client_type])
            self._client = Minio(url.netloc,
                                 access_key=self.data['minioLogin'],
                                 secret_key=self.data['minioPassword'],
                                 secure=False)
        return self._client

    @property
    def service(self):
        if self._service is None:
            raise RuntimeError("s3 service doesn't exist yet, call deploy to create it")
        return self._service

    @property
    def service_vm(self):
        return self.dm_robot.services.get(name=self.service.guid)

    @property
    def vm_node(self):
        """
        zos client on the zos VM that host the minio container
        """
        if self._vm_node is None:
            dm_vm = self.dm_robot.services.get(name=self.service.guid)
            ip = dm_vm.schedule_action('info').wait(die=True).result['zerotier']['ip']
            self._vm_node = j.clients.zos.get('demo_vm_node', data={'host': ip})
        return self._vm_node

    @property
    def vm_robot(self):
        """
        zrobot client on the zos VM that host the minio container
        """
        if self._vm_robot is None:
            j.clients.zrobot.get('demo_vm_robot', data={'url': "http://%s:6600" % self.vm_node.public_addr})
            self._vm_robot = j.clients.zrobot.robots['demo_vm_robot']
        return self._vm_robot

    @property
    def vm_host_robot(self):
        """
        zrobot client of the node  that hosts the zos VM which hosts the minio container
        """
        if self._vm_host_robot is None:
            j.clients.zrobot.get('demo_vm_host_robot', data={'url': "http://%s:6600" % self.vm_host.public_addr}) # 'god_token_': ''
            self._vm_host_robot = j.clients.zrobot.robots['demo_vm_host_robot']
        return self._vm_host_robot

    @property
    def minio_container(self):
        if self._container_client is None:
            container = self.vm_node.client.container.find('minio_{}'.format(self.service.guid))
            self._container_client = self.vm_node.client.container.client(list(container.keys())[0])
        return self._container_client

    @property
    def zerodb_nodes(self):
        for zerodb in self.service.data['data']['namespaces']:
            yield j.clients.zos.get(zerodb['node'])

    @property
    def tlog_node(self):
        data = self.service.data['data']
        if data['tlog'] and data['tlog']['node']:
            if data['tlog']['node'] in j.clients.zos.list():
                return j.clients.zos.get(data['tlog']['node'])
            else:
                tlogs_host = data['tlog']['url'].replace('//', '').split(':')[1]
                j.clients.zos.get(data['tlog']['node'], data={'host': tlogs_host})
                return j.clients.zos.get(data['tlog']['node'])

    @property
    def minio_config(self):
        return self.minio_container.download_content('/bin/zerostor.yaml')

    @property
    def vm_host(self):
        """
        zos machine that host the vm_node
        """
        if self._vm_host is None:
            vm = self.dm_robot.services.get(template_name='dm_vm', name=self.service.guid)
            result = vm.schedule_action('info').wait(die=True).result
            self._vm_host = j.clients.zos.get(result['node_id'], data={'host': result['host']['public_addr']})
        return self._vm_host

    @property
    def robot_host(self):
        """
        robot of the the vm host
        """

        vm = self.dm_robot.services.get(template_name='dm_vm', name=self.service.guid)
        return j.clients.zrobot.robots[vm.data['data']['nodeId']]

    @property
    def url(self):
        """
        return the urls of the s3 once it's deployed
        """
        return self.service.schedule_action('url').wait(die=True).result

    @property
    def data(self):
        return self.service.data['data']

    @property
    def parity(self):
        return self.data['parityShards']

    @property
    def shards(self):
        return self.data['dataShards']

    def _create_file(self, file_name, size, directory='/tmp'):
        file_path = '{}/{}'.format(directory, file_name)
        with open('{}/{}'.format(directory, file_name), 'wb') as f:
            f.write(os.urandom(size))
        return file_path

    def _create_bucket(self):
        bucket_name = j.data.idgenerator.generateXCharID(16)
        try:
            logger.info("create bucket")
            self.client.make_bucket(bucket_name)
        except BucketAlreadyExists:
            logger.warning('Bucket already exists')
        except BucketAlreadyOwnedByYou:
            logger.warning('Bucket already owned by you')
        except:
            logger.warning("Can't create bucket!")
            raise RuntimeError
        return bucket_name

    def upload_file(self, size=1024 * 1024):
        bucket_name = self._create_bucket()
        if not bucket_name:
            raise RuntimeError
        file_name = j.data.idgenerator.generateXCharID(16)
        file_path = self._create_file(file_name, size)
        file_md5 = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        logger.info("Upload a file")
        try:
            self.client.fput_object(bucket_name, file_name, file_path)
        except:
            logger.warning("Can't upload {} file".format(file_name))
            raise RuntimeError
        os.remove(file_path)
        return file_name, bucket_name, file_md5

    @staticmethod
    def call_timeout(timeout, resource, *args, **kwargs):
        now = time.time()
        while now + timeout > time.time():
            try:
                result = resource(*args, **kwargs)
                return result
            except:
                time.sleep(1)
                continue
        else:
            resource(*args, **kwargs)

    def download_file(self, file_name, bucket_name, timeout=300, delete_bucket=True):
        try:
            logger.info("Download a file")
            d_file = self.call_timeout(timeout, self.client.get_object, bucket_name, file_name)
        except:
            logger.warning("Can't download {} file".format(file_name))
            raise
        finally:
            if delete_bucket:
                self.client.remove_bucket(bucket_name)
        d_file_md5 = hashlib.md5(d_file.data).hexdigest()
        return d_file_md5

    def execute_all_nodes(self, func, nodes=None):
        """
        execute func on all the nodes

        if nodes is None, func is execute on all the nodes that play a role with the minio
        deployement, if nodes is not None, it needs to be an iterable containing a node object

        :param func: function to execute, func needs to accept one argument, a node object
        :type func: function
        :param nodes: list of node on whic to execute func, defaults to None
        :param nodes: iterable, optional
        """

        if nodes is None:
            nodes = set([self.vm_node, self.vm_host])
            nodes.update(self.zerodb_nodes)

        g = Group()
        g.map(func, nodes)
        g.join()

    def deploy(self, farm, size=20000, data=4, parity=2, nsName='namespace', login='admin', password='adminadmin'):
        """
        deploy an s3 environment

        :return: return the install task of the s3 service created
        :rtype: Task
        """

        logger.info("install zerotier client")
        self.dm_robot.services.find_or_create('zerotier_client', 'zt', data={'token': self._zt_token})

        logger.info("install s3 service")
        s3_data = {
            'mgmtNic': {'id': self._zt_id, 'ztClient': 'zt'},
            'farmerIyoOrg': farm,
            'dataShards': data,
            'parityShards': parity,
            'storageType': 'hdd',
            'storageSize': size,
            'minioLogin': login,
            'minioPassword': password,
            'nsName': nsName}
        self._service = self.dm_robot.services.find_or_create('s3', self.name, data=s3_data)
        return self._service.schedule_action('install')

