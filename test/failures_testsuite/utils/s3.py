from jumpscale import j
from zerorobot.service_collection import ServiceNotFoundError
from gevent.pool import Group
from utils.reset import EnvironmentReset
from utils.failures import FailureGenenator

logger = j.logger.get('s3demo')


class S3Manager:
    def __init__(self, parent, name):
        self.failures = FailureGenenator(self)
        self.reset = EnvironmentReset(self)

        self._parent = parent
        self.name = name
        j.clients.zrobot.get('demo', data={'url': self._parent.config['robot']['url']})
        self.dm_robot = j.clients.zrobot.robots['demo']

        self._zt_id = self._parent.config['zerotier']['id']
        self._zt_token = self._parent.config['zerotier']['token']

        self._vm_node = None
        self._vm_robot = None
        self._vm_host_robot = None
        try:
            self._service = self.dm_robot.services.get(name=name)
        except ServiceNotFoundError:
            self._service = None

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
    def minio_container(self):
        """
        container running minio.
        This containers run on vm_node
        """
        return self.vm_node.containers.get("minio_%s" % self.service.guid)

    @property
    def minio_config(self):
        return self.minio_container.download_content('/bin/zerostor.yaml')

    @property
    def vm_host(self):
        """
        zos machine that host the vm_node
        """

        vm = self.dm_robot.services.get(template_name='dm_vm', name=self.service.guid)
        return j.clients.zos.get(vm.data['data']['nodeId'])

    @property
    def robot_host(self):
        """
        robot of the the vm host
        """

        vm = self.dm_robot.services.get(template_name='dm_vm', name=self.service.guid)
        return j.clients.zrobot.robots[vm.data['data']['nodeId']]

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

    @property
    def url(self):
        """
        return the urls of the s3 once it's deployed
        """
        return self.service.schedule_action('url').wait(die=True).result
