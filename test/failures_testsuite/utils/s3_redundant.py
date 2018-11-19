from jumpscale import j
from zerorobot.service_collection import ServiceNotFoundError
from utils.reset import EnvironmentReset
from utils.failures import FailureGenenator

logger = j.logger.get('s3demo')


class S3RedundantManager:
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
            self._service = self.dm_robot.services.get(name=name, template_name='s3_redundant')
        except ServiceNotFoundError:
            self._service = None

    @property
    def service(self):
        if self._service is None:
            raise RuntimeError("s3 service doesn't exist yet, call deploy to create it")
        return self._service

    @property
    def url(self):
        """
        return the urls of the s3 once it's deployed
        """
        return self.service.schedule_action('urls').wait(die=True).result

    @property
    def data(self):
        return self.service.data['data']

    @property
    def active_s3(self):
        return self.service.data['activeS3']

    @property
    def passive_s3(self):
        return self.service.data['passiveS3']

    def deploy(self, farm, size=1000, data=1, parity=1, login='admin', password='adminadmin', wait=False):
        """
        deploy an s3 redundant

        :return: return the install task of the s3 service created
        :rtype: Task
        """

        logger.info("install zerotier client")
        self.dm_robot.services.find_or_create('zerotier_client', 'zt', data={'token': self._zt_token})

        logger.info("install s3 redundant service")
        s3_data = {
            'mgmtNic': {'id': self._zt_id, 'ztClient': 'zt'},
            'farmerIyoOrg': farm,
            'dataShards': data,
            'parityShards': parity,
            'storageType': 'hdd',
            'storageSize': size,
            'minioLogin': login,
            'minioPassword': password}
        self._service = self.dm_robot.services.find_or_create('s3_redundant', self.name, data=s3_data)
        if wait:
            s3_redundant = self._service.schedule_action('install').wait(die=True)
            return s3_redundant
        else:
            return self._service.schedule_action('install')

    def uninstall(self):
        return self.service.schedule_action('uninstall')

    def urls(self):
        return self.service.schedule_action('urls')

    def start_active(self):
        return self.service.schedule_action('start_active')

    def stop_active(self):
        return self.service.schedule_action('stop_active')

    def upgrade_active(self):
        return self.service.schedule_action('upgrade_active')

    def start_passive(self):
        return self.service.schedule_action('start_passive')

    def stop_passive(self):
        return self.service.schedule_action('stop_passive')

    def upgrade_passive(self):
        return self.service.schedule_action('upgrade_passive')

    def update_reverse_proxy(self):
        return self.service.schedule_action('update_reverse_proxy')

    def delete(self):
        self.service.delete()
