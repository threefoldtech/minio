#!/usr/bin/env python3
from gevent import monkey

monkey.patch_all()
from gevent.pool import Group
from jumpscale import j
from utils.s3 import S3Manager
from utils.s3_redundant import S3RedundantManager


class Controller:
    def __init__(self, config, god_token=None):
        self.config = config
        if god_token:
            j.clients.zrobot.get(self.config['robot']['client'], data={'url': config['robot']['url'],
                                                                       'god_token_': god_token})
        else:
            j.clients.zrobot.get(self.config['robot']['client'], data={'url': config['robot']['url']})
        self.dm_robot = j.clients.zrobot.robots[self.config['robot']['client']]
        self.s3 = {}
        for service in self.dm_robot.services.find(template_name='s3'):
            self.s3[service.name] = S3Manager(self, service.name)

        self.s3_redundant = {}
        for service in self.dm_robot.services.find(template_name='s3_redundant'):
            self.s3_redundant[service.name] = S3RedundantManager(self, service.name)

    def deploy(self, name, farm, size=1000, data=1, parity=1, nsName='namespace', login='admin', password='adminadmin'):
        self.s3[name] = S3Manager(self, name)
        return self.s3[name].deploy(farm, size=size, data=data, parity=parity, nsName=nsName, login=login,
                                    password=password)

    def deploy_s3_redundant(self, name, farm, size=1000, data=1, parity=1, login='admin', password='adminadmin', wait=False):
        self.s3_redundant[name] = S3RedundantManager(self, name)
        return self.s3_redundant[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password,
                                              wait=wait)

    def urls(self):
        return {name: url for name, url in self._do_on_all(lambda s3: (s3.name, s3.url))}

    def minio_config(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.minio_config))}

    def states(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.service.state))}

    def _do_on_all(self, func):
        group = Group()
        return group.imap_unordered(func, self.s3.values())


def read_config(path):
    config = j.data.serializer.yaml.load(path)
    return config
