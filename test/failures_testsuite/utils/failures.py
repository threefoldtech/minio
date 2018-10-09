import signal
import time
from urllib.parse import urlparse

import requests
from requests.exceptions import ConnectionError, ConnectTimeout

from jumpscale import j
from zerorobot.template.state import StateCheckError

logger = j.logger.get()


class FailureGenenator:

    def __init__(self, parent):
        self._parent = parent

    def zdb_start_all(self):
        """
        start all the zerodb services used by minio
        """
        s3 = self._parent

        def do(namespace):
            robot = j.clients.zrobot.robots[namespace['node']]
            robot = robot_god_token(robot)
            for zdb in robot.services.find(template_name='zerodb'):
                logger.info('start zerodb %s on node %s', zdb.name, namespace['node'])
                zdb.schedule_action('start')

        self._parent.execute_all_nodes(do, nodes=s3.service.data['data']['namespaces'])

    def zdb_stop_all(self):
        """
        stop all the zerodb services used by minio
        """

        s3 = self._parent

        def do(namespace):
            robot = j.clients.zrobot.robots[namespace['node']]
            robot = robot_god_token(robot)
            for zdb in robot.services.find(template_name='zerodb'):
                logger.info('stop zerodb %s on node %s', zdb.name, namespace['node'])
                zdb.schedule_action('stop')

        self._parent.execute_all_nodes(do, nodes=s3.service.data['data']['namespaces'])

    def minio_process_down(self):
        """
        turn off the minio process, then count how much times it takes to restart
        """
        s3 = self._parent
        url = s3.url['public']
        cont = s3.minio_container

        logger.info('killing minio process')
        job_id = 'minio.%s' % s3.service.guid
        cont.client.job.kill(job_id, signal=signal.SIGINT)
        jobids = [j['cmd']['id'] for j in cont.client.job.list()]
        while job_id in jobids:
            jobids = [j['cmd']['id'] for j in cont.client.job.list()]
        logger.info('minio process killed')

        logger.info("wait for minio to restart")
        start = time.time()
        while True:
            try:
                resp = requests.get(url, timeout=0.2)
                end = time.time()
                break
            except ConnectionError:
                continue
            except ConnectionError:
                continue

        duration = end-start
        logger.info("minio took %s sec to restart" % duration)
        return duration

    def zdb_down(self, count=1):
        """
        ensure that count zdb are turned off
        """
        s3 = self._parent
        if not s3:
            return

        n = 0
        for namespace in s3.service.data['data']['namespaces']:
            if n >= count:
                break
            robot = j.clients.zrobot.robots[namespace['node']]
            robot = robot_god_token(robot)
            ns = robot.services.get(name=namespace['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])

            try:
                zdb.state.check('status', 'running', 'ok')
                logger.info('stop %s on node %s', zdb.name, namespace['node'])
                zdb.schedule_action('stop').wait(die=True)
                n += 1
            except StateCheckError:
                pass

    def zdb_up(self, count=1):
        """
        ensure that count zdb are turned on
        """
        s3 = self._parent
        if not s3:
            return

        n = 0
        for namespace in s3.service.data['data']['namespaces']:
            if n >= count:
                break
            robot = j.clients.zrobot.robots[namespace['node']]
            robot = robot_god_token(robot)
            ns = robot.services.get(name=namespace['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])

            try:
                zdb.state.check('status', 'running', 'ok')
                continue
            except StateCheckError:
                logger.info('start %s on node %s', zdb.name, namespace['node'])
                zdb.schedule_action('start').wait(die=True)
                n += 1


def robot_god_token(robot):
    """
    try to retreive the god token from the node 0-robot
    of a node
    """

    try:
        u = urlparse(robot._client.config.data['url'])
        node = j.clients.zos.get('godtoken', data={'host': u.hostname})
        zcont = node.containers.get('zrobot')
        resp = zcont.client.system('zrobot godtoken get').get()
        token = resp.stdout.split(':', 1)[1].strip()
        robot._client.god_token_set(token)
    finally:
        node = j.clients.zos.delete('godtoken')
    return robot