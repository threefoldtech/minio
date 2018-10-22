import signal
import time
from urllib.parse import urlparse

import requests, random
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

    def minio_process_down(self, timeout):
        """
        turn off the minio process, then count how much times it takes to restart
        """
        s3 = self._parent
        url = s3.url['public']
        cont = s3.minio_container

        logger.info('killing minio process')
        job_id = 'minio.%s' % s3.service.guid
        cont.client.job.kill(job_id, signal=signal.SIGINT)
        logger.info('minio process killed')

        logger.info("wait for minio to restart")
        start = time.time()
        while (start + timeout) > time.time():
            try:
                requests.get(url, timeout=0.2)
                end = time.time()
                duration = end - start
                logger.info("minio took %s sec to restart" % duration)
                return True
            except ConnectionError:
                continue
        return False

    def zdb_process_down(self, count=1,timeout=100):
        """
        turn off zdb process , check it will be restart.
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
                n +=1
            except StateCheckError:
                continue
            logger.info('stop %s  process on node %s', zdb.name, namespace['node'])
            zdb_node = j.clients.zos.get(zdb.name,data={"host": namespace['url'][7:-5]})
            zdb_cont_client = zdb_node.containers.get("zerodb_{}".format(zdb.name))
            zdb_node.client.container.terminate(zdb_cont_client.id)
            logger.info('zdb process killed')
            start = time.time()
            while (start + timeout) > time.time():
                try:
                    zdb_node.containers.get("zerodb_{}".format(zdb.name))
                    end = time.time()
                    duration = end - start 
                    logger.info("zdb took %s sec to restart" % duration)
                    return True
                except LookupError:
                    continue
            return False

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

    def tlog_down(self):
        """
            Turn down tlog
        """
        s3 = self._parent
        if not s3:
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])

        try:
            zdb.state.check('status', 'running', 'ok')
            logger.info('stop %s on node %s', zdb.name, tlog['node'])
            zdb.schedule_action('stop').wait(die=True)
        except StateCheckError:
            logger.error("tlog zdb status isn't running")

    def tlog_up(self):
        """
            Turn up tlog
        """
        s3 = self._parent
        if not s3:
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])

        try:
            zdb.state.check('status', 'running', 'ok')
        except StateCheckError:
            logger.info('start %s on node %s', zdb.name, tlog['node'])
            zdb.schedule_action('start').wait(die=True)

    def tlog_status(self):
        """
        Check tlog status
        :return:
        True if tlog status is up
        """
        s3 = self._parent
        if not s3:
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])

        try:
            return zdb.state.check('status', 'running', 'ok')
        except StateCheckError:
            return False

    def kill_tlog(self):
        """
        Tlog is a namespace under a zdb container, This method will terminate this container
        :return:
        """
        s3 = self._parent
        if not s3:
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])

        tlog_node = s3.tlog_node
        import ipdb; ipdb.set_trace()

    def Kill_node_robot_process(self,node_addr=None, timeout=100):
        """
        kill robot process. 
        """
        s3 = self._parent
        if not s3:
            return
            
        if not node_addr:
            farm_name = s3.service.data['data']['farmerIyoOrg']
            capacity = j.clients.threefold_directory.get(interactive=False)
            nodes_data = capacity.api.ListCapacity(query_params={'farmer': farm_name})[1].json()
            for _ in range(len(nodes_data)):
                node_addr = random.choice(nodes_data)["robot_address"][7:-5]
                node = j.clients.zos.get("zrobot", data={"host":node_addr})
                try:
                    node.client.ping()
                    break                         
                except:
                    logger.error(" can't reach %s skipping", node.addr)
                    continue
        else:
            node = j.clients.zos.get("zrobot", data={"host":node_addr})

        logger.info("kill the robot on node{}".format(node_addr))
        node.client.container.terminate(node.containers.get('zrobot').id)

        logger.info("wait for the robot to restart")
        start = time.time()
        while (start + timeout) > time.time():
            try:
                node.containers.get("zrobot")
                end = time.time()
                duration = end - start 
                logger.info("zrobot took %s sec to restart" % duration)
                return True
            except LookupError:
                continue
        return False


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
