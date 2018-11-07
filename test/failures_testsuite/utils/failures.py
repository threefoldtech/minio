import signal
import time
from urllib.parse import urlparse

import requests, random
from requests.exceptions import ConnectionError, ConnectTimeout

from jumpscale import j
from zerorobot.template.state import StateCheckError
from zerorobot.service_collection import ServiceNotFoundError

logger = j.logger.get()


class FailureGenenator:
    def __init__(self, parent):
        self._parent = parent

    def zdb_start_all(self):
        """
        start all the zerodb services used by minio
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        def do(namespace):
            robot = j.clients.zrobot.robots[namespace['node']]
            robot = robot_god_token(robot)
            ns = robot.services.get(name=namespace['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])
            try:
                zdb.state.check('status', 'running', 'ok')
            except StateCheckError:
                logger.info('start %s on node %s', zdb.name, namespace['node'])
                zdb.schedule_action('start').wait(die=True)

        self._parent.execute_all_nodes(do, nodes=s3.service.data['data']['namespaces'])

    def zdb_stop_all(self):
        """
        stop all the zerodb services used by minio
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        def do(namespace):
            robot = j.clients.zrobot.robots[namespace['node']]
            robot = robot_god_token(robot)
            ns = robot.services.get(name=namespace['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])
            try:
                zdb.state.check('status', 'running', 'ok')
                logger.info('stop %s on node %s', zdb.name, namespace['node'])
                zdb.schedule_action('stop').wait(die=True)
            except StateCheckError:
                pass

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
            logger.warning('There is no s3')
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
            logger.info('kill %s zdb process on node %s', zdb.name, namespace['node'])
            zdb_node = j.clients.zos.get(zdb.name,data={"host": namespace['url'][7:-5]})
            zdb_cont_client = zdb_node.containers.get("zerodb_{}".format(zdb.name))
            job_id = "zerodb.{}".format(zdb.name)
            result = zdb_cont_client.client.job.kill(job_id, signal=signal.SIGINT)
            if not result:
                logger.info("zerodb job not exist, retun false")
                return False

            logger.info("wait zdb process to restart. ")
            start = time.time()
            while (start + timeout) > time.time():
                zdb_job = [job for job in zdb_cont_client.client.job.list() if job['cmd']["id"]==job_id]
                if zdb_job:
                    end = time.time()
                    duration = end - start
                    logger.info("zdb took %s sec to restart" % duration)
                    return True
            return False

    def zdb_down(self, count=1, except_namespaces=[]):
        """
        ensure that count zdb are turned off
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        n = 0
        for namespace in s3.service.data['data']['namespaces']:
            if namespace['name'] in except_namespaces:
                continue
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

    def zdb_up(self, count=1, except_namespaces=[]):
        """
        ensure that count zdb are turned on
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        n = 0
        for namespace in s3.service.data['data']['namespaces']:
            if namespace['name'] in except_namespaces:
                continue
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

    def disable_minio_vdisk_ssd(self):
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        dm_vm = s3.dm_robot.services.get(template_name='dm_vm', name=s3.service.guid)
        vdisk = s3.vm_host_robot.services.get(template_name='vdisk', name='%s_s3vm' % dm_vm.guid)
        zerodb = s3.vm_host_robot.services.get(name=vdisk.data['data']['zerodb'])
        storagepools = s3.tlog_node.storagepools.list()
        device = None
        for sp in storagepools:
            for filesystem in sp.list():
                if filesystem.path == zerodb.data['data']['path']:
                    device = sp.device.split('/')[-1]
                    break
            if device:
                break
        else:
            return
        disk = ''.join([i for i in device if not i.isdigit()])
        s3.vm_host.client.bash('echo 1 > /sys/block/{}/device/delete'.format(disk)).get()
        return disk

    def disable_minio_tlog_ssd(self):
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)
        ns = robot.services.get(name=tlog['name'])
        zerodb = robot.services.get(name=ns.data['data']['zerodb'])
        storagepools = s3.vm_host.storagepools.list()
        device = None
        for sp in storagepools:
            for filesystem in sp.list():
                if filesystem.path == zerodb.data['data']['path']:
                    device = sp.device.split('/')[-1]
                    break
            if device:
                break
        else:
            return
        disk = ''.join([i for i in device if not i.isdigit()])
        s3.vm_host.client.bash('echo 1 > /sys/block/{}/device/delete'.format(disk)).get()
        return disk

    def tlog_down(self):
        """
            Turn down tlog
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])

        try:
            zdb.state.check('status', 'running', 'ok')
            logger.info('stop Tlog %s on node %s', zdb.name, tlog['node'])
            zdb.schedule_action('stop').wait(die=True)
        except StateCheckError:
            logger.error("tlog zdb status isn't running")

    def tlog_up(self):
        """
            Turn up tlog
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb = robot.services.get(name=ns.data['data']['zerodb'])

        try:
            zdb.state.check('status', 'running', 'ok')
        except StateCheckError:
            logger.info('start Tlog %s on node %s', zdb.name, tlog['node'])
            zdb.schedule_action('start').wait(die=True)

    def tlog_status(self):
        """
        Check tlog status
        :return:
        True if tlog status is up
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        try:
            ns = robot.services.get(name=tlog['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])
        except ServiceNotFoundError:
            logger.warning("Seems that there is no tlog namespace nor zdb")
            return False

        try:
            return zdb.state.check('status', 'running', 'ok')
        except StateCheckError:
            return False

    def kill_tlog(self):
        """
        Tlog is a namespace under a zdb container, This method will terminate this container but the zrobot will bring
        it back.
        :return:
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return
        logger.info('kill Tlog zdb process, zrobot will bring it back')
        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        ns = robot.services.get(name=tlog['name'])
        zdb_name = ns.data['data']['zerodb']

        tlog_node = s3.tlog_node
        zdb_cont = tlog_node.containers.get(name='zerodb_{}'.format(zdb_name))
        logger.info('kill {} Tlog zdb'.format(zdb_name))
        zdb_cont.stop()
        return zdb_cont.is_running()

    def tlog_die_forever(self):
        """
        Tlog is a namespace under a zdb container, This method will terminate this container forever
        :return:
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        tlog = s3.service.data['data']['tlog']
        robot = j.clients.zrobot.robots[tlog['node']]
        robot = robot_god_token(robot)

        try:
            ns = robot.services.get(name=tlog['name'])
            zdb_name = ns.data['data']['zerodb']
            zdb = robot.services.get(name=zdb_name)
        except ServiceNotFoundError:
            logger.warning("Seems that there is no tlog namespace nor zdb")
            return False

        ns.delete()
        zdb.delete()

        tlog_node = s3.tlog_node
        zdb_cont = tlog_node.containers.get(name='zerodb_{}'.format(zdb_name))
        zdb_cont.stop()
        return zdb_cont.is_running()

    def Kill_node_robot_process(self, node_addr=None, timeout=100):
        """
        kill robot process.
        """
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
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
                node.client.ping()
        else:
            node = j.clients.zos.get("zrobot", data={"host":node_addr})

        logger.info("kill the robot on node{}".format(node_addr))
        zrobot_cl = node.containers.get('zrobot')
        job_id = 'zrobot'
        result = zrobot_cl.client.job.kill(job_id, signal=signal.SIGINT)
        if not result:
            logger.info("zrobot job not exist")
            return False
        logger.info('the robot process killed')

        logger.info("wait for the robot to restart")
        start = time.time()
        while (start + timeout) > time.time():
            zrobot_job = [job for job in zrobot_cl.client.job.list() if job['cmd']["id"]=="zrobot"]
            if zrobot_job:
                end = time.time()
                duration = end - start
                logger.info("zrobot took %s sec to restart" % duration)
                return True
        return False

    def get_tlog_info(self):
        self.tlog = {}
        s3 = self._parent
        if not s3:
            logger.warning('There is no s3')
            return

        minio_config = s3.minio_config.split('\n')
        for data in minio_config:
            if 'address' in data:
                self.tlog['ip'] = data.re.findall(r'[0-9]+(?:\.[0-9]+){3}:[0-9]{4}', data)[0]
                logger.info(' Tlog ip in minio config : {}'.format(self.tlog['ip']))
                break

        self.tlog['s3_data_ip'] = s3.service.data['data']['tlog']['address']
        logger.info(' Tlog ip in s3 data : {}'.format(self.tlog['s3_data_ip']))


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
        j.clients.zos.delete('godtoken')
    return robot
