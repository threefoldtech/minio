# This script is used for testing the network failures of a zero-os node using nft ports.

from jumpscale import j
from subprocess import Popen, PIPE
import os
import click
from IPython import embed

logger = j.logger.get()


class AlertaFailures:
    def __init__(self, node_ip):
        self.node_ip = node_ip
        self.node = j.clients.zos.get("test-node", data={"host": node_ip})
        self.z_robot = self.node.containers.get('zrobot')
        self.z_robot_cont_id = self.z_robot.id

    def add_ssh_key(self, ssh_key_path):
        if os.path.exists(ssh_key_path):
            with open(ssh_key_path) as ssh_key_file:
                key = ssh_key_file.readlines()
            self.node.client.bash("echo {} >> /root/.ssh/authorized_keys".format(key))
            self.node.client.nft.open_port(22)
        else:
            raise RuntimeError("There is no ssh key!")

    def execute_cmd(self, cmd):
        sub = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        out, err = sub.communicate()
        return out, err

    def drop_zrobot_port(self):
        logger.info("dropping 0-robot port 6600")
        self.node.client.container.remove_portforward(self.z_robot_cont_id,6600,6600)

    def restore_zrobot_port(self):
        logger.info("restoring 0-robot port 6600")
        self.node.client.container.add_portforward(self.z_robot_cont_id, 6600, 6600)

    def simulate_zdb_failure(self):
        containters = self.node.containers.list()
        for i in containters :
            if i.name[:6] == "zerodb":
                mounts = i.info['container']['arguments']['mount'].popitem()[0]
                mountpoint = mounts[:-16]
                zdb_id = i.id
        logger.info("dropping ZDB container")
        d = self.node.client.bash('lsblk |grep {mount}'.format(mount=mountpoint)).get()
        disk = d.stdout.split(' ')[0][2:][:-1]
        self.node.client.bash('echo 1 > /sys/block/{disk}/device/delete'.format(disk= disk))
        self.node.client.bash('umount {mount}'.format(mount=mountpoint))
        self.node.client.container.terminate(zdb_id)

    def drop_redis_port(self):
        logger.info("dropping redis port 6379")
        self.node.client.nft.drop_port(6379)
        
    def backup_nft_table(self):
        logger.info("backup all nft ports")
        self.node.client.bash("nft list ruleset > backup.nft")


    def restore_nft_table(self):
        logger.info("restoring all nft ports")
        self.execute_cmd("ssh {addr} 'nft flush ruleset && nft -f /backup.nft' ".format(addr=self.node_ip))


    # the next function , if you run it on a node you will not be able to reach it again and need to reboot it from ipmi
    def drop_management_interface(self):
        logger.info("deactivating management interface ztrf2qmjmj")
        logger.warning("if you run it on a node you will not be able to reach it again and need to reboot it from ipmi")
        self.node.client.ip.link.down('ztrf2qmjmj')
        logger.info('status of the interface')
        self.node.client.bash("ip a | grep ztrf2qmjmj ").get()


    def drop_single_backend_interface(self):
        logger.info("deactivating first backend interface")
        self.node.client.ip.link.down('enp2s0f0')
        logger.info('status of the interface')
        self.node.client.bash("ip a | grep enp2s0f0 ").get()


    def drop_all_backend_interfaces(self):
        logger.info("deactivating all backend interfaces")
        self.node.client.ip.link.down('enp2s0f0')
        self.node.client.ip.link.down('enp2s0f1')
        logger.info('status of the first backend interface')
        self.node.client.bash("ip a | grep enp2s0f0 ").get()
        logger.info('status of the second backend interface')
        self.node.client.bash("ip a | grep enp2s0f1 ").get()

    def restore_single_backend_interface(self):
        logger.info("activating first backend interface")
        self.node.client.ip.link.up('enp2s0f0')
        logger.info('status of the interface')
        self.node.client.bash("ip a | grep enp2s0f0 ").get()

    def restore_all_backend_interfaces(self):
        logger.info("deactivating all backend interfaces")
        self.node.client.ip.link.up('enp2s0f0')
        self.node.client.ip.link.up('enp2s0f1')
        logger.info('status of the first backend interface')
        self.node.client.bash("ip a | grep enp2s0f0 ").get()
        logger.info('status of the second backend interface')
        self.node.client.bash("ip a | grep enp2s0f0 ").get()


@click.command()
@click.option("-n", "--node_ip", help="node ip to test failures on it ", required=True)
def main(node_ip):
    alerta = AlertaFailures(node_ip)
    embed()

if __name__ == '__main__':
    main()

