
# This script is used for testing the network failures of a zero-os node using nft ports.

from jumpscale import j
from subprocess import Popen, PIPE
import os
import click

logger = j.logger.get()
@click.command()
@click.option("-n", "--node_ip", help="node ip to test failures on it ", required=True)
def node_client(node_ip):
    node = j.clients.zos.get("test-node", data={"host": node_ip})
    z_robot = node.containers.get('zrobot')
    z_robot_cont_id = z_robot.id

def add_ssh_key(ssh_key_path):
    if os.path.exists(ssh_key_path):
        with open(ssh_key_path) as ssh_key_file:
            key = ssh_key_file.readlines()
        node = j.clients.zos.get("test-node")
        node.client.bash("echo {} >> /root/.ssh/authorized_keys".format(key))
        node.client.nft.open_port(22)
    else:
        raise RuntimeError("There is no ssh key!")


def execute_cmd(cmd):
    sub = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = sub.communicate()
    return out, err


def drop_zrobot_port():
    logger.info("dropping 0-robot port 6600")
    node = j.clients.zos.get("test-node")
    node.client.container.remove_portforward(z_robot_cont_id,6600,6600)

def restore_zrobot_port():
    
    logger.info("restoring 0-robot port 6600")
    node = j.clients.zos.get("test-node")
    node.client.container.add_portforward(z_robot_cont_id, 6600, 6600)



def drop_zdb_ports():
    logger.info("backup all ZDB ports")
    node.client.bash("nft list ruleset ip > zdb_backup.nft")
    logger.info("dropping all ZDB ports")
    node.client.bash(
        "for i in $(nft list ruleset -na | grep 9900 | awk '{print $NF}'); do nft delete rule ip nat pre handle $i ; done")


def drop_redis_port():
    logger.info("dropping redis port 6379")
    node.client.bash("nft delete rule inet filter input handle $(nft list ruleset -na | grep 6379| awk '{print $NF}')")

def backup_nft_table():
    logger.info("backup all nft ports")
    node.client.bash("nft list ruleset > backup.nft")


def restore_nft_table():
    logger.info("restoring all nft ports")
    execute_cmd("ssh {addr} 'nft flush ruleset && nft -f /backup.nft' ".format(addr=node_ip))


# the next function , if you run it on a node you will not be able to reach it again and need to reboot it from ipmi

def drop_management_interface():
    logger.info("deactivating management interface ztrf2qmjmj")
    logger.warning("if you run it on a node you will not be able to reach it again and need to reboot it from ipmi")
    x = input("please enter y to continue or n to quit: ")
    if x == "y":
        node.client.ip.link.down('ztrf2qmjmj')
        logger.info('status of the interface')
        node.client.bash("ip a | grep ztrf2qmjmj ").get()
    else:
        exit


def drop_single_backend_interface():
    logger.info("deactivating first backend interface")
    node.client.ip.link.down('enp2s0f0')
    logger.info('status of the interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()


def drop_all_backend_interfaces():
    logger.info("deactivating all backend interfaces")
    node.client.ip.link.down('enp2s0f0')
    node.client.ip.link.down('enp2s0f1')
    logger.info('status of the first backend interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()
    logger.info('status of the second backend interface')
    node.client.bash("ip a | grep enp2s0f1 ").get()


def restore_single_backend_interface():
    logger.info("activating first backend interface")
    node.client.ip.link.up('enp2s0f0')
    logger.info('status of the interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()


def restore_all_backend_interfaces():
    logger.info("deactivating all backend interfaces")
    node.client.ip.link.up('enp2s0f0')
    node.client.ip.link.up('enp2s0f1')
    logger.info('status of the first backend interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()
    logger.info('status of the second backend interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()
# This script is used for testing the network failures of a zero-os node using nft ports.

from jumpscale import j
from subprocess import Popen, PIPE
import os
import click

logger = j.logger.get()
@click.command()
@click.option("-n", "--node_ip", help="node ip to test failures on it ", required=True)
def node_client(node_ip):
    node = j.clients.zos.get("test-node", data={"host": node_ip})
    z_robot = node.containers.get('zrobot')
    z_robot_cont_id = z_robot.id

def add_ssh_key(ssh_key_path):
    if os.path.exists(ssh_key_path):
        with open(ssh_key_path) as ssh_key_file:
            key = ssh_key_file.readlines()
        node = j.clients.zos.get("test-node")
        node.client.bash("echo {} >> /root/.ssh/authorized_keys".format(key))
        node.client.nft.open_port(22)
    else:
        raise RuntimeError("There is no ssh key!")


def execute_cmd(cmd):
    sub = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = sub.communicate()
    return out, err


def drop_zrobot_port():
    logger.info("dropping 0-robot port 6600")
    node = j.clients.zos.get("test-node")
    node.client.container.remove_portforward(z_robot_cont_id,6600,6600)

def restore_zrobot_port():
    
    logger.info("restoring 0-robot port 6600")
    node = j.clients.zos.get("test-node")
    node.client.container.add_portforward(z_robot_cont_id, 6600, 6600)



def drop_zdb_ports():
    logger.info("backup all ZDB ports")
    node.client.bash("nft list ruleset ip > zdb_backup.nft")
    logger.info("dropping all ZDB ports")
    node.client.bash(
        "for i in $(nft list ruleset -na | grep 9900 | awk '{print $NF}'); do nft delete rule ip nat pre handle $i ; done")


def drop_redis_port():
    logger.info("dropping redis port 6379")
    node.client.bash("nft delete rule inet filter input handle $(nft list ruleset -na | grep 6379| awk '{print $NF}')")

def backup_nft_table():
    logger.info("backup all nft ports")
    node.client.bash("nft list ruleset > backup.nft")


def restore_nft_table():
    logger.info("restoring all nft ports")
    execute_cmd("ssh {addr} 'nft flush ruleset && nft -f /backup.nft' ".format(addr=node_ip))


# the next function , if you run it on a node you will not be able to reach it again and need to reboot it from ipmi

def drop_management_interface():
    logger.info("deactivating management interface ztrf2qmjmj")
    logger.warning("if you run it on a node you will not be able to reach it again and need to reboot it from ipmi")
    x = input("please enter y to continue or n to quit: ")
    if x == "y":
        node.client.ip.link.down('ztrf2qmjmj')
        logger.info('status of the interface')
        node.client.bash("ip a | grep ztrf2qmjmj ").get()
    else:
        exit


def drop_single_backend_interface():
    logger.info("deactivating first backend interface")
    node.client.ip.link.down('enp2s0f0')
    logger.info('status of the interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()


def drop_all_backend_interfaces():
    logger.info("deactivating all backend interfaces")
    node.client.ip.link.down('enp2s0f0')
    node.client.ip.link.down('enp2s0f1')
    logger.info('status of the first backend interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()
    logger.info('status of the second backend interface')
    node.client.bash("ip a | grep enp2s0f1 ").get()


def restore_single_backend_interface():
    logger.info("activating first backend interface")
    node.client.ip.link.up('enp2s0f0')
    logger.info('status of the interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()


def restore_all_backend_interfaces():
    logger.info("deactivating all backend interfaces")
    node.client.ip.link.up('enp2s0f0')
    node.client.ip.link.up('enp2s0f1')
    logger.info('status of the first backend interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()
    logger.info('status of the second backend interface')
    node.client.bash("ip a | grep enp2s0f0 ").get()

def drop_zdb():
    logger.info("causing zdb failure")
    node.client.bash('echo 1 > /sys/block/sda/device/delete')
