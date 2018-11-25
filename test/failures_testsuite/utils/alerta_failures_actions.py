# This script is used for testing the network failures of a zero-os node using nft ports.

from jumpscale import j
from subprocess import Popen, PIPE
import os

logger = j.logger.get()

node_ip = '10.147.20.24'
node_client = j.clients.zos.get("zrobot", data={"host": node_ip})



def add_ssh_key(ssh_key_path):
    if os.path.exists(ssh_key_path):
        with open(ssh_key_path) as ssh_key_file:
            key = ssh_key_file.readlines()

        node_client.client.bash("echo {} >> /root/.ssh/authorized_keys".format(key))
        node_client.client.nft.open_port(22)
    else:
        raise RuntimeError("There is no ssh key!")


def execute_cmd(cmd):
    sub = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = sub.communicate()
    return out, err


def drop_zrobot_port():
    logger.info("dropping 0-robot port 6600")
    node_client.client.bash("nft delete rule ip nat pre handle $(nft list ruleset -na | grep 6600| awk '{print $NF}')")
    node_client.client.bash('nft add rule ip nat pre iifname "zt*" tcp dport 6600 accept')


def drop_zdb_ports():
    logger.info("backup all ZDB ports")
    node_client.client.bash("nft list ruleset ip > zdb_backup.nft")
    logger.info("dropping all ZDB ports")
    node_client.client.bash(
        "for i in $(nft list ruleset -na | grep 9900 | awk '{print $NF}'); do nft delete rule ip nat pre handle $i ; done")


def drop_redis_port():
    logger.info("dropping redis port 6379")
    node_client.client.bash("nft delete rule inet filter input handle $(nft list ruleset -na | grep 6379| awk '{print $NF}')")


def restore_zdb_ports():
    logger.info("restoring all zdb ports")
    execute_cmd("ssh {addr} 'nft -f /zdb_backup.nf' ".format(addr=node_ip))


def backup_nft_table():
    logger.info("backup all nft ports")
    node_client.client.bash("nft list ruleset > backup.nft")


def restore_nft_table():
    logger.info("restoring all nft ports")
    execute_cmd("ssh {addr} 'nft flush ruleset && nft -f /backup.nft' ".format(addr=node_ip))

# the next function , if you run it on a node you will not be able to reach it again and need to reboot it from ipmi

def drop_management_interface():
    logger.info("deactivating management interface eno1")
    logger.warning("if you run it on a node you will not be able to reach it again and need to reboot it from ipmi")
    x= input("please enter y to continue or n to quit: ")
    if x = "y"
        node_client.client.bash('ip lin set eno1 down')
        node_client.client.bash("ip a | grep eno1 ").get()
    else 
        exit
   
def restore_management_interface():
    logger.info("restoring management interface")
    node_client.client.bash('ip lin set eno1 down')
    node_client.client.bash("ip a | grep eno1 ").get()

def drop_single_backend_interface():
    logger.info("deactivating first backend interface")
    node_client.client.bash('ip lin set enp2s0f0 down')
    logger.info('status of the interface')
    node_client.client.bash("ip a | grep enp2s0f0 ").get()

def drop_all_backend_interfaces():
    logger.info("deactivating all backend interfaces")
    node_client.client.bash('ip lin set enp2s0f0 down')
    node_client.client.bash('ip lin set enp2s0f1 down')
    logger.info('status of the first backend interface')
    node_client.client.bash("ip a | grep enp2s0f0 ").get()
    logger.info('status of the second backend interface')
    node_client.client.bash("ip a | grep enp2s0f1 ").get()

def restore_single_backend_interface():
    logger.info("activating first backend interface")
    node_client.client.bash('ip lin set enp2s0f0 up')
    logger.info('status of the interface')
    node_client.client.bash("ip a | grep enp2s0f0 ").get()


def restore_all_backend_interfaces():
    logger.info("deactivating all backend interfaces")
    node_client.client.bash('ip lin set enp2s0f0 up')
    node_client.client.bash('ip lin set enp2s0f1 up')
    logger.info('status of the first backend interface')
    node_client.client.bash("ip a | grep enp2s0f0 ").get()
    logger.info('status of the second backend interface')
node_client.client.bash("ip a | grep enp2s0f1 ").get()
