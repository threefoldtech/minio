from unittest import TestCase
from utils.controller import Controller
from jumpscale import j
from subprocess import Popen, PIPE
import time, socket

logger = j.logger.get('s3_failures')


class BaseTest(TestCase):
    file_name = None
    socket.setdefaulttimeout(120) # Minio use _GLOBAL_DEFAULT_TIMEOUT which is None by default

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = j.data.serializer.yaml.load('./config.yaml')
        self.logger = logger

    @classmethod
    def setUpClass(cls):
        """
        Deploy s3.

        function to deploy s3 with one of pre-configured parameters.

        """
        cls.config = j.data.serializer.yaml.load('./config.yaml')
        if cls.config['s3']['deploy']:
            cls.s3_controller = Controller(cls.config)
            cls.s3_service_name = str(time.time()).split('.')[0]
            logger.info("s3 service name : {}".format(cls.s3_service_name))

            data = [cls.config['s3']['instance']['farm'], cls.config['s3']['instance']['size'],
                    cls.config['s3']['instance']['shards'], cls.config['s3']['instance']['parity'],
                    cls.config['s3']['instance']['nsName']]
            instance = cls.s3_controller.deploy(cls.s3_service_name, *data)
            logger.info("wait for deploying {} s3 service".format(cls.s3_service_name))
            try:
                instance.wait(die=True)
            except:
                logger.error("May be there is an error while installing s3! ")
            for _ in range(10):
                cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
                state = cls.s3.service.state
                logger.info(" s3 state : {}".format(state))
                try:
                    state.check('actions', 'install', 'ok')
                    logger.info(" waiting s3 state to be ok ... ")
                    break
                except:
                    time.sleep(5 * 60)
                    logger.info("wait for 5 mins")
        else:
            sub = Popen('zrobot godtoken get', stdout=PIPE, stderr=PIPE, shell=True)
            out, err = sub.communicate()
            god_token = str(out).split(' ')[2]
            cls.s3_controller = Controller(cls.config, god_token)
            cls.s3_service_name = cls.config['s3']['use']['s3_service_name']
            s3_services = []
            cls.s3_service_name = cls.config['s3']['use']['s3_service_name']
            s3_services.append(cls.s3_service_name)
            cls.s3_active_service_name = cls.config['s3']['use']['s3_active_service_name']
            if cls.s3_active_service_name:
                s3_services.append(cls.s3_active_service_name)
            cls.s3_passive_service_name = cls.config['s3']['use']['s3_passive_service_name']
            if cls.s3_passive_service_name:
                s3_services.append(cls.s3_passive_service_name)
            for s3_service in s3_services:
                if s3_service not in cls.s3_controller.s3:
                    logger.error("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                        cls.config['robot']['client']))
                    raise Exception("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                           cls.config['robot']['client']))
        cls.s3 = cls.s3_controller.s3[cls.s3_service_name]

    @classmethod
    def tearDownClass(cls):
        """
        TearDown

        :return:
        """
        pass

    def setUp(self):
        self.s3 = self.s3_controller.s3[self.s3_service_name]

    def tearDown(self):
        pass

