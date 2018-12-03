from unittest import TestCase
from utils.controller import Controller
from jumpscale import j
from subprocess import Popen, PIPE
import time, socket



class BaseTest(TestCase):
    file_name = None
    logger = j.logger.get('s3_failures')
    socket.setdefaulttimeout(60) # Minio use _GLOBAL_DEFAULT_TIMEOUT which is None by default
    s3_service_name = None
    config = j.data.serializer.yaml.load('./config.yaml')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = j.data.serializer.yaml.load('./config.yaml')

    @classmethod
    def setUpClass(cls):
        """
        Deploy s3.

        function to deploy s3 with one of pre-configured parameters.

        """
        if cls.config['s3']['deploy']:
            for _ in range(5):
                cls.s3_controller = Controller(cls.config)
                cls.s3_service_name = 's3_{}'.format(str(time.time()).split('.')[0])
                cls.logger.info("s3 service name : {}".format(cls.s3_service_name))

                data = [cls.config['s3']['instance']['farm'], cls.config['s3']['instance']['size'],
                        cls.config['s3']['instance']['shards'], cls.config['s3']['instance']['parity'],
                        cls.config['s3']['instance']['nsName']]
                instance = cls.s3_controller.deploy(cls.s3_service_name, *data)
                try:
                    cls.logger.info("wait for deploying {} service".format(cls.s3_service_name))
                    instance.wait(die=True, timeout=300)
                    break
                except Exception as e:
                    cls.logger.error("there is an error while installing s3 .. we will re-install it!")
                    cls.logger.error(e)
                    cls.logger.info('uninstall {} service'.format(cls.s3_service_name))
                    s3_object = cls.s3_controller.s3[cls.s3_service_name]
                    s3_object.service.schedule_action('uninstall')
                    cls.logger.info('delete {} service'.format(cls.s3_service_name))
                    s3_object.service.delete()
            else:
                raise TimeoutError("can't install s3 .. gonna quit!")

            cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
            for _ in range(20):
                state = cls.s3.service.state
                cls.logger.info("s3 state : {}".format(state))
                try:
                    state.check('actions', 'install', 'ok')
                    cls.logger.info('{} install : ok'.format(cls.s3_service_name))
                    break
                except:
                    cls.logger.info("waiting {} state to be ok ... ".format(cls.s3_service_name))
                    time.sleep(60)
            else:
                state.check('actions', 'install', 'ok')

            for _ in range(10):
                try:
                    url = cls.s3.url
                    if 'http' in url['public'] and 'http' in url['storage']:
                        cls.logger.info('s3 has a public and storage ip')
                        break
                    cls.logger.info('wait till s3 get the url')
                    time.sleep(60)
                except:
                    time.sleep(60)
            else:
                raise TimeoutError("There is no ip for the s3 ... gonna quit!")
            cls.general_s3 = cls.s3_service_name
        else:
            sub = Popen('zrobot godtoken get', stdout=PIPE, stderr=PIPE, shell=True)
            out, err = sub.communicate()
            god_token = str(out).split(' ')[2]
            cls.s3_controller = Controller(cls.config, god_token)
            cls.s3_service_name = cls.config['s3']['use']['s3_service_name']

        cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
        cls.logger.info('{} url : {}'.format(cls.s3_service_name, cls.s3.url))

        # cls.logger.info('minio config')
        # cls.logger.info(cls.s3.minio_config)

    @classmethod
    def tearDownClass(cls):
        """
        TearDown

        :return:
        """
        cls.config = j.data.serializer.yaml.load('./config.yaml')
        cls.s3_controller = Controller(cls.config)
        cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
        cls.logger.info('minio config')
        cls.logger.info(cls.s3.minio_config)

    def setUp(self):
        self.s3 = self.s3_controller.s3[self.s3_service_name]
        self.wait_and_update(function_name=self.s3.failures.zdb_start_service_all)
        self.wait_and_update(function_name=self.s3.failures.tlog_start_service)

    def tearDown(self):
        pass

    def wait_and_update(self, function_name):
        for _ in range(30):
            try:
                function_name()
                return
            except Exception as e:
                self.logger.warning(e)
                time.sleep(5)
                self.s3_controller = Controller(self.config)
                self.s3 = self.s3_controller.s3[self.s3_service_name]
