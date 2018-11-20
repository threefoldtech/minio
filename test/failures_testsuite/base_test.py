from unittest import TestCase
from utils.controller import Controller
from jumpscale import j
from subprocess import Popen, PIPE
import time, socket



class BaseTest(TestCase):
    file_name = None
    logger = j.logger.get('s3_failures')
    socket.setdefaulttimeout(120) # Minio use _GLOBAL_DEFAULT_TIMEOUT which is None by default
    general_s3 = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = j.data.serializer.yaml.load('./config.yaml')

    @classmethod
    def setUpClass(cls):
        """
        Deploy s3.

        function to deploy s3 with one of pre-configured parameters.

        """
        cls.config = j.data.serializer.yaml.load('./config.yaml')
        if cls.config['s3']['deploy']:
            if cls.general_s3:
                cls.s3_service_name = cls.general_s3
                cls.logger.info('use {} general s3'.format(cls.general_s3))
            else:
                for _ in range(5):
                    cls.s3_controller = Controller(cls.config)
                    cls.s3_service_name = 's3_{}'.format(str(time.time()).split('.')[0])
                    cls.logger.info("s3 service name : {}".format(cls.s3_service_name))

                    data = [cls.config['s3']['instance']['farm'], cls.config['s3']['instance']['size'],
                            cls.config['s3']['instance']['shards'], cls.config['s3']['instance']['parity'],
                            cls.config['s3']['instance']['nsName']]
                    instance = cls.s3_controller.deploy(cls.s3_service_name, *data)
                    cls.logger.info("wait for deploying {} s3 service".format(cls.s3_service_name))
                    try:
                        cls.logger.info("wait for deploying {} service".format(cls.s3_service_name))
                        instance.wait(die=True)
                        break
                    except Exception as e:
                        cls.logger.error("There is an error while installing s3 .. we will re-install it!")
                        cls.logger.error(e)
                        cls.logger.info('uninstall {} service'.format(cls.s3_service_name))
                        s3_redundant_object = cls.s3_controller.s3[cls.s3_service_name]
                        s3_redundant_object.uninstall()
                        cls.logger.info('delete {} service'.format(cls.s3_service_name))
                        s3_redundant_object.delete()
                else:
                    raise TimeoutError("can't install s3 .. gonna quit!")

                for _ in range(10):
                    cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
                    state = cls.s3.service.state
                    cls.logger.info(" s3 state : {}".format(state))
                    try:
                        cls.logger.info("waiting {} state to be ok ... ".format(cls.s3_service_name))
                        state.check('actions', 'install', 'ok')
                        cls.logger.info('{} install : ok'.format(cls.s3_service_name))
                        break
                    except:
                        time.sleep(5 * 60)
                        cls.logger.info("wait for 5 mins .. then we try again!")
                else:
                    state.check('actions', 'install', 'ok')

                for _ in range(5):
                    try:
                        cls.logger('try to create bucket ... ')
                        cls.s3._create_bucket()
                        cls.logger('minio is working well ... fire test cases')
                        break
                    except RuntimeError:
                        cls.logger.error('wait for 3 mins and try again')
                        time.sleep(3*60)
                else:
                    cls.s3._create_bucket()
                cls.general_s3 = cls.s3_service_name
        else:
            sub = Popen('zrobot godtoken get', stdout=PIPE, stderr=PIPE, shell=True)
            out, err = sub.communicate()
            god_token = str(out).split(' ')[2]
            cls.s3_controller = Controller(cls.config, god_token)
            cls.s3_service_name = cls.config['s3']['use']['s3_service_name']

        cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
        cls.s3.failures.zdb_start_all()

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

