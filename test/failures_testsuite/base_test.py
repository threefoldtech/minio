from unittest import TestCase
from utils.controller import Controller
from uuid import uuid4
from jumpscale import j
from subprocess import Popen, PIPE
import time, os, hashlib

logger = j.logger.get('s3_failures')


class BaseTest(TestCase):
    file_name = None

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
            if cls.s3_service_name not in cls.s3_controller.s3:
                logger.error("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                    cls.config['robot']['client']))
                raise Exception("cant find {} s3 service under {} robot client".format(cls.s3_service_name,
                                                                                       cls.config['robot']['client']))
        cls.s3 = cls.s3_controller.s3[cls.s3_service_name]
        # cls.s3.failures.zdb_start_all()

    @classmethod
    def tearDownClass(cls):
        """
        TearDown

        :return:
        """
        self = cls()
        self._delete_directory(directory='tmp')

    def setUp(self):
        self.s3 = self.s3_controller.s3[self.s3_service_name]
        self.get_s3_info()
        # logger.info('Start all zdb')
        # self.s3.failures.zdb_start_all()

    def tearDown(self):
        pass

    def upload_file(self):
        """
         - Create random 2M file
         - Calc its md5 checksum hash
         - Rename file to make its name = md5
         - Upload it
        :return: file_name
        """
        self.minio['name'] = 'minio_{}'.format(self.s3_service_name)
        self.minio['bucket'] = 'bucket{}'.format(self.s3_service_name)
        self._create_directory(directory='tmp')
        self.file_name = self._create_file(directory='tmp', size=1024 * 1024 * 2)

        config_minio_cmd = '/bin/mc config host add {} {} {} {}'.format(self.minio['name'],
                                                                        self.minio['minio_ip'],
                                                                        self.minio['username'],
                                                                        self.minio['password'])
        self.logger.info('config minio')
        self.logger.info(config_minio_cmd)
        out, err = self.execute_cmd(cmd=config_minio_cmd)
        if err:
            self.logger.error(err)

        self.logger.info('create {} bucket'.format(self.minio['bucket']))
        create_bucket_cmd = '/bin/mc mb {}/{}'.format(self.minio['name'], self.minio['bucket'])
        out, err = self.execute_cmd(cmd=create_bucket_cmd)
        if err:
            self.logger.warning(err)

        self.logger.info('Upload {} to  {} bucket'.format(self.file_name, self.minio['bucket']))
        err = self._upload_file(self.minio['name'], self.minio['bucket'], 'tmp/{}'.format(self.file_name))
        if err:
            return None

        self.logger.info('{} file has been uploaded'.format(self.file_name))
        return self.file_name

    def _upload_file(self, minio, bucket, file_name):
        upload_cmd = '/bin/mc cp {} {}/{}/{}'.format(file_name, minio, bucket, file_name)
        out, err = self.execute_cmd(cmd=upload_cmd)
        if err:
            self.logger.error(err)
            return err

        return

    def download_file(self, file_name, keep_trying=False):
        """
         - downlaod file
         - return its md5 checksum hash
        :return: str(downloaded_file_md5)
        """
        self.logger.info('downloading {} .... '.format(file_name))
        download_cmd = '/bin/mc cp {}/{}/tmp/{} tmp/{}_out'.format(self.minio['name'], self.minio['bucket'],
                                                                   file_name, file_name)
        out, err = self.execute_cmd(cmd=download_cmd)
        if keep_trying and err:
            for _ in range(50):
                out, err = self.execute_cmd(cmd=download_cmd)
                if not err:
                    break
                else:
                    time.sleep(5)

        if err:
            self.logger.error(err)
            return None

        return self.calc_md5_checksum('tmp/{}_out'.format(file_name))

    def get_s3_info(self):
        self.s3_data = self.s3.service.data['data']
        self.parity = self.s3_data['parityShards']
        self.shards = self.s3_data['dataShards']
        self.logger.info('- shards {}, parity {}'.format(self.shards, self.parity))
        self.minio = {'minio_ip': self.s3_data['minioUrls']['public'],
                      'username': self.s3_data['minioLogin'],
                      'password': self.s3_data['minioPassword']}
        self.tlog = self.s3_data['tlog']

    def execute_cmd(self, cmd):
        sub = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        out, err = sub.communicate()
        return out, err

    def calc_md5_checksum(self, file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _create_directory(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _delete_directory(self, directory):
        os.system("rm -rf {}".format(directory))

    def _create_file(self, directory, size):
        with open('{}/random'.format(directory), 'wb') as fout:
            fout.write(os.urandom(size))  # 1

        file_name = self.calc_md5_checksum('{}/random'.format(directory))

        os.rename('{}/random'.format(directory), '{}/{}'.format(directory, file_name))
        return file_name

    def _delete_file(self, file_path):
        os.system("rm -f {}".format(file_path))
