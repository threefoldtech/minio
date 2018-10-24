import os
from urllib.parse import urlparse
import hashlib
from jumpscale import j
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou

logger = j.logger.get()

MiB = 1024**2
GiB = 1024**3


class s3_utils:
    def __init__(self, parent):
        self._parent = parent
        self._client_type = 'public'
        self._client = None

    @property
    def client(self):
        if self._client is None:
            s3 = self._parent.service
            if not s3:
                raise RuntimeError("s3 services not found")

            url = s3.schedule_action('url').wait(die=True).result
            u = urlparse(url[self._client_type])

            self._client = Minio(u.netloc,
                                 access_key=s3.data['data']['minioLogin'],
                                 secret_key=s3.data['data']['minioPassword'],
                                 secure=False)
        return self._client

    def _create_file(self, file_name, size, directory='/tmp'):
        file_path = '{}/{}'.format(directory, file_name)
        with open('{}/{}'.format(directory, file_name), 'wb') as f:
            f.write(os.urandom(size))
        return file_path

    def _create_bucket(self):
        bucket_name = j.data.idgenerator.generateXCharID(16)
        try:
            logger.info("create bucket")
            self.client.make_bucket(bucket_name)
        except BucketAlreadyExists:
            pass
        except BucketAlreadyOwnedByYou:
            pass
        return bucket_name

    def upload_file(self, size=1024*1024):
        logger.info("create a bucket")
        bucket_name = self._create_bucket()
        file_name = j.data.idgenerator.generateXCharID(16)
        file_path = self._create_file(file_name, size)
        file_md5 = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        self.client.fput_object(bucket_name, file_name, file_path)
        os.remove(file_path)
        return file_name, bucket_name, file_md5

    def download_file(self, file_name, bucket_name, upload_file_md5):
        d_file = self.client.get_object(bucket_name, file_name)
        try:
            assert(hashlib.md5(d_file.data).hexdigest() == upload_file_md5)
            logger.info("comparison valid")
        finally:
            self.client.remove_bucket(bucket_name)
