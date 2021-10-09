import io

from minio.select import SelectRequest, CSVInputSerialization, CSVOutputSerialization

from minio import Minio
import glob
import os
from decouple import config

MINIO_HOST = os.getenv("MINIO_HOST", config("MINIO_HOST"))


class MyMinio:

    def __init__(self, access, secret):
        self.access = access
        self.secret = secret
        self.minioClient = self._getMinioClient()

    def _getMinioClient(self):
        """
        Create a minio client object
        :return: return the minio client object
        """

        return Minio(
            f'{MINIO_HOST}:9000',
            access_key=self.access,
            secret_key=self.secret,
            secure=False
        )

    def create_bucket(self, bucket_name: str):
        """
        Creates a bucket in minio
        :param bucket_name: the name of the bucket
        """
        if not self.minioClient.bucket_exists(bucket_name):
            self.minioClient.make_bucket(bucket_name)

    def upload_local_directory_to_minio(self, local_path: str, bucket_name: str):
        """
        Uploads local directory contents into a bucket in minio.
        :param local_path: the path of the source local directory
        :param bucket_name: the name of the destination bucket
        """
        assert os.path.isdir(local_path)

        for local_file in glob.glob(local_path + '/**'):
            local_file = local_file.replace(os.sep, "/")
            if not os.path.isfile(local_file):
                self.upload_local_directory_to_minio(
                    local_file, bucket_name)
            else:
                remote_path = os.path.join(
                    local_file[1 + len(local_path):])
                remote_path = remote_path.replace(
                    os.sep, "/")
                self.minioClient.fput_object(bucket_name, remote_path, local_file)

    def read_csv_file_from_minio(self, bucket_name: str, file_name: str):
        """
        Reads a csv file from minio and returns its contents
        :param bucket_name: the name of the bucket the file exists in
        :param file_name: the name of the file to read from
        :return: the contents of the file as a list of all values alongside each other
        """
        with self.minioClient.select_object_content(
                bucket_name,
                file_name,
                SelectRequest(
                    "select * from S3Object",
                    CSVInputSerialization(),
                    CSVOutputSerialization(),
                    request_progress=True,
                ),
        ) as result:
            for data in result.stream():
                data = data.decode().replace('\n', ',').split(',')
                data = [item.strip('"').strip(' ') for item in data]
                data = [item for item in data if item != '']
                return data

    def add_file(self, bucket_name: str, file_name: str, content: str):
        """
        Add a file file_name into bucket_name with an initiate data content
        :param bucket_name: the name of the bucket
        :param file_name: the name of the file to create
        :param content: the initiate contents of the file
        """
        self.minioClient.put_object(
            bucket_name, file_name, io.BytesIO(content.encode('ASCII')), len(content))

    def get_files_in_bucket(self, bucket_name: str):
        """
        Gets the names of the files inside a bucket
        :param bucket_name: the name of the bucket
        :return: a list of the names of the files inside the bucket
        """
        files = self.minioClient.list_objects(bucket_name)

        return [file.object_name for file in files]

    def clear_bucket(self, bucket_name: str):
        """
        Clears the contents of the given bucket
        :param bucket_name: the name of the bucket
        """
        files = self.get_files_in_bucket(bucket_name)
        for file in files:
            self.minioClient.remove_object(bucket_name, file)

