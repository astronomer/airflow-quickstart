from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from minio import Minio
import io

# define the class inheriting from an existing hook class
class MinIOHook(BaseHook):
    """
    Interact with MinIO.
    :param minio_ip: IP Address of the MinIO instance
    :param minio_access_key: MinIO Access Key (default: minioadmin)
    :param minio_secret_key: MinIO Secret Key (default: minioadmin)
    :param secure_connection: toggle connection security (default:false)
    """

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        minio_ip,
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        secure_connection=False,
        *args,
        **kwargs
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.minio_ip = minio_ip
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.secure_connection = secure_connection
        # call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to MinIO."""

        client = Minio(
            self.minio_ip,
            self.minio_access_key,
            self.minio_secret_key,
            secure=self.secure_connection,
        )

        return client

    def put_object(
        self, bucket_name, object_name, data, length, part_size
    ):
        """
        Write an object to Minio
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket (key).
        :param data: An object having callable read() returning bytes object.
        :param length: Data size; (-1 for unknown size and set valid part_size).
        :param part_size: Multipart part size.
        """
        client = self.get_conn()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            length=length,
            part_size=part_size,
        )


class LocalCSVToMinIOOperator(BaseOperator):
    """
    Simple example operator that logs one parameter and returns a string saying hi.
    :param minio_ip: (required) IP address of the MinIO instance.
    :param bucket_name: Name of the bucket.
    :param object_name: Object name in the bucket (key).
    :param data: An object having callable read() returning bytes object.
    :param length: Data size; (default: -1 for unknown size and set valid part_size).
    :param part_size: Multipart part size (default: 10*1024*1024).
    """

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        minio_ip,
        csv_path,
        bucket_name,
        object_name,
        length=-1,
        part_size=10*1024*1024,
        *args,
        **kwargs
    ):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.minio_ip = minio_ip
        self.csv_path = csv_path
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.length = length
        self.part_size = part_size

    # .execute runs when the task runs
    def execute(self, context):

        # open local CSV and write contents to Bytes
        with open(self.csv_path, 'r') as f:
            string_file = f.read()
            data = io.BytesIO(bytes(string_file, 'utf-8'))

        response = MinIOHook(self.minio_ip).put_object(
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            data=data,
            length=self.length,
            part_size=self.part_size,
        )
        self.log.info(response)
