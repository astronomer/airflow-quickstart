from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from minio import Minio
import io
import json

# define the class inheriting from an existing hook class
class MinIOHook(BaseHook):
    """
    Interact with MinIO.
    :param minio_conn_id: ID of MinIO connection. (default: minio_default)
    """

    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "general"
    hook_name = "MinIOHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        minio_conn_id: str = "minio_default",
        secure_connection=False,
        *args,
        **kwargs,
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.minio_conn_id = minio_conn_id
        self.secure_connection = secure_connection
        # call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to MinIO."""

        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        client = Minio(
            conn.host,
            conn.login,
            conn.password,
            secure=self.secure_connection,
        )

        return client

    def put_object(self, bucket_name, object_name, data, length, part_size):
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

    def list_objects(self, bucket_name, prefix=""):
        """
        List objects in MinIO bucket.
        :param bucket_name: Name of the bucket.
        :param prefix: optional prefix of files to list.
        """

        client = self.get_conn()
        list_of_objects = client.list_objects(bucket_name=bucket_name, prefix=prefix)
        return list_of_objects


class LocalFilesystemToMinIOOperator(BaseOperator):
    """
    Operator that writes content from a local json/csv file or directly
    provided json serializeable information to MinIO.
    :param bucket_name: (required) Name of the bucket.
    :param object_name: (required) Object name in the bucket (key).
    :param local_file_path: Path to the local file that is uploaded to MinIO.
    :param json_serializeable_information: Alternatively to providing a filepath provide json-serializeable information.
    :param minio_conn_id: connection id of the MinIO connection.
    :param data: An object having callable read() returning bytes object.
    :param length: Data size; (default: -1 for unknown size and set valid part_size).
    :param part_size: Multipart part size (default: 10*1024*1024).
    """

    supported_filetypes = ["json", "csv"]
    template_fields = (
        "bucket_name",
        "object_name",
        "local_file_path",
        "json_serializeable_information",
        "minio_conn_id",
        "length",
        "part_size",
    )

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        bucket_name,
        object_name,
        local_file_path=None,
        json_serializeable_information=None,
        minio_conn_id: str = MinIOHook.default_conn_name,
        length=-1,
        part_size=10 * 1024 * 1024,
        *args,
        **kwargs,
    ):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.local_file_path = local_file_path
        self.json_serializeable_information = json_serializeable_information
        self.minio_conn_id = minio_conn_id
        self.length = length
        self.part_size = part_size

        if self.local_file_path:
            self.filetype = self.local_file_path.split("/")[-1].split(".")[1]
            if self.filetype not in self.supported_filetypes:
                raise AirflowException(
                    f"The LocalFilesystemToMinIOOperator currently only supports uploading the following filetypes: {self.supported_filetypes}"
                )
        elif not self.json_serializeable_information:
            raise AirflowException(
                "Provide at least one of the parameters local_file_path or json_serializeable_information"
            )

    # .execute runs when the task runs
    def execute(self, context):

        if self.local_file_path:
            if self.filetype == "csv":
                with open(self.local_file_path, "r") as f:
                    string_file = f.read()
                    data = io.BytesIO(bytes(string_file, "utf-8"))

            if self.filetype == "json":
                with open(self.local_file_path, "r") as f:
                    string_file = f.read()
                    data = io.BytesIO(bytes(json.dumps(string_file), "utf-8"))
        else:
            data = io.BytesIO(
                bytes(json.dumps(self.json_serializeable_information), "utf-8")
            )

        response = MinIOHook(self.minio_conn_id).put_object(
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            data=data,
            length=self.length,
            part_size=self.part_size,
        )
        self.log.info(response)


class MinIOListOperator(BaseOperator):
    """
    List all objects from the MinIO bucket with the given string prefix in name.
    :param bucket_name: Name of the bucket.
    :param prefix: optional prefix of files to list.
    :param minio_conn_id: ID to minio connection.
    """

    def __init__(
        self,
        bucket_name,
        prefix: str = "",
        minio_conn_id: str = MinIOHook.default_conn_name,
        *args,
        **kwargs,
    ):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.minio_conn_id = minio_conn_id

    def execute(self, context):
        list_of_objects = MinIOHook(self.minio_conn_id).list_objects(
            self.bucket_name, self.prefix
        )

        return list_of_objects
