#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import os
import re
from abc import ABC, abstractmethod
from typing import Optional

import humps
import base64

from hsfs import engine, client
from hsfs.core import storage_connector_api


class StorageConnector(ABC):
    HOPSFS = "HOPSFS"
    S3 = "S3"
    JDBC = "JDBC"
    REDSHIFT = "REDSHIFT"
    ADLS = "ADLS"
    SNOWFLAKE = "SNOWFLAKE"
    KAFKA = "KAFKA"
    GCS = "GCS"
    BIGQUERY = "BIGQUERY"

    def __init__(self, id, name, description, featurestore_id, **kwargs):
        self._id = id
        self._name = name
        self._description = description
        self._featurestore_id = featurestore_id

        self._storage_connector_api = storage_connector_api.StorageConnectorApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type", None)
        for subcls in cls.__subclasses__():
            if subcls.type == json_decamelized["storage_connector_type"]:
                _ = json_decamelized.pop("storage_connector_type")
                return subcls(**json_decamelized)
        raise ValueError

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type", None)
        if self.type == json_decamelized["storage_connector_type"]:
            _ = json_decamelized.pop("storage_connector_type")
            self.__init__(**json_decamelized)
        else:
            raise ValueError("Failed to update storage connector information.")
        return self

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "featurestoreId": self._featurestore_id,
            "storageConnectorType": self.type,
        }

    @property
    def type(self):
        """Type of the connector as string, e.g. "HOPFS, S3, ADLS, REDSHIFT, JDBC or SNOWFLAKE."""
        return self._type

    @property
    def id(self):
        """Id of the storage connector uniquely identifying it in the Feature store."""
        return self._id

    @property
    def name(self):
        """Name of the storage connector."""
        return self._name

    @property
    def description(self):
        """User provided description of the storage connector."""
        return self._description

    @abstractmethod
    def spark_options(self):
        pass

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = None,
    ):
        """Reads a query or a path into a dataframe using the storage connector.

        Note, paths are only supported for object stores like S3, HopsFS and ADLS, while
        queries are meant for JDBC or databases like Redshift and Snowflake.

        # Arguments
            query: By default, the storage connector will read the table configured together
                with the connector, if any. It's possible to overwrite this by passing a SQL
                query here. Defaults to `None`.
            data_format: When reading from object stores such as S3, HopsFS and ADLS, specify
                the file format to be read, e.g. `csv`, `parquet`.
            options: Any additional key/value options to be passed to the connector.
            path: Path to be read from within the bucket of the storage connector. Not relevant
                for JDBC or database based connectors such as Snowflake, JDBC or Redshift.

        # Returns
            `DataFrame`.
        """
        return engine.get_instance().read(self, data_format, options, path)

    def refetch(self):
        """
        Refetch storage connector.
        """
        self._storage_connector_api.refetch(self)

    def _get_path(self, sub_path: str):
        return None

    def connector_options(self):
        """Return prepared options to be passed to an external connector library.
        Not implemented for this connector type.
        """
        return {}


class HopsFSConnector(StorageConnector):
    type = StorageConnector.HOPSFS

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        hopsfs_path=None,
        dataset_name=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # HopsFS
        self._hopsfs_path = hopsfs_path
        self._dataset_name = dataset_name

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        return {}

    def _get_path(self, sub_path: str):
        return os.path.join(self._hopsfs_path, sub_path)


class S3Connector(StorageConnector):
    type = StorageConnector.S3

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        access_key=None,
        secret_key=None,
        server_encryption_algorithm=None,
        server_encryption_key=None,
        bucket=None,
        session_token=None,
        iam_role=None,
        arguments=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # S3
        self._access_key = access_key
        self._secret_key = secret_key
        self._server_encryption_algorithm = server_encryption_algorithm
        self._server_encryption_key = server_encryption_key
        self._bucket = bucket
        self._session_token = session_token
        self._iam_role = iam_role
        self._arguments = (
            {opt["name"]: opt["value"] for opt in arguments} if arguments else {}
        )

    @property
    def access_key(self):
        """Access key."""
        return self._access_key

    @property
    def secret_key(self):
        """Secret key."""
        return self._secret_key

    @property
    def server_encryption_algorithm(self):
        """Encryption algorithm if server-side S3 bucket encryption is enabled."""
        return self._server_encryption_algorithm

    @property
    def server_encryption_key(self):
        """Encryption key if server-side S3 bucket encryption is enabled."""
        return self._server_encryption_key

    @property
    def bucket(self):
        """Return the bucket for S3 connectors."""
        return self._bucket

    @property
    def session_token(self):
        """Session token."""
        return self._session_token

    @property
    def iam_role(self):
        """IAM role."""
        return self._iam_role

    @property
    def path(self):
        """If the connector refers to a path (e.g. S3) - return the path of the connector"""
        return "s3://" + self._bucket

    @property
    def arguments(self):
        return self._arguments

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        return self._arguments

    def prepare_spark(self, path: Optional[str] = None):
        """Prepare Spark to use this Storage Connector.

        ```python
        conn.prepare_spark()

        spark.read.format("json").load("s3a://[bucket]/path")

        # or
        spark.read.format("json").load(conn.prepare_spark("s3a://[bucket]/path"))
        ```

        # Arguments
            path: Path to prepare for reading from cloud storage. Defaults to `None`.
        """
        return engine.get_instance().setup_storage_connector(self, path)

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = "",
    ):
        """Reads a query or a path into a dataframe using the storage connector.

        Note, paths are only supported for object stores like S3, HopsFS and ADLS, while
        queries are meant for JDBC or databases like Redshift and Snowflake.

        # Arguments
            query: Not relevant for S3 connectors.
            data_format: The file format of the files to be read, e.g. `csv`, `parquet`.
            options: Any additional key/value options to be passed to the S3 connector.
            path: Path within the bucket to be read.

        # Returns
            `DataFrame`.
        """
        self.refetch()
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if not path.startswith("s3://"):
            path = self._get_path(path)
            print(
                "Prepending default bucket specified on connector, final path: {}".format(
                    path
                )
            )

        return engine.get_instance().read(self, data_format, options, path)

    def _get_path(self, sub_path: str):
        return os.path.join(self.path, sub_path)


class RedshiftConnector(StorageConnector):
    type = StorageConnector.REDSHIFT
    JDBC_FORMAT = "jdbc"

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        cluster_identifier=None,
        database_driver=None,
        database_endpoint=None,
        database_name=None,
        database_port=None,
        table_name=None,
        database_user_name=None,
        auto_create=None,
        database_password=None,
        database_group=None,
        iam_role=None,
        arguments=None,
        expiration=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # Redshift
        self._cluster_identifier = cluster_identifier
        self._database_driver = database_driver
        self._database_endpoint = database_endpoint
        self._database_name = database_name
        self._database_port = database_port
        self._table_name = table_name
        self._database_user_name = database_user_name
        self._auto_create = auto_create
        self._database_password = database_password
        self._database_group = database_group
        self._iam_role = iam_role
        self._arguments = (
            {arg["name"]: arg.get("value", None) for arg in arguments}
            if isinstance(arguments, list)
            else arguments
        )
        self._expiration = expiration

    @property
    def cluster_identifier(self):
        """Cluster identifier for redshift cluster."""
        return self._cluster_identifier

    @property
    def database_driver(self):
        """Database endpoint for redshift cluster."""
        return self._database_driver

    @property
    def database_endpoint(self):
        """Database endpoint for redshift cluster."""
        return self._database_endpoint

    @property
    def database_name(self):
        """Database name for redshift cluster."""
        return self._database_name

    @property
    def database_port(self):
        """Database port for redshift cluster."""
        return self._database_port

    @property
    def table_name(self):
        """Table name for redshift cluster."""
        return self._table_name

    @property
    def database_user_name(self):
        """Database username for redshift cluster."""
        return self._database_user_name

    @property
    def auto_create(self):
        """Database username for redshift cluster."""
        return self._auto_create

    @property
    def database_group(self):
        """Database username for redshift cluster."""
        return self._database_group

    @property
    def database_password(self):
        """Database password for redshift cluster."""
        return self._database_password

    @property
    def iam_role(self):
        """IAM role."""
        return self._iam_role

    @property
    def expiration(self):
        """Cluster temporary credential expiration time."""
        return self._expiration

    @property
    def arguments(self):
        """Additional JDBC, REDSHIFT, or Snowflake arguments."""
        if isinstance(self._arguments, dict):
            return ",".join(
                [k + ("" if v is None else "=" + v) for k, v in self._arguments.items()]
            )
        return self._arguments

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        connstr = (
            "jdbc:redshift://"
            + self._cluster_identifier
            + "."
            + self._database_endpoint
            + ":"
            + str(self._database_port)
            + "/"
            + self._database_name
        )
        if isinstance(self.arguments, str):
            connstr = connstr + "?" + self.arguments
        props = {
            "url": connstr,
            "driver": self._database_driver,
            "user": self._database_user_name,
            "password": self._database_password,
        }
        if self._table_name is not None:
            props["dbtable"] = self._table_name

        return props

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = None,
    ):
        """Reads a table or query into a dataframe using the storage connector.

        # Arguments
            query: By default, the storage connector will read the table configured together
                with the connector, if any. It's possible to overwrite this by passing a SQL
                query here. Defaults to `None`.
            data_format: Not relevant for JDBC based connectors such as Redshift.
            options: Any additional key/value options to be passed to the JDBC connector.
            path: Not relevant for JDBC based connectors such as Redshift.

        # Returns
            `DataFrame`.
        """
        # refetch to update temporary credentials
        self._storage_connector_api.refetch(self)
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query
            # if table also specified we override to use query
            options.pop("dbtable", None)

        return engine.get_instance().read(self, self.JDBC_FORMAT, options, None)

    def refetch(self):
        """
        Refetch storage connector in order to retrieve updated temporary credentials.
        """
        self._storage_connector_api.refetch(self)


class AdlsConnector(StorageConnector):
    type = StorageConnector.ADLS

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        generation=None,
        directory_id=None,
        application_id=None,
        service_credential=None,
        account_name=None,
        container_name=None,
        spark_options=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # ADL
        self._generation = generation
        self._directory_id = directory_id
        self._application_id = application_id
        self._account_name = account_name
        self._service_credential = service_credential
        self._container_name = container_name

        self._spark_options = (
            {opt["name"]: opt["value"] for opt in spark_options}
            if spark_options
            else {}
        )

    @property
    def generation(self):
        """Generation of the ADLS storage connector"""
        return self._generation

    @property
    def directory_id(self):
        """Directory ID of the ADLS storage connector"""
        return self._directory_id

    @property
    def application_id(self):
        """Application ID of the ADLS storage connector"""
        return self._application_id

    @property
    def account_name(self):
        """Account name of the ADLS storage connector"""
        return self._account_name

    @property
    def container_name(self):
        """Container name of the ADLS storage connector"""
        return self._container_name

    @property
    def service_credential(self):
        """Service credential of the ADLS storage connector"""
        return self._service_credential

    @property
    def path(self):
        """If the connector refers to a path (e.g. ADLS) - return the path of the connector"""
        if self.generation == 2:
            return "abfss://{}@{}.dfs.core.windows.net".format(
                self.container_name, self.account_name
            )
        else:
            return "adl://{}.azuredatalakestore.net".format(self.account_name)

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        return self._spark_options

    def prepare_spark(self, path: Optional[str] = None):
        """Prepare Spark to use this Storage Connector.

        ```python
        conn.prepare_spark()

        spark.read.format("json").load("abfss://[container-name]@[account_name].dfs.core.windows.net/[path]")

        # or
        spark.read.format("json").load(conn.prepare_spark("abfss://[container-name]@[account_name].dfs.core.windows.net/[path]"))
        ```

        # Arguments
            path: Path to prepare for reading from cloud storage. Defaults to `None`.
        """
        return engine.get_instance().setup_storage_connector(self, path)

    def _get_path(self, sub_path: str):
        return os.path.join(self.path, sub_path)

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = "",
    ):
        """Reads a path into a dataframe using the storage connector.
        # Arguments
            query: Not relevant for ADLS connectors.
            data_format: The file format of the files to be read, e.g. `csv`, `parquet`.
            options: Any additional key/value options to be passed to the ADLS connector.
            path: Path within the bucket to be read. For example, path=`path` will read directly from the container specified on connector by constructing the URI as 'abfss://[container-name]@[account_name].dfs.core.windows.net/[path]'.
            If no path is specified default container path will be used from connector.

        # Returns
            `DataFrame`.
        """
        path = path.strip()
        if not path.startswith("abfss://") or path.startswith("adl://"):
            path = self._get_path(path)
            print(
                "Using default container specified on connector, final path: {}".format(
                    path
                )
            )

        return engine.get_instance().read(self, data_format, options, path)


class SnowflakeConnector(StorageConnector):
    type = StorageConnector.SNOWFLAKE
    SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        database=None,
        password=None,
        token=None,
        role=None,
        schema=None,
        table=None,
        url=None,
        user=None,
        warehouse=None,
        application=None,
        sf_options=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # SNOWFLAKE
        self._url = url
        self._warehouse = warehouse
        self._database = database
        self._user = user
        self._password = password
        self._token = token
        self._schema = schema
        self._table = table
        self._role = role
        self._application = application

        self._options = (
            {opt["name"]: opt["value"] for opt in sf_options} if sf_options else {}
        )

    @property
    def url(self):
        """URL of the Snowflake storage connector"""
        return self._url

    @property
    def warehouse(self):
        """Warehouse of the Snowflake storage connector"""
        return self._warehouse

    @property
    def database(self):
        """Database of the Snowflake storage connector"""
        return self._database

    @property
    def user(self):
        """User of the Snowflake storage connector"""
        return self._user

    @property
    def password(self):
        """Password of the Snowflake storage connector"""
        return self._password

    @property
    def token(self):
        """OAuth token of the Snowflake storage connector"""
        return self._token

    @property
    def schema(self):
        """Schema of the Snowflake storage connector"""
        return self._schema

    @property
    def table(self):
        """Table of the Snowflake storage connector"""
        return self._table

    @property
    def role(self):
        """Role of the Snowflake storage connector"""
        return self._role

    @property
    def account(self):
        """Account of the Snowflake storage connector"""
        return self._url.replace("https://", "").replace(".snowflakecomputing.com", "")

    @property
    def application(self):
        """Application of the Snowflake storage connector"""
        return self._application

    @property
    def options(self):
        """Additional options for the Snowflake storage connector"""
        return self._options

    def snowflake_connector_options(self):
        """Alias for `connector_options`"""
        return self.connector_options()

    def connector_options(self):
        """In order to use the `snowflake.connector` Python library, this method
        prepares a Python dictionary with the needed arguments for you to connect to
        a Snowflake database.

        ```python
        import snowflake.connector

        sc = fs.get_storage_connector("snowflake_conn")
        ctx = snowflake.connector.connect(**sc.connector_options())
        ```
        """
        props = {
            "user": self._user,
            "account": self.account,
            "database": self._database + "/" + self._schema,
        }
        if self._password:
            props["password"] = self._password
        else:
            props["authenticator"] = "oauth"
            props["token"] = self._token
        if self._warehouse:
            props["warehouse"] = self._warehouse
        if self._application:
            props["application"] = self._application
        return props

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        props = self._options
        props["sfURL"] = self._url
        props["sfSchema"] = self._schema
        props["sfDatabase"] = self._database
        props["sfUser"] = self._user
        if self._password:
            props["sfPassword"] = self._password
        else:
            props["sfAuthenticator"] = "oauth"
            props["sfToken"] = self._token
        if self._warehouse:
            props["sfWarehouse"] = self._warehouse
        if self._application:
            props["application"] = self._application
        if self._role:
            props["sfRole"] = self._role
        if self._table:
            props["dbtable"] = self._table

        return props

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = None,
    ):
        """Reads a table or query into a dataframe using the storage connector.

        # Arguments
            query: By default, the storage connector will read the table configured together
                with the connector, if any. It's possible to overwrite this by passing a SQL
                query here. Defaults to `None`.
            data_format: Not relevant for Snowflake connectors.
            options: Any additional key/value options to be passed to the engine.
            path: Not relevant for Snowflake connectors.

        # Returns
            `DataFrame`.
        """
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query
            # if table also specified we override to use query
            options.pop("dbtable", None)

        return engine.get_instance().read(self, self.SNOWFLAKE_FORMAT, options, None)


class JdbcConnector(StorageConnector):
    type = StorageConnector.JDBC
    JDBC_FORMAT = "jdbc"

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        connection_string=None,
        arguments=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # JDBC
        self._connection_string = connection_string
        self._arguments = arguments

    @property
    def connection_string(self):
        """JDBC connection string."""
        return self._connection_string

    @property
    def arguments(self):
        """Additional JDBC arguments. When running hsfs with PySpark/Spark in Hopsworks,
        the driver is automatically provided in the classpath but you need to set the `driver` argument to
        `com.mysql.cj.jdbc.Driver` when creating the Storage Connector"""
        return self._arguments

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        options = (
            {arg.get("name"): arg.get("value") for arg in self._arguments}
            if self._arguments
            else {}
        )

        options["url"] = self._connection_string

        return options

    def read(
        self,
        query: str,
        data_format: str = None,
        options: dict = {},
        path: str = None,
    ):
        """Reads a query into a dataframe using the storage connector.

        # Arguments
            query: A SQL query to be read.
            data_format: Not relevant for JDBC based connectors.
            options: Any additional key/value options to be passed to the JDBC connector.
            path: Not relevant for JDBC based connectors.

        # Returns
            `DataFrame`.
        """
        self.refetch()
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query

        return engine.get_instance().read(self, self.JDBC_FORMAT, options, None)


class KafkaConnector(StorageConnector):
    type = StorageConnector.KAFKA
    SPARK_FORMAT = "kafka"

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        bootstrap_servers=None,
        security_protocol=None,
        ssl_truststore_location=None,
        ssl_truststore_password=None,
        ssl_keystore_location=None,
        ssl_keystore_password=None,
        ssl_key_password=None,
        ssl_endpoint_identification_algorithm=None,
        options=None,
        external_kafka=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        # KAFKA
        self._bootstrap_servers = bootstrap_servers
        self._security_protocol = security_protocol
        self._ssl_truststore_location = engine.get_instance().add_file(
            ssl_truststore_location
        )
        self._ssl_truststore_password = ssl_truststore_password
        self._ssl_keystore_location = engine.get_instance().add_file(
            ssl_keystore_location
        )
        self._ssl_keystore_password = ssl_keystore_password
        self._ssl_key_password = ssl_key_password
        self._ssl_endpoint_identification_algorithm = (
            ssl_endpoint_identification_algorithm
        )
        self._options = (
            {option["name"]: option["value"] for option in options}
            if options is not None
            else {}
        )
        self._external_kafka = external_kafka
        self._pem_files_created = False

    @property
    def bootstrap_servers(self):
        """Bootstrap servers string."""
        return self._bootstrap_servers

    @property
    def security_protocol(self):
        """Bootstrap servers string."""
        return self._security_protocol

    @property
    def ssl_truststore_location(self):
        """Bootstrap servers string."""
        return self._ssl_truststore_location

    @property
    def ssl_keystore_location(self):
        """Bootstrap servers string."""
        return self._ssl_keystore_location

    @property
    def ssl_endpoint_identification_algorithm(self):
        """Bootstrap servers string."""
        return self._ssl_endpoint_identification_algorithm

    @property
    def options(self):
        """Bootstrap servers string."""
        return self._options

    def kafka_options(self):
        """Return prepared options to be passed to kafka, based on the additional arguments.
        https://kafka.apache.org/documentation/
        """
        config = {}

        # set kafka storage connector options
        config.update(self.options)

        # set connection properties
        config.update(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "security.protocol": self.security_protocol,
            }
        )

        # set ssl
        config[
            "ssl.endpoint.identification.algorithm"
        ] = self._ssl_endpoint_identification_algorithm

        if not self._external_kafka:
            self._ssl_truststore_location = (
                client.get_instance()._get_jks_trust_store_path()
            )
            self._ssl_truststore_password = client.get_instance()._cert_key
            self._ssl_keystore_location = (
                client.get_instance()._get_jks_key_store_path()
            )
            self._ssl_keystore_password = client.get_instance()._cert_key
            self._ssl_key_password = client.get_instance()._cert_key

        if self.ssl_truststore_location is not None:
            config["ssl.truststore.location"] = self._ssl_truststore_location
        if self._ssl_truststore_password is not None:
            config["ssl.truststore.password"] = self._ssl_truststore_password
        if self.ssl_keystore_location is not None:
            config["ssl.keystore.location"] = self._ssl_keystore_location
        if self._ssl_keystore_password is not None:
            config["ssl.keystore.password"] = self._ssl_keystore_password
        if self._ssl_key_password is not None:
            config["ssl.key.password"] = self._ssl_key_password

        return config

    def confluent_options(self):
        """Return prepared options to be passed to confluent_kafka, based on the provided apache spark configuration.
        Right now only producer values with Importance >= medium are implemented.
        https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
        """
        config = {}
        kafka_options = self.kafka_options()
        for key, value in kafka_options.items():
            if (
                key
                in [
                    "ssl.truststore.location",
                    "ssl.truststore.password",
                    "ssl.keystore.location",
                    "ssl.keystore.password",
                ]
                and not self._pem_files_created
            ):
                (
                    ca_chain_path,
                    client_cert_path,
                    client_key_path,
                ) = client.get_instance()._write_pem(
                    kafka_options["ssl.keystore.location"],
                    kafka_options["ssl.keystore.password"],
                    kafka_options["ssl.truststore.location"],
                    kafka_options["ssl.truststore.password"],
                    f"kafka_sc_{client.get_instance()._project_id}_{self._id}",
                )
                self._pem_files_created = True
                config["ssl.ca.location"] = ca_chain_path
                config["ssl.certificate.location"] = client_cert_path
                config["ssl.key.location"] = client_key_path
            elif key == "sasl.jaas.config":
                groups = re.search(
                    "(.+?) .*username=[\"'](.+?)[\"'] .*password=[\"'](.+?)[\"']",
                    value,
                )
                if "sasl.mechanisms" not in config:
                    mechanism = groups.group(1)
                    mechanism_value = None
                    if (
                        mechanism
                        == "org.apache.kafka.common.security.plain.PlainLoginModule"
                    ):
                        mechanism_value = "PLAIN"
                    elif (
                        mechanism
                        == "org.apache.kafka.common.security.scram.ScramLoginModule"
                    ):
                        mechanism_value = "SCRAM-SHA-256"  # could also be SCRAM-SHA-512
                    elif (
                        mechanism
                        == "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
                    ):
                        mechanism_value = "OAUTHBEARER"
                config["sasl.mechanisms"] = mechanism_value
                config["sasl.username"] = groups.group(2)
                config["sasl.password"] = groups.group(3)
            elif key == "ssl.endpoint.identification.algorithm":
                config[key] = "none" if value == "" else value
            elif key == "queued.max.requests":
                config["queue.buffering.max.messages"] = value
            elif key == "queued.max.request.bytes":
                config["queue.buffering.max.kbytes"] = value
            elif key in [
                "bootstrap.servers",
                "security.protocol",
                "compression.type",
                "sasl.mechanism",
                "request.timeout.ms",
                "group.id",
                "transactional.id",
                "transaction.timeout.ms",
                "enable.idempotence",
                "message.max.bytes",
                "linger.ms",
                "retries",
                "retry.backoff.ms",
                "acks",
                "socket.connection.setup.timeout.ms",
                "connections.max.idle.ms",
                "reconnect.backoff.ms",
                "reconnect.backoff.max.ms",
                "delivery.timeout.ms",
            ]:
                # same between config
                config[key] = value
            else:
                # ignored values (if not specified then configuration is ignored)
                continue

        return config

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional arguments.
        This is done by just adding 'kafka.' prefix to kafka_options.
        https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
        """
        config = {}
        for key, value in self.kafka_options().items():
            config[f"{KafkaConnector.SPARK_FORMAT}.{key}"] = value

        return config

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = None,
    ):
        """NOT SUPPORTED."""
        raise NotImplementedError(
            "Reading a Kafka Stream into a static Spark Dataframe is not supported."
        )

    def read_stream(
        self,
        topic: str,
        topic_pattern: bool = False,
        message_format: str = "avro",
        schema: str = None,
        options: dict = {},
        include_metadata: bool = False,
    ):
        """Reads a Kafka stream from a topic or multiple topics into a Dataframe.

        !!! warning "Engine Support"
            **Spark only**

            Reading from data streams using Pandas/Python as engine is currently not supported.
            Python/Pandas has no notion of streaming.

        # Arguments
            topic: Name or pattern of the topic(s) to subscribe to.
            topic_pattern: Flag to indicate if `topic` string is a pattern.
                Defaults to `False`.
            message_format: The format of the messages to use for decoding.
                Can be `"avro"` or `"json"`. Defaults to `"avro"`.
            schema: Optional schema, to use for decoding, can be an Avro schema string for
                `"avro"` message format, or for JSON encoding a Spark StructType schema,
                or a DDL formatted string. Defaults to `None`.
            options: Additional options as key/value string pairs to be passed to Spark.
                Defaults to `{}`.
            include_metadata: Indicate whether to return additional metadata fields from
                messages in the stream. Otherwise, only the decoded value fields are
                returned. Defaults to `False`.

        # Raises
            `ValueError`: Malformed arguments.

        # Returns
            `StreamingDataframe`: A Spark streaming dataframe.
        """
        if message_format.lower() not in ["avro", "json", None]:
            raise ValueError("Can only read JSON and AVRO encoded records from Kafka.")

        if topic_pattern is True:
            options["subscribePattern"] = topic
        else:
            options["subscribe"] = topic

        # if include_headers is True:
        #    stream = stream.option("includeHeaders", "true")
        #    kafka_cols.append(col("headers"))

        return engine.get_instance().read_stream(
            self,
            message_format.lower(),
            schema,
            options,
            include_metadata,
        )


class GcsConnector(StorageConnector):
    type = StorageConnector.GCS
    GS_FS_PREFIX = "gs://"  # Google Storage Filesystem prefix

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        key_path=None,
        bucket=None,
        algorithm=None,
        encryption_key=None,
        encryption_key_hash=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)

        self._bucket = bucket
        self._key_path = key_path
        self._algorithm = algorithm
        self._encryption_key = encryption_key
        self._encryption_key_hash = encryption_key_hash

    @property
    def key_path(self):
        """JSON keyfile for service account"""
        return self._key_path

    @property
    def algorithm(self):
        """Encryption Algorithm"""
        return self._algorithm

    @property
    def encryption_key(self):
        """Encryption Key"""
        return self._encryption_key

    @property
    def encryption_key_hash(self):
        """Encryption Key Hash"""
        return self._encryption_key_hash

    @property
    def path(self):
        """the path of the connector along with gs file system prefixed"""
        return self.GS_FS_PREFIX + self._bucket

    @property
    def bucket(self):
        """GCS Bucket"""
        return self._bucket

    def _get_path(self, sub_path: str):
        if sub_path:
            return os.path.join(self.path, sub_path)
        else:
            return self.path

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        return {}

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = "",
    ):
        """Reads GCS path into a dataframe using the storage connector.

        To read directly from the default bucket, you can omit the path argument:
        ```python
        conn.read(data_format='spark_formats')
        ```
        Or to read objects from default bucket provide the object path without gsUtil URI schema. For example,
        following will read from a path gs://bucket_on_connector/Path/object :
        ```python
        conn.read(data_format='spark_formats', paths='Path/object')
        ```
        Or to read with full gsUtil URI path,
        ```python
        conn.read(data_format='spark_formats',path='gs://BUCKET/DATA')
        ```
        # Arguments
            query: Not relevant for GCS connectors.
            data_format: Spark data format. Defaults to `None`.
            options: Spark options. Defaults to `None`.
            path: GCS path. Defaults to `None`.
        # Raises
            `ValueError`: Malformed arguments.

        # Returns
            `Dataframe`: A Spark dataframe.
        """
        # validate engine supports connector type
        if not engine.get_instance().is_connector_type_supported(self.type):
            raise NotImplementedError(
                "GCS connector not yet supported for engine: " + engine.get_type()
            )

        # validate path begins with gs://
        if not path.startswith(self.GS_FS_PREFIX):
            path = self._get_path(path)
            print(
                "Prepending default bucket specified on connector, final path: {}".format(
                    path
                )
            )

        return engine.get_instance().read(self, data_format, options, path)

    def prepare_spark(self, path: Optional[str] = None):
        """Prepare Spark to use this Storage Connector.

        ```python
        conn.prepare_spark()
        spark.read.format("json").load("gs://bucket/path")
        # or
        spark.read.format("json").load(conn.prepare_spark("gs://bucket/path"))
        ```

        # Arguments
            path: Path to prepare for reading from Google cloud storage. Defaults to `None`.
        """
        return engine.get_instance().setup_storage_connector(self, path)


class BigQueryConnector(StorageConnector):
    type = StorageConnector.BIGQUERY
    BIGQUERY_FORMAT = "bigquery"
    BIGQ_CREDENTIALS = "credentials"
    BIGQ_PARENT_PROJECT = "parentProject"
    BIGQ_MATERIAL_DATASET = "materializationDataset"
    BIGQ_VIEWS_ENABLED = "viewsEnabled"
    BIGQ_PROJECT = "project"
    BIGQ_DATASET = "dataset"

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        description=None,
        # members specific to type of connector
        key_path=None,
        parent_project=None,
        dataset=None,
        query_table=None,
        query_project=None,
        materialization_dataset=None,
        arguments=None,
        **kwargs,
    ):
        super().__init__(id, name, description, featurestore_id)
        self._key_path = key_path
        self._parent_project = parent_project
        self._dataset = dataset
        self._query_table = query_table
        self._query_project = query_project
        self._materialization_dataset = materialization_dataset
        self._arguments = (
            {opt["name"]: opt["value"] for opt in arguments} if arguments else {}
        )

    @property
    def key_path(self):
        """JSON keyfile for service account"""
        return self._key_path

    @property
    def parent_project(self):
        """BigQuery parent project (Google Cloud Project ID of the table to bill for the export)"""
        return self._parent_project

    @property
    def dataset(self):
        """BigQuery dataset (The dataset containing the table)"""
        return self._dataset

    @property
    def query_table(self):
        """BigQuery table name"""
        return self._query_table

    @property
    def query_project(self):
        """BigQuery project (The Google Cloud Project ID of the table)"""
        return self._query_project

    @property
    def materialization_dataset(self):
        """BigQuery materialization dataset (The dataset where the materialized view is going to be created,
        used in case of query)"""
        return self._materialization_dataset

    @property
    def arguments(self):
        """Additional spark options"""
        return self._arguments

    def connector_options(self):
        """Return options to be passed to an external BigQuery connector library"""
        props = {
            "key_path": self._key_path,
            "project_id": self._query_project,
            "dataset_id": self._dataset,
            "parent_project": self._parent_project,
        }
        return props

    def spark_options(self):
        """Return spark options to be set for BigQuery spark connector"""
        properties = self._arguments
        properties[self.BIGQ_PARENT_PROJECT] = self._parent_project

        local_key_path = engine.get_instance().add_file(self._key_path)
        with open(local_key_path, "rb") as credentials_file:
            properties[self.BIGQ_CREDENTIALS] = str(
                base64.b64encode(credentials_file.read()), "utf-8"
            )

        if self._materialization_dataset:
            properties[self.BIGQ_MATERIAL_DATASET] = self._materialization_dataset
            properties[self.BIGQ_VIEWS_ENABLED] = "true"

        if self._query_project:
            properties[self.BIGQ_PROJECT] = self._query_project

        if self._dataset:
            properties[self.BIGQ_DATASET] = self._dataset

        return properties

    def read(
        self,
        query: str = None,
        data_format: str = None,
        options: dict = {},
        path: str = None,
    ):
        """Reads results from BigQuery into a spark dataframe using the storage connector.

          Reading from bigquery is done via either specifying the BigQuery table or BigQuery query.
          For example, to read from a BigQuery table, set the BigQuery project, dataset and table on storage connector
          and read directly from the corresponding path.
            ```python
            conn.read()
            ```
          OR, to read results from a BigQuery query, set `Materialization Dataset` on storage connector,
           and pass your SQL to `query` argument.
            ```python
            conn.read(query='SQL')
            ```
          Optionally, passing `query` argument will take priority at runtime if the table options were also set
          on the storage connector. This allows user to run from both a query or table with same connector, assuming
          all fields were set.
          Also, user can set the `path` argument to a bigquery table path to read at runtime,
           if table options were not set initially while creating the connector.
            ```python
            conn.read(path='project.dataset.table')
            ```

        # Arguments
            query: BigQuery query. Defaults to `None`.
            data_format: Spark data format. Defaults to `None`.
            options: Spark options. Defaults to `None`.
            path: BigQuery table path. Defaults to `None`.

        # Raises
            `ValueError`: Malformed arguments.

        # Returns
            `Dataframe`: A Spark dataframe.
        """
        # validate engine supports connector type
        if not engine.get_instance().is_connector_type_supported(self.type):
            raise NotImplementedError(
                "BigQuery connector not yet supported for engine: " + engine.get_type()
            )
        # merge user spark options on top of default spark options
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            if not {self.BIGQ_MATERIAL_DATASET, self.BIGQ_VIEWS_ENABLED}.issubset(
                options.keys()
            ):
                raise ValueError(
                    "BigQuery materialization views should be enabled for SQL query. "
                    "Set spark options viewsEnabled=True and "
                    + self.BIGQ_MATERIAL_DATASET
                    + "=<temporaryDatasetName> to options argument or instead use BigQuery Query type connector from UI."
                )
            path = query
        elif self._query_table:
            path = self._query_table
        elif path:
            pass
        else:
            raise ValueError(
                "Either query should be provided "
                "or Query Project,Dataset and Table should be set"
            )

        return engine.get_instance().read(self, self.BIGQUERY_FORMAT, options, path)
