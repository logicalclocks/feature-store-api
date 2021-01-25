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

import humps


class StorageConnector:
    HOPSFS = "HOPSFS"
    S3 = "S3"
    JDBC = "JDBC"
    REDSHIFT = "REDSHIFT"
    ADLS = "ADLS"
    HOPSFS_DTO = "featurestoreHopsfsConnectorDTO"
    JDBC_DTO = "featurestoreJdbcConnectorDTO"
    S3_DTO = "featurestoreS3ConnectorDTO"
    REDSHIFT_DTO = "featurestoreRedshiftConnectorDTO"
    ADLS_DTO = "featurestoreADLSConnectorDTO"

    def __init__(
        self,
        id,
        name,
        featurestore_id,
        storage_connector_type,
        description=None,
        # members specific to type of connector
        hopsfs_path=None,
        dataset_name=None,
        access_key=None,
        secret_key=None,
        server_encryption_algorithm=None,
        server_encryption_key=None,
        bucket=None,
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
        session_token=None,
        iam_role=None,
        connection_string=None,
        arguments=None,
        expiration=None,
        generation=None,
        directory_id=None,
        application_id=None,
        service_credential=None,
        account_name=None,
        container_name=None,
        spark_options=None,
    ):
        self._id = id
        self._name = name
        self._description = description
        self._feature_store_id = featurestore_id
        self._storage_connector_type = storage_connector_type

        # HopsFS
        self._hopsfs_path = hopsfs_path
        self._dataset_name = dataset_name

        # S3
        self._access_key = access_key
        self._secret_key = secret_key
        self._server_encryption_algorithm = server_encryption_algorithm
        self._server_encryption_key = server_encryption_key
        self._bucket = bucket

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
        self._session_token = session_token
        self._iam_role = iam_role
        self._connection_string = connection_string
        self._arguments = arguments
        self._expiration = expiration

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

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def to_dict(self):
        # Currently we use this method only when creating on demand feature groups.
        # The backend needs only the id.
        return {"id": self._id}

    @property
    def id(self):
        """Id of the storage connector uniquely identifying it in the Feature store."""
        return self._id

    @property
    def connector_type(self):
        """Type of the connector. S3, JDBC, REDSHIFT or HOPSFS."""
        return self._storage_connector_type

    @property
    def access_key(self):
        """Access key."""
        if self._storage_connector_type.upper() == self.S3:
            return self._access_key
        else:
            raise Exception(
                "Access key is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def secret_key(self):
        """Secret key."""
        if self._storage_connector_type.upper() == self.S3:
            return self._secret_key
        else:
            raise Exception(
                "Secret key is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def server_encryption_algorithm(self):
        """Encryption algorithm if server-side S3 bucket encryption is enabled."""
        if self._storage_connector_type.upper() == self.S3:
            return self._server_encryption_algorithm
        else:
            raise Exception(
                "Encryption algorithm is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def server_encryption_key(self):
        """Encryption key if server-side S3 bucket encryption is enabled."""
        if self._storage_connector_type.upper() == self.S3:
            return self._server_encryption_key
        else:
            raise Exception(
                "Encryption key is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def cluster_identifier(self):
        """Cluster identifier for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._cluster_identifier
        else:
            raise Exception(
                "Cluster identifier is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_driver(self):
        """Database endpoint for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_driver
        else:
            raise Exception(
                "Database driver is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_endpoint(self):
        """Database endpoint for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_endpoint
        else:
            raise Exception(
                "Database endpoint is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_name(self):
        """Database name for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_name
        else:
            raise Exception(
                "Database name is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_port(self):
        """Database port for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_port
        else:
            raise Exception(
                "Database port is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def table_name(self):
        """Table name for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._table_name
        else:
            raise Exception(
                "Table name is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_user_name(self):
        """Database username for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_user_name
        else:
            raise Exception(
                "Database username is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def auto_create(self):
        """Database username for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._auto_create
        else:
            raise Exception(
                "Auto create is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_group(self):
        """Database username for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_group
        else:
            raise Exception(
                "Database group is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def database_password(self):
        """Database password for redshift cluster."""
        if self._storage_connector_type.upper() == self.REDSHIFT:
            return self._database_password
        else:
            raise Exception(
                "Database password is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def session_token(self):
        """Session token."""
        if self._storage_connector_type.upper() == self.S3:
            return self._session_token
        else:
            raise Exception(
                "Session token is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def iam_role(self):
        """IAM role."""
        return self._iam_role

    @property
    def expiration(self):
        """Cluster temporary credential expiration time."""
        if self._storage_connector_type.upper() in [self.S3, self.REDSHIFT]:
            return self._expiration
        else:
            raise Exception(
                "Expiration is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def bucket(self):
        """Return the bucket for S3 connectors."""
        if self._storage_connector_type.upper() == self.S3:
            return self._bucket
        else:
            raise Exception(
                "Bucket is not supported for connector " + self._storage_connector_type
            )

    @property
    def connection_string(self):
        """JDBC connection string."""
        if self._storage_connector_type.upper() == self.JDBC:
            return self._connection_string
        else:
            raise Exception(
                "Connection string is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def arguments(self):
        """Additional JDBC arguments."""
        if self._storage_connector_type.upper() == self.JDBC:
            return self._arguments
        else:
            raise Exception(
                "Arguments is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def path(self):
        """If the connector refers to a path (e.g. S3) - return the path of the connector"""
        if self._storage_connector_type.upper() == self.S3:
            return "s3://" + self._bucket
        else:
            raise Exception(
                "Path is not supported for connector " + self._storage_connector_type
            )

    @property
    def generation(self):
        """Generation of the ADLS storage connector"""
        if self._storage_connector_type.upper() == self.ADLS:
            self._generation
        else:
            raise Exception(
                "Generation is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def directory_id(self):
        """Directory ID of the ADLS storage connector"""
        if self._storage_connector_type.upper() == self.ADLS:
            self._directory_id
        else:
            raise Exception(
                "Directory ID is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def application_id(self):
        """Application ID of the ADLS storage connector"""
        if self._storage_connector_type.upper() == self.ADLS:
            self._application_id
        else:
            raise Exception(
                "Application ID is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def account_name(self):
        """Account name of the ADLS storage connector"""
        if self._storage_connector_type.upper() == self.ADLS:
            self._account_name
        else:
            raise Exception(
                "Account name is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def container_name(self):
        """Container name of the ADLS storage connector"""
        if self._storage_connector_type.upper() == self.ADLS:
            self._container_name
        else:
            raise Exception(
                "Container name is not supported for connector "
                + self._storage_connector_type
            )

    @property
    def service_credential(self):
        """Service credential of the ADLS storage connector"""
        if self._storage_connector_type.upper() == self.ADLS:
            self._service_credential
        else:
            raise Exception(
                "Service Credential is not supported for connector "
                + self._storage_connector_type
            )

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        if self._storage_connector_type.upper() == self.JDBC:
            args = [arg.split("=") for arg in self._arguments.split(",")]

            options = {a[0]: a[1] for a in args}
            options["url"] = self._connection_string

            return options
        elif self._storage_connector_type.upper() == self.REDSHIFT:
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
            if self._arguments is not None:
                connstr = connstr + "?" + self._arguments
            props = {
                "url": connstr,
                "driver": self._database_driver,
                "user": self._database_user_name,
                "password": self._database_password,
            }
            if self._table_name is not None:
                props["dbtable"] = self._table_name
            return props
        elif self._storage_connector_type.upper() == self.ADLS:
            return self._spark_options
        else:
            raise Exception(
                "Spark options are not supported for connector "
                + self._storage_connector_type
            )
