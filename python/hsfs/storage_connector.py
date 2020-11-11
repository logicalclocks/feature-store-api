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
    HOPSFS_DTO = "featurestoreHopsfsConnectorDTO"
    JDBC_DTO = "featurestoreJdbcConnectorDTO"
    S3_DTO = "featurestoreS3ConnectorDTO"

    def __init__(
        self,
        id,
        name,
        description,
        featurestore_id,
        storage_connector_type,
        # members specific to type of connector
        hopsfs_path=None,
        dataset_name=None,
        access_key=None,
        secret_key=None,
        server_encryption_algorithm=None,
        server_encryption_key=None,
        bucket=None,
        connection_string=None,
        arguments=None,
    ):
        self._id = id
        self._name = name
        self._description = description
        self._feature_store_id = featurestore_id
        self._storage_connector_type = storage_connector_type
        self._hopsfs_path = hopsfs_path
        self._dataset_name = dataset_name
        self._access_key = access_key
        self._secret_key = secret_key
        self._server_encryption_algorithm = server_encryption_algorithm
        self._server_encryption_key = server_encryption_key
        self._bucket = bucket
        self._connection_string = connection_string
        self._arguments = arguments

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
        """Type of the connector. S3, JDBC or HOPSFS."""
        return self._storage_connector_type

    @property
    def access_key(self):
        """Access key for S3 buckets."""
        return self._access_key

    @property
    def secret_key(self):
        """Secret key for S3 buckets."""
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
    def connection_string(self):
        """JDBC connection string."""
        return self._connection_string

    @property
    def arguments(self):
        """Additional JDBC arguments."""
        return self._arguments

    def spark_options(self):
        """Return prepared options to be passed to Spark, based on the additional
        arguments.
        """
        args = [arg.split("=") for arg in self._arguments.split(",")]

        return {
            "url": self._connection_string,
            "user": [arg[1] for arg in args if arg[0] == "user"][0],
            "password": [arg[1] for arg in args if arg[0] == "password"][0],
        }
