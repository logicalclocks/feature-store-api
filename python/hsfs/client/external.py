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
import boto3
import base64
import json
import requests

try:
    from pyspark.sql import SparkSession
except ImportError:
    pass

from hsfs.client import base, auth, exceptions
from hsfs.client.exceptions import FeatureStoreException


class Client(base.Client):
    DEFAULT_REGION = "default"
    SECRETS_MANAGER = "secretsmanager"
    PARAMETER_STORE = "parameterstore"
    LOCAL_STORE = "local"

    def __init__(
        self,
        host,
        port,
        project,
        engine,
        region_name,
        secrets_store,
        hostname_verification,
        trust_store_path,
        cert_folder,
        api_key_file,
        api_key_value,
    ):
        """Initializes a client in an external environment such as AWS Sagemaker."""
        if not host:
            raise exceptions.ExternalClientError("host")
        if not project:
            raise exceptions.ExternalClientError("project")

        self._host = host
        self._port = port
        self._base_url = "https://" + self._host + ":" + str(self._port)
        self._project_name = project
        self._region_name = region_name or self.DEFAULT_REGION

        if api_key_value is not None:
            api_key = api_key_value
        else:
            api_key = self._get_secret(secrets_store, "api-key", api_key_file)
        self._auth = auth.ApiKeyAuth(api_key)

        self._session = requests.session()
        self._connected = True
        self._verify = self._get_verify(self._host, trust_store_path)

        project_info = self._get_project_info(self._project_name)
        self._project_id = str(project_info["projectId"])

        self._cert_key = None
        self._cert_folder_base = None

        if engine == "hive":
            # On external Spark clients (Databricks, Spark Cluster),
            # certificates need to be provided before the Spark application starts.
            self._cert_folder_base = cert_folder
            self._cert_folder = os.path.join(cert_folder, host, project)
            self._trust_store_path = os.path.join(self._cert_folder, "trustStore.jks")
            self._key_store_path = os.path.join(self._cert_folder, "keyStore.jks")

            os.makedirs(self._cert_folder, exist_ok=True)
            credentials = self._get_credentials(self._project_id)
            self._write_b64_cert_to_bytes(
                str(credentials["kStore"]),
                path=self._get_jks_key_store_path(),
            )
            self._write_b64_cert_to_bytes(
                str(credentials["tStore"]),
                path=self._get_jks_trust_store_path(),
            )

            self._cert_key = str(credentials["password"])
            with open(os.path.join(self._cert_folder, "material_passwd"), "w") as f:
                f.write(str(credentials["password"]))

        elif engine == "spark":
            _spark_session = SparkSession.builder.getOrCreate()

            self.validate_spark_configuration(_spark_session)
            with open(
                _spark_session.conf.get("spark.hadoop.hops.ssl.keystores.passwd.name"),
                "r",
            ) as f:
                self._cert_key = f.read()

            self._trust_store_path = _spark_session.conf.get(
                "spark.hadoop.hops.ssl.trustore.name"
            )
            self._key_store_path = _spark_session.conf.get(
                "spark.hadoop.hops.ssl.keystore.name"
            )

    def validate_spark_configuration(self, _spark_session):
        exception_text = "Spark is misconfigured for communication with Hopsworks, missing or invalid property: "

        configuration_dict = {
            "spark.hadoop.hops.ssl.trustore.name": None,
            "spark.hadoop.hops.rpc.socket.factory.class.default": "io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.hadoop.hops.ssl.hostname.verifier": "ALLOW_ALL",
            "spark.hadoop.hops.ssl.keystore.name": None,
            "spark.hadoop.fs.hopsfs.impl": "io.hops.hopsfs.client.HopsFileSystem",
            "spark.hadoop.hops.ssl.keystores.passwd.name": None,
            "spark.hadoop.hops.ipc.server.ssl.enabled": "true",
            "spark.sql.hive.metastore.jars": None,
            "spark.hadoop.client.rpc.ssl.enabled.protocol": "TLSv1.2",
            "spark.hadoop.hive.metastore.uris": None,
        }

        for key, value in configuration_dict.items():
            if not (
                _spark_session.conf.get(key, "not_found") != "not_found"
                and (value is None or _spark_session.conf.get(key, None) == value)
            ):
                raise FeatureStoreException(exception_text + key)

    def _close(self):
        """Closes a client and deletes certificates."""
        if self._cert_folder_base is None:
            # On external Spark clients (Databricks, Spark Cluster),
            # certificates need to be provided before the Spark application starts.
            return

        # Clean up only on AWS
        self._cleanup_file(self._get_jks_key_store_path())
        self._cleanup_file(self._get_jks_trust_store_path())
        self._cleanup_file(os.path.join(self._cert_folder, "material_passwd"))

        try:
            # delete project level
            os.rmdir(self._cert_folder)
            # delete host level
            os.rmdir(os.path.dirname(self._cert_folder))
            # on AWS base dir will be empty, and can be deleted otherwise raises OSError
            os.rmdir(self._cert_folder_base)
        except OSError:
            pass
        self._connected = False

    def _get_jks_trust_store_path(self):
        return self._trust_store_path

    def _get_jks_key_store_path(self):
        return self._key_store_path

    def _get_secret(self, secrets_store, secret_key=None, api_key_file=None):
        """Returns secret value from the AWS Secrets Manager or Parameter Store.

        :param secrets_store: the underlying secrets storage to be used, e.g. `secretsmanager` or `parameterstore`
        :type secrets_store: str
        :param secret_key: key for the secret value, e.g. `api-key`, `cert-key`, `trust-store`, `key-store`, defaults to None
        :type secret_key: str, optional
        :param api_key_file: path to a file containing an api key, defaults to None
        :type api_key_file: str optional
        :raises ExternalClientError: `api_key_file` needs to be set for local mode
        :raises UnkownSecretStorageError: Provided secrets storage not supported
        :return: secret
        :rtype: str
        """
        if secrets_store == self.SECRETS_MANAGER:
            return self._query_secrets_manager(secret_key)
        elif secrets_store == self.PARAMETER_STORE:
            return self._query_parameter_store(secret_key)
        elif secrets_store == self.LOCAL_STORE:
            if not api_key_file:
                raise exceptions.ExternalClientError(
                    "api_key_file needs to be set for local mode"
                )
            with open(api_key_file) as f:
                return f.readline().strip()
        else:
            raise exceptions.UnknownSecretStorageError(
                "Secrets storage " + secrets_store + " is not supported."
            )

    def _query_secrets_manager(self, secret_key):
        secret_name = "hopsworks/role/" + self._assumed_role()
        args = {"service_name": "secretsmanager"}
        region_name = self._get_region()
        if region_name:
            args["region_name"] = region_name
        client = boto3.client(**args)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return json.loads(get_secret_value_response["SecretString"])[secret_key]

    def _assumed_role(self):
        client = boto3.client("sts")
        response = client.get_caller_identity()
        # arns for assumed roles in SageMaker follow the following schema
        # arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
        local_identifier = response["Arn"].split(":")[-1].split("/")
        if len(local_identifier) != 3 or local_identifier[0] != "assumed-role":
            raise Exception(
                "Failed to extract assumed role from arn: " + response["Arn"]
            )
        return local_identifier[1]

    def _get_region(self):
        if self._region_name != self.DEFAULT_REGION:
            return self._region_name
        else:
            return None

    def _query_parameter_store(self, secret_key):
        args = {"service_name": "ssm"}
        region_name = self._get_region()
        if region_name:
            args["region_name"] = region_name
        client = boto3.client(**args)
        name = "/hopsworks/role/" + self._assumed_role() + "/type/" + secret_key
        return client.get_parameter(Name=name, WithDecryption=True)["Parameter"][
            "Value"
        ]

    def _get_project_info(self, project_name):
        """Makes a REST call to hopsworks to get all metadata of a project for the provided project.

        :param project_name: the name of the project
        :type project_name: str
        :return: JSON response with project info
        :rtype: dict
        """
        return self._send_request("GET", ["project", "getProjectInfo", project_name])

    def _get_credentials(self, project_id):
        """Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive

        :param project_id: id of the project
        :type project_id: int
        :return: JSON response with credentials
        :rtype: dict
        """
        return self._send_request("GET", ["project", project_id, "credentials"])

    def _write_b64_cert_to_bytes(self, b64_string, path):
        """Converts b64 encoded certificate to bytes file .

        :param b64_string:  b64 encoded string of certificate
        :type b64_string: str
        :param path: path where file is saved, including file name. e.g. /path/key-store.jks
        :type path: str
        """

        with open(path, "wb") as f:
            cert_b64 = base64.b64decode(b64_string)
            f.write(cert_b64)

    def _cleanup_file(self, file_path):
        """Removes local files with `file_path`."""
        try:
            os.remove(file_path)
        except OSError:
            pass

    @property
    def host(self):
        return self._host
