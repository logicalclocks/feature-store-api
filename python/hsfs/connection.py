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
import importlib.util

from requests.exceptions import ConnectionError

from hsfs.decorators import connected, not_connected
from hsfs import engine, client, util
from hsfs.core import feature_store_api, project_api, hosts_api, services_api

AWS_DEFAULT_REGION = "default"
HOPSWORKS_PORT_DEFAULT = 443
SECRETS_STORE_DEFAULT = "parameterstore"
HOSTNAME_VERIFICATION_DEFAULT = True
CERT_FOLDER_DEFAULT = "/tmp"


class Connection:
    """A feature store connection object.

    The connection is project specific, so you can access the project's own feature
    store but also any feature store which has been shared with the project you connect
    to.

    This class provides convenience classmethods accessible from the `hsfs`-module:

    !!! example "Connection factory"
        For convenience, `hsfs` provides a factory method, accessible from the top level
        module, so you don't have to import the `Connection` class manually:

        ```python
        import hsfs
        conn = hsfs.connection()
        ```

    !!! hint "Save API Key as File"
        To get started quickly, without saving the Hopsworks API in a secret storage,
        you can simply create a file with the previously created Hopsworks API Key and
        place it on the environment from which you wish to connect to the Hopsworks
        Feature Store.

        You can then connect by simply passing the path to the key file when
        instantiating a connection:

        ```python hl_lines="6"
            import hsfs
            conn = hsfs.connection(
                'my_instance',                      # DNS of your Feature Store instance
                443,                                # Port to reach your Hopsworks instance, defaults to 443
                'my_project',                       # Name of your Hopsworks Feature Store project
                api_key_file='featurestore.key',    # The file containing the API key generated above
                hostname_verification=True)         # Disable for self-signed certificates
            )
            fs = conn.get_feature_store()           # Get the project's default feature store
        ```

    Clients in external clusters need to connect to the Hopsworks Feature Store using an
    API key. The API key is generated inside the Hopsworks platform, and requires at
    least the "project" and "featurestore" scopes to be able to access a feature store.
    For more information, see the [integration guides](../setup.md).

    # Arguments
        host: The hostname of the Hopsworks instance, defaults to `None`.
        port: The port on which the Hopsworks instance can be reached,
            defaults to `443`.
        project: The name of the project to connect to. When running on Hopsworks, this
            defaults to the project from where the client is run from.
            Defaults to `None`.
        engine: Which engine to use, `"spark"`, `"python"` or `"training"`. Defaults to `None`,
            which initializes the engine to Spark if the environment provides Spark, for
            example on Hopsworks and Databricks, or falls back on Hive in Python if Spark is not
            available, e.g. on local Python environments or AWS SageMaker. This option
            allows you to override this behaviour. `"training"` engine is useful when only
            feature store metadata is needed, for example training dataset location and label
            information when Hopsworks training experiment is conducted.
        region_name: The name of the AWS region in which the required secrets are
            stored, defaults to `"default"`.
        secrets_store: The secrets storage to be used, either `"secretsmanager"`,
            `"parameterstore"` or `"local"`, defaults to `"parameterstore"`.
        hostname_verification: Whether or not to verify Hopsworks’ certificate, defaults
            to `True`.
        trust_store_path: Path on the file system containing the Hopsworks certificates,
            defaults to `None`.
        cert_folder: The directory to store retrieved HopsFS certificates, defaults to
            `"/tmp"`. Only required when running without a Spark environment.
        api_key_file: Path to a file containing the API Key, if provided,
            `secrets_store` will be ignored, defaults to `None`.
        api_key_value: API Key as string, if provided, `secrets_store` will be ignored`,
            however, this should be used with care, especially if the used notebook or
            job script is accessible by multiple parties. Defaults to `None`.

    # Returns
        `Connection`. Feature Store connection handle to perform operations on a
            Hopsworks project.
    """

    def __init__(
        self,
        host: str = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str = None,
        engine: str = None,
        region_name: str = AWS_DEFAULT_REGION,
        secrets_store: str = SECRETS_STORE_DEFAULT,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        self._host = host
        self._port = port
        self._project = project
        self._engine = engine
        self._region_name = region_name
        self._secrets_store = secrets_store
        self._hostname_verification = hostname_verification
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder
        self._api_key_file = api_key_file
        self._api_key_value = api_key_value
        self._connected = False

        self.connect()

    @connected
    def get_feature_store(self, name: str = None):
        """Get a reference to a feature store to perform operations on.

        Defaulting to the project name of default feature store. To get a
        Shared feature stores, the project name of the feature store is required.

        # Arguments
            name: The name of the feature store, defaults to `None`.

        # Returns
            `FeatureStore`. A feature store handle object to perform operations on.
        """
        if not name:
            name = client.get_instance()._project_name
        return self._feature_store_api.get(util.rewrite_feature_store_name(name))

    @not_connected
    def connect(self):
        """Instantiate the connection.

        Creating a `Connection` object implicitly calls this method for you to
        instantiate the connection. However, it is possible to close the connection
        gracefully with the `close()` method, in order to clean up materialized
        certificates. This might be desired when working on external environments such
        as AWS SageMaker. Subsequently you can call `connect()` again to reopen the
        connection.

        !!! example
            ```python
            import hsfs
            conn = hsfs.connection()
            conn.close()
            conn.connect()
            ```
        """
        self._connected = True
        try:
            # determine engine, needed to init client
            if (self._engine is not None and self._engine.lower() == "spark") or (
                self._engine is None and importlib.util.find_spec("pyspark")
            ):
                self._engine = "spark"
            elif (
                self._engine is not None and self._engine.lower() in ["hive", "python"]
            ) or (self._engine is None and not importlib.util.find_spec("pyspark")):
                self._engine = "python"
            elif self._engine is not None and self._engine.lower() == "training":
                self._engine = "training"
            else:
                raise ConnectionError(
                    "Engine you are trying to initialize is unknown. "
                    "Supported engines are `'spark'`, `'python'` and `'training'`."
                )

            # init client
            if client.base.Client.REST_ENDPOINT not in os.environ:
                client.init(
                    "external",
                    self._host,
                    self._port,
                    self._project,
                    self._engine,
                    self._region_name,
                    self._secrets_store,
                    self._hostname_verification,
                    self._trust_store_path,
                    self._cert_folder,
                    self._api_key_file,
                    self._api_key_value,
                )
            else:
                client.init("hopsworks")

            # init engine
            engine.init(self._engine)

            self._feature_store_api = feature_store_api.FeatureStoreApi()
            self._project_api = project_api.ProjectApi()
            self._hosts_api = hosts_api.HostsApi()
            self._services_api = services_api.ServicesApi()
        except (TypeError, ConnectionError):
            self._connected = False
            raise
        print("Connected. Call `.close()` to terminate connection gracefully.")

    def close(self):
        """Close a connection gracefully.

        This will clean up any materialized certificates on the local file system of
        external environments such as AWS SageMaker.

        Usage is recommended but optional.
        """
        client.stop()
        self._feature_store_api = None
        engine.stop()
        self._connected = False
        print("Connection closed.")

    @classmethod
    def setup_databricks(
        cls,
        host: str = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str = None,
        engine: str = None,
        region_name: str = AWS_DEFAULT_REGION,
        secrets_store: str = SECRETS_STORE_DEFAULT,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        """Set up the HopsFS and Hive connector on a Databricks cluster.

        This method will setup the HopsFS and Hive connectors to connect from a
        Databricks cluster to a Hopsworks Feature Store instance. It returns a
        `Connection` object and will print instructions on how to finalize the setup
        of the Databricks cluster.
        See also the [Databricks integration guide](../integrations/databricks/configuration.md).
        """
        connection = cls(
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
        )

        dbfs_folder = client.get_instance()._cert_folder_base

        os.makedirs(os.path.join(dbfs_folder, "scripts"), exist_ok=True)
        connection._get_clients(dbfs_folder)
        hive_host = connection._get_hivemetastore_hostname()
        connection._write_init_script(dbfs_folder)
        connection._print_instructions(
            cert_folder, client.get_instance()._cert_folder, hive_host
        )

        return connection

    @classmethod
    def connection(
        cls,
        host: str = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str = None,
        engine: str = None,
        region_name: str = AWS_DEFAULT_REGION,
        secrets_store: str = SECRETS_STORE_DEFAULT,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        """Connection factory method, accessible through `hsfs.connection()`."""
        return cls(
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
        )

    def _get_clients(self, dbfs_folder: str):
        """Get the client libraries and save them in the dbfs folder.

        # Arguments
            dbfs_folder: The directory in which to save the client libraries.
        """
        client_path = os.path.join(dbfs_folder, "client.tar.gz")
        if not os.path.exists(client_path):
            client_libs = self._project_api.get_client()
            with open(client_path, "wb") as f:
                for chunk in client_libs:
                    f.write(chunk)

    def _get_hivemetastore_hostname(self):
        """Get the internal hostname of the Hopsworks instance."""
        hosts = self._hosts_api.get()
        hivemetastore = self._services_api.get_service("hivemetastore")
        hosts = [host for host in hosts if host["id"] == hivemetastore["hostId"]]
        return hosts[0]["hostname"]

    def _write_init_script(self, dbfs_folder: str):
        """Write the init script for databricks clusters to dbfs.

        # Arguments
            dbfs_folder: The directory in which to save the client libraries.
        """
        initScript = """
            #!/bin/sh

            tar -xvf PATH/client.tar.gz -C /tmp
            tar -xvf /tmp/client/apache-hive-*-bin.tar.gz -C /tmp
            mv /tmp/apache-hive-*-bin /tmp/apache-hive-bin
            chmod -R +xr /tmp/apache-hive-bin
            cp /tmp/client/hopsfs-client*.jar /databricks/jars/
        """
        script_path = os.path.join(dbfs_folder, "scripts/initScript.sh")
        if not os.path.exists(script_path):
            initScript = initScript.replace("PATH", dbfs_folder)
            with open(script_path, "w") as f:
                f.write(initScript)

    def _print_instructions(
        self, user_cert_folder: str, cert_folder: str, internal_host: str
    ):
        """Print the instructions to set up the HopsFS Hive connection on Databricks.

        # Arguments
            user_cert_folder: The original user specified cert_folder without `/dbfs/`
                prefix.
            cert_folder: The directory in which the credential were saved, prefixed with
                `/dbfs/` and `[hostname]`.
            internal_host: The internal ip of the hopsworks instance.
        """

        instructions = """
        In the advanced options of your databricks cluster configuration
        add the following path to Init Scripts: dbfs:/{0}/scripts/initScript.sh

        add the following to the Spark Config:

        spark.hadoop.fs.hopsfs.impl io.hops.hopsfs.client.HopsFileSystem
        spark.hadoop.hops.ipc.server.ssl.enabled true
        spark.hadoop.hops.ssl.hostname.verifier ALLOW_ALL
        spark.hadoop.hops.rpc.socket.factory.class.default io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory
        spark.hadoop.client.rpc.ssl.enabled.protocol TLSv1.2
        spark.hadoop.hops.ssl.keystores.passwd.name {1}/material_passwd
        spark.hadoop.hops.ssl.keystore.name {1}/keyStore.jks
        spark.hadoop.hops.ssl.trustore.name {1}/trustStore.jks
        spark.sql.hive.metastore.jars /tmp/apache-hive-bin/lib/*
        spark.hadoop.hive.metastore.uris thrift://{2}:9083

        Then save and restart the cluster.
        """.format(
            user_cert_folder, cert_folder, internal_host
        )

        print(instructions)

    @property
    def host(self):
        return self._host

    @host.setter
    @not_connected
    def host(self, host):
        self._host = host

    @property
    def port(self):
        return self._port

    @port.setter
    @not_connected
    def port(self, port):
        self._port = port

    @property
    def project(self):
        return self._project

    @project.setter
    @not_connected
    def project(self, project):
        self._project = project

    @property
    def region_name(self):
        return self._region_name

    @region_name.setter
    @not_connected
    def region_name(self, region_name):
        self._region_name = region_name

    @property
    def secrets_store(self):
        return self._secrets_store

    @secrets_store.setter
    @not_connected
    def secrets_store(self, secrets_store):
        self._secrets_store = secrets_store

    @property
    def hostname_verification(self):
        return self._hostname_verification

    @hostname_verification.setter
    @not_connected
    def hostname_verification(self, hostname_verification):
        self._hostname_verification = hostname_verification

    @property
    def trust_store_path(self):
        return self._trust_store_path

    @trust_store_path.setter
    @not_connected
    def trust_store_path(self, trust_store_path):
        self._trust_store_path = trust_store_path

    @property
    def cert_folder(self):
        return self._cert_folder

    @cert_folder.setter
    @not_connected
    def cert_folder(self, cert_folder):
        self._cert_folder = cert_folder

    @property
    def api_key_file(self):
        return self._api_key_file

    @property
    def api_key_value(self):
        return self._api_key_value

    @api_key_file.setter
    @not_connected
    def api_key_file(self, api_key_file):
        self._api_key_file = api_key_file

    @api_key_value.setter
    @not_connected
    def api_key_value(self, api_key_value):
        self._api_key_value = api_key_value

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
