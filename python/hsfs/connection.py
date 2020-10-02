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
from requests.exceptions import ConnectionError

from hsfs.decorators import connected, not_connected
from hsfs import engine, client
from hsfs.core import feature_store_api, project_api, hosts_api, services_api


class Connection:
    AWS_DEFAULT_REGION = "default"
    HOPSWORKS_PORT_DEFAULT = 443
    SECRETS_STORE_DEFAULT = "parameterstore"
    HOSTNAME_VERIFICATION_DEFAULT = True
    CERT_FOLDER_DEFAULT = "hops"

    def __init__(
        self,
        host=None,
        port=None,
        project=None,
        region_name=None,
        secrets_store=None,
        hostname_verification=None,
        trust_store_path=None,
        cert_folder=None,
        api_key_file=None,
        api_key_value=None,
    ):
        self._host = host
        self._port = port or self.HOPSWORKS_PORT_DEFAULT
        self._project = project
        self._region_name = region_name or self.AWS_DEFAULT_REGION
        self._secrets_store = secrets_store or self.SECRETS_STORE_DEFAULT
        self._hostname_verification = (
            hostname_verification or self.HOSTNAME_VERIFICATION_DEFAULT
        )
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder or self.CERT_FOLDER_DEFAULT
        self._api_key_file = api_key_file
        self._api_key_value = api_key_value
        self._connected = False

        self.connect()

    @classmethod
    def connection(
        cls,
        host: str = None,
        port: int = None,
        project: str = None,
        region_name: str = None,
        secrets_store: str = None,
        hostname_verification: bool = None,
        trust_store_path: str = None,
        cert_folder: str = None,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        """Instantiate a connection to a feature store.

        This feature store can be located on the same cluster if you are
        running it from within Hopsworks, or an external cluster when running
        on Databricks or Sagemaker.

        # Arguments
            host: str, optional.
                The hostname of the Hopsworks instance, defaults to `None`.
            port: int, optional.
                The port on which the Hopsworks instance can be reached, defaults to `None`.

        # Returns
            Connection. A connection reference to retrieve and perform operations on a
                Hopsworks project.
        """
        return cls(
            host,
            port,
            project,
            region_name,
            secrets_store,
            hostname_verification,
            trust_store_path,
            cert_folder,
            api_key_file,
            api_key_value,
        )

    @classmethod
    def setup_databricks(
        cls,
        host,
        project,
        port=443,
        region_name="default",
        secrets_store="parameterstore",
        cert_folder="hops",
        hostname_verification=True,
        trust_store_path=None,
        api_key_file=None,
        api_key_value=None,
    ):
        connection = cls(
            host,
            port,
            project,
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

    @not_connected
    def connect(self):
        self._connected = True
        try:
            if client.base.Client.REST_ENDPOINT not in os.environ:
                if os.path.exists("/dbfs/"):
                    # databricks
                    client.init(
                        "external",
                        self._host,
                        self._port,
                        self._project,
                        self._region_name,
                        self._secrets_store,
                        self._hostname_verification,
                        os.path.join("/dbfs", self._trust_store_path)
                        if self._trust_store_path is not None
                        else None,
                        os.path.join("/dbfs", self._cert_folder),
                        os.path.join("/dbfs", self._api_key_file)
                        if self._api_key_file is not None
                        else None,
                        self._api_key_value,
                    )
                    engine.init("spark")
                else:
                    # aws
                    client.init(
                        "external",
                        self._host,
                        self._port,
                        self._project,
                        self._region_name,
                        self._secrets_store,
                        self._hostname_verification,
                        self._trust_store_path,
                        self._cert_folder,
                        self._api_key_file,
                        self._api_key_value,
                    )
                    engine.init(
                        "hive",
                        self._host,
                        self._cert_folder,
                        self._project,
                        client.get_instance()._cert_key,
                    )
            else:
                client.init("hopsworks")
                engine.init("spark")
            self._feature_store_api = feature_store_api.FeatureStoreApi()
            self._project_api = project_api.ProjectApi()
            self._hosts_api = hosts_api.HostsApi()
            self._services_api = services_api.ServicesApi()
        except (TypeError, ConnectionError):
            self._connected = False
            raise
        print("Connected. Call `.close()` to terminate connection gracefully.")

    def close(self):
        client.stop()
        self._feature_store_api = None
        engine.stop()
        self._connected = False
        print("Connection closed.")

    @connected
    def get_feature_store(self, name=None):
        """Get a reference to a feature store, to perform operations on.

        Defaulting to the project's default feature store. Shared feature stores can be
        retrieved by passing the `name`.

        :param name: the name of the feature store, defaults to None
        :type name: str, optional
        :return: feature store object
        :rtype: FeatureStore
        """
        if not name:
            name = client.get_instance()._project_name.lower() + "_featurestore"
        return self._feature_store_api.get(name)

    def _get_clients(self, dbfs_folder):
        """
        Get the client libraries and save them in the dbfs folder.

        :param dbfs_folder: the folder in which to save the libraries
        :type dbfs_folder: str
        """
        client_path = os.path.join(dbfs_folder, "client.tar.gz")
        if not os.path.exists(client_path):
            client_libs = self._project_api.get_client()
            with open(client_path, "wb") as f:
                for chunk in client_libs:
                    f.write(chunk)

    def _get_hivemetastore_hostname(self):
        """
        Get the internal hostname of the Hopsworks instance.
        """
        hosts = self._hosts_api.get()
        hivemetastore = self._services_api.get_service("hivemetastore")
        hosts = [host for host in hosts if host["id"] == hivemetastore["hostId"]]
        return hosts[0]["hostname"]

    def _write_init_script(self, dbfs_folder):
        """
        Write the init script for databricks clusters to dbfs.

        :param dbfs_folder: the folder on dbfs in which to save the script
        :type dbfs_foler: str
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

    def _print_instructions(self, user_cert_folder, cert_folder, internal_host):
        """
        Print the instructions to set up the hopsfs hive connection on databricks.

        :param user_cert_folder: the original user specified cert_folder without `/dbfs/` prefix
        :type user_cert_folder: str
        :cert_folder: the directory in which the credential were saved, prefixed with `/dbfs/` and `[hostname]`
        :type cert_folder: str
        :param internal_ip: the internal ip of the hopsworks instance
        :type internal_ip: str
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
