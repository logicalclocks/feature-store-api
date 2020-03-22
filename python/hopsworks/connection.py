import os
from requests.exceptions import ConnectionError

from hopsworks.decorators import connected, not_connected
from hopsworks import engine, client
from hopsworks.core import feature_store_api


class Connection:
    AWS_DEFAULT_REGION = "default"
    HOPSWORKS_PORT_DEFAULT = 443
    SECRETS_STORE_DEFAULT = "parameterstore"
    HOSTNAME_VERIFICATION_DEFAULT = True
    CERT_FOLDER_DEFAULT = ""

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
    ):
        self._host = host
        self._port = port or self.HOPSWORKS_PORT_DEFAULT
        self._project = project
        self._region_name = region_name or self.AWS_DEFAULT_REGION
        self._secrets_store = secrets_store or self.SECRETS_STORE_DEFAULT
        self._hostname_verification = (
            hostname_verification or self.HOSTNAME_VERIFICATION_DEFAULT
        )
        # what's the difference between trust store path and cert folder
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder or self.CERT_FOLDER_DEFAULT
        self._api_key_file = api_key_file
        self._connected = False

        self.connect()

    @classmethod
    def connection(
        cls,
        host=None,
        port=None,
        project=None,
        region_name=None,
        secrets_store=None,
        hostname_verification=None,
        trust_store_path=None,
        cert_folder=None,
        api_key_file=None,
    ):
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
        )

    @not_connected
    def connect(self):
        self._connected = True
        try:
            if client.base.Client.REST_ENDPOINT not in os.environ:
                client.init(
                    "aws",
                    self._host,
                    self._port,
                    self._project,
                    self._region_name,
                    self._secrets_store,
                    self._hostname_verification,
                    self._trust_store_path,
                    self._cert_folder,
                    self._api_key_file,
                )
                engine.init(
                    "hive",
                    self._host,
                    self._cert_folder,
                    client.get_instance()._cert_key,
                )
            else:
                client.init("hopsworks")
                engine.init("spark")
            self._feature_store_api = feature_store_api.FeatureStoreApi()
        except (TypeError, ConnectionError):
            self._connected = False
            raise
        print("Connected. Call `.close()` to terminate connection gracefully.")

    def close(self):
        client.get_instance().stop()
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
            name = client.get_instance()._project_name + "_featurestore"
        return self._feature_store_api.get(name)

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

    @api_key_file.setter
    @not_connected
    def api_key_file(self, api_key_file):
        self._api_key_file = api_key_file

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
