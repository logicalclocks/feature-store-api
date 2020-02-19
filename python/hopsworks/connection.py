import os
from requests.exceptions import ConnectionError

from hopsworks import util, engine
from hopsworks.core import client, feature_store_api


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
        self._client = None

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

    def connect(self):
        self._connected = True
        try:
            if client.BaseClient.REST_ENDPOINT not in os.environ:
                self._client = client.ExternalClient(
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
                    "hive", self._host, self._cert_folder, self._client._cert_key
                )
            else:
                self._client = client.HopsworksClient()
                engine.init("spark")
            self._feature_store_api = feature_store_api.FeatureStoreApi(self._client)
        except (TypeError, ConnectionError):
            self._connected = False
            raise
        print("CONNECTED")

    def close(self):
        self._client._close()
        self._feature_store_api = None
        engine.stop()
        self._connected = False
        print("CONNECTION CLOSED")

    @util.connected
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
            name = self._client._project_name + "_featurestore"
        # TODO: this won't work with multiple feature stores
        engine.get_instance()._feature_store = name
        return self._feature_store_api.get(name)

    @property
    def host(self):
        return self._host

    @host.setter
    @util.not_connected
    def host(self, host):
        self._host = host

    @property
    def port(self):
        return self._port

    @port.setter
    @util.not_connected
    def port(self, port):
        self._port = port

    @property
    def project(self):
        return self._project

    @project.setter
    @util.not_connected
    def project(self, project):
        self._project = project

    @property
    def region_name(self):
        return self._region_name

    @region_name.setter
    @util.not_connected
    def region_name(self, region_name):
        self._region_name = region_name

    @property
    def secrets_store(self):
        return self._secrets_store

    @secrets_store.setter
    @util.not_connected
    def secrets_store(self, secrets_store):
        self._secrets_store = secrets_store

    @property
    def hostname_verification(self):
        return self._hostname_verification

    @hostname_verification.setter
    @util.not_connected
    def hostname_verification(self, hostname_verification):
        self._hostname_verification = hostname_verification

    @property
    def trust_store_path(self):
        return self._trust_store_path

    @trust_store_path.setter
    @util.not_connected
    def trust_store_path(self, trust_store_path):
        self._trust_store_path = trust_store_path

    @property
    def cert_folder(self):
        return self._cert_folder

    @cert_folder.setter
    @util.not_connected
    def cert_folder(self, cert_folder):
        self._cert_folder = cert_folder

    @property
    def api_key_file(self):
        return self._api_key_file

    @api_key_file.setter
    @util.not_connected
    def api_key_file(self, api_key_file):
        self._api_key_file = api_key_file

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class HopsworksConnectionError(Exception):
    """Thrown when attempted to change connection attributes while connected."""

    def __init__(self):
        super().__init__(
            "Connection is currently in use. Needs to be closed for modification."
        )


class NoHopsworksConnectionError(Exception):
    """Thrown when attempted to perform operation on connection while not connected."""

    def __init__(self):
        super().__init__(
            "Connection is not active. Needs to be connected for feature store operations."
        )
