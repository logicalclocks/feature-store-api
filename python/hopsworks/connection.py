import os

from hopsworks.core import client


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
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder or self.CERT_FOLDER_DEFAULT
        self._api_key_file = api_key_file
        self._connected = False
        self._client = None

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
            else:
                self._client = client.HopsworksClient()
        except TypeError:
            self._connected = False
            raise
        print("CONNECTED")

    def close(self):
        self._client._close()
        self._client = None
        # clean up certificates
        self._connected = False
        print("CONNECTION CLOSED")

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, host):
        if self._connected:
            raise ConnectionError
        self._host = host

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port):
        if self._connected:
            raise ConnectionError
        self._port = port

    @property
    def project(self):
        return self._project

    @project.setter
    def project(self, project):
        if self._connected:
            raise ConnectionError
        self._project = project

    @property
    def region_name(self):
        return self._region_name

    @region_name.setter
    def region_name(self, region_name):
        if self._connected:
            raise ConnectionError
        self._region_name = region_name

    @property
    def secrets_store(self):
        return self._secrets_store

    @secrets_store.setter
    def secrets_store(self, secrets_store):
        if self._connected:
            raise ConnectionError
        self._secrets_store = secrets_store

    @property
    def hostname_verification(self):
        return self._hostname_verification

    @hostname_verification.setter
    def hostname_verification(self, hostname_verification):
        if self._connected:
            raise ConnectionError
        self._hostname_verification = hostname_verification

    @property
    def trust_store_path(self):
        return self._trust_store_path

    @trust_store_path.setter
    def trust_store_path(self, trust_store_path):
        if self._connected:
            raise ConnectionError
        self._trust_store_path = trust_store_path

    @property
    def cert_folder(self):
        return self._cert_folder

    @cert_folder.setter
    def cert_folder(self, cert_folder):
        if self._connected:
            raise ConnectionError
        self._cert_folder = cert_folder

    @property
    def api_key_file(self):
        return self._api_key_file

    @api_key_file.setter
    def api_key_file(self, api_key_file):
        if self._connected:
            raise ConnectionError
        self._api_key_file = api_key_file

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class ConnectionError(Exception):
    """Connection Error

    Thrown when attempted to change connection attributes while connected.
    """

    def __init__(self):
        super().__init__(
            "Connection is currently in use. Needs to be closed for modification."
        )
