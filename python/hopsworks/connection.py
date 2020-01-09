from hopsworks.core import client


class Connection:
    def __init__(self, hostname=None, project=None, secrets_store=None):
        self._hostname = hostname
        self._project = project
        self._secrets_store = secrets_store
        self._connected = False
        self._client = None

    @property
    def hostname(self):
        print("prop")
        return self._hostname

    @hostname.setter
    def hostname(self, hostname):
        if self._connected:
            raise Exception(
                "Connection is currently in use. Close before modifying it."
            )
        self._hostname = hostname

    @property
    def project(self):
        return self._project

    @project.setter
    def project(self, project):
        if self._connected:
            raise Exception(
                "Connection is currently in use. Close before modifying it."
            )
        self._project = project

    @property
    def secrets_store(self):
        return self._secrets_store

    @secrets_store.setter
    def secrets_store(self, secrets_store):
        if self._connected:
            raise Exception(
                "Connection is currently in use. Close before modifying it."
            )
        self._secrets_store = secrets_store

    def connect(self):
        self._connected = True
        self._client = client.Client()
        print("connected")

    def close(self):
        self._client = None
        self._connected = False
        print("closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
