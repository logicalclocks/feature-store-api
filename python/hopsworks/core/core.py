from hopsworks.core import client


class Core:
    def __init__(self):
        self._client = client.Client()

    def feature_group(self, feature_store, name, version):
        self._client.get_feature_group(feature_store.id, name, version)
