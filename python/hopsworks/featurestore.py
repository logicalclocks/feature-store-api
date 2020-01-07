from hopsworks import core


class FeatureStore:
    def __init__(self, name):
        self._name = name
        # self._id = # Find ID
        self._core = core.Core()

    def feature_group(self, name, version):
        return self._core.feature_group(self, name, version)
