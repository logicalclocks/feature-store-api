from hopsworks import client
from hopsworks.feature_store import FeatureStore


class FeatureStoreApi:
    def __init__(self):
        pass

    def get(self, identifier):
        """Get feature store with specific id or name.

        :param identifier: id or name of the feature store
        :type identifier: int, str
        :return: the featurestore metadata
        :rtype: FeatureStore
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "featurestores", identifier]
        return FeatureStore.from_response_json(
            _client._send_request("GET", path_params)
        )
