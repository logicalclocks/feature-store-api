from hopsworks.feature_store import FeatureStore


class FeatureStoreApi:
    def __init__(self, client):
        self._client = client

    def get(self, identifier):
        """Get feature store with specific id or name.

        :param identifier: id or name of the feature store
        :type identifier: int, str
        :return: the featurestore metadata
        :rtype: FeatureStore
        """
        path_params = ["project", self._client._project_id, "featurestores", identifier]
        return FeatureStore.from_response_json(
            self._client._send_request("GET", path_params)
        )
