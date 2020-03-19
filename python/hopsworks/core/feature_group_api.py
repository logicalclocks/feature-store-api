from hopsworks import client
from hopsworks import feature_group


class FeatureGroupApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def get(self, name, version):
        """Get feature store with specific id or name.

        :param identifier: id or name of the feature store
        :type identifier: int, str
        :return: the featurestore metadata
        :rtype: FeatureStore
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            name,
        ]
        query_params = {"version": version}
        return feature_group.FeatureGroup.from_response_json(
            _client._send_request("GET", path_params, query_params)[0],
        )
