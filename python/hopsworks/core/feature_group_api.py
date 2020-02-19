from hopsworks import feature_group


class FeatureGroupApi:
    def __init__(self, client, feature_store_id):
        # or should the client be passed with every call to the API
        self._client = client
        self._feature_store_id = feature_store_id

    def get(self, name, version, dataframe_type):
        """Get feature store with specific id or name.

        :param identifier: id or name of the feature store
        :type identifier: int, str
        :return: the featurestore metadata
        :rtype: FeatureStore
        """
        path_params = [
            "project",
            self._client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            name,
        ]
        query_params = {"version": version}
        return feature_group.FeatureGroup.from_response_json(
            self._client,
            dataframe_type,
            self._client._send_request("GET", path_params, query_params)[0],
        )
