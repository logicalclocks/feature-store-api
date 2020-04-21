from hopsworks import client

from hopsworks import training_dataset


class TrainingDatasetApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def post(self, training_dataset_instance):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
        ]
        headers = {"content-type": "application/json"}
        return training_dataset_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=training_dataset_instance.json(),
            ),
        )

    def get(self, name, version):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            name,
        ]
        query_params = {"version": version}
        return training_dataset.TrainingDataset.from_response_json(
            _client._send_request("GET", path_params, query_params)[0],
        )
