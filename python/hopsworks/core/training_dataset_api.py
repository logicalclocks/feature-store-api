from hopsworks import client
from hopsworks import training_dataset as td


class TrainingDatasetApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def post(self, training_dataset):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
        ]
        return td.TrainingDataset.from_response_json(
            _client._send_request("POST", path_params)[0],
        )
