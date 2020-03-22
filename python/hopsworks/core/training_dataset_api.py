from hopsworks import training_dataset as td


class TrainingDatasetApi:
    def __init__(self, client, feature_store_id):
        self._client = client
        self._feature_store_id = feature_store_id

    def post(self, training_dataset):
        path_params = [
            "project",
            self._client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
        ]
        return td.TrainingDataset.from_response_json(
            self._client, self._client._send_request("POST", path_params)[0],
        )
