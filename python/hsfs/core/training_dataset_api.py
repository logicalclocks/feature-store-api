#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from hsfs import client, training_dataset


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

    def get_query(self, training_dataset_instance, with_label):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "query",
        ]
        query_params = {"withLabel": with_label}
        return _client._send_request("GET", path_params, query_params)
