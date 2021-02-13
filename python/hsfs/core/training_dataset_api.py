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
from hsfs.core import job
from hsfs.constructor import serving_prepared_statement


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

    def compute(self, training_dataset_instance, td_app_conf):
        """
        Setup a Hopsworks job to compute the query and write the training dataset
        Args:
            training_dataset_instance (training_dataset): the metadata instance of the training dataset
            app_options ([type]): the configuration for the training dataset job application
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "compute",
        ]
        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=td_app_conf.json()
            )
        )

    def update_metadata(
        self, training_dataset_instance, training_dataset_copy, query_parameter
    ):
        """Update the metadata of a training dataset.

        This only updates description and schema/features. The
        `training_dataset_copy` is the metadata object sent to the backend, while
        `training_dataset_instance` is the user object, which is only updated
        after a successful REST call.

        # Arguments
            training_dataset_instance: FeatureGroup. User metadata object of the
                training dataset.
            training_dataset_copy: FeatureGroup. Metadata object of the training
                dataset with the information to be updated.
            query_parameter: str. Query parameter that will be set to true to
                control which information is updated. E.g. "updateMetadata" or
                "updateStatsConfig".

        # Returns
            FeatureGroup. The updated feature group metadata object.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
        ]
        headers = {"content-type": "application/json"}
        query_params = {query_parameter: True}
        return training_dataset_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                query_params,
                headers=headers,
                data=training_dataset_copy.json(),
            ),
        )

    def get_serving_prepared_statement(self, training_dataset_instance):
        """
        ... the training dataset

        Args:
            training_dataset_instance (training_dataset): the metadata instance of the training dataset
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "preparedstatements",
        ]
        headers = {"content-type": "application/json"}
        return serving_prepared_statement.ServingPreparedStatement.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )
