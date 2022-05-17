#
#   Copyright 2022 Logical Clocks AB
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

from hsfs import (
    client,
    feature_view,
    transformation_function_attached,
    training_dataset,
)
from hsfs.core import job
from hsfs.constructor import serving_prepared_statement, query


class FeatureViewApi:
    _POST = "POST"
    _GET = "GET"
    _DELETE = "DELETE"
    _VERSION = "version"
    _QUERY = "query"
    _BATCH = "batch"
    _DATA = "data"
    _PREPARED_STATEMENT = "preparedstatement"
    _TRANSFORMATION = "transformation"
    _TRAINING_DATASET = "trainingdatasets"
    _COMPUTE = "compute"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id
        self._client = client.get_instance()
        self._base_path = [
            "project",
            self._client._project_id,
            "featurestores",
            self._feature_store_id,
            "featureview",
        ]

    def post(self, feature_view_obj):
        headers = {"content-type": "application/json"}
        return feature_view_obj.update_from_response_json(
            self._client._send_request(
                self._POST,
                self._base_path,
                headers=headers,
                data=feature_view_obj.json(),
            )
        )

    def get_by_name(self, name):
        path = self._base_path + [name]
        return [
            feature_view.FeatureView.from_response_json(fv)
            for fv in self._client._send_request(
                self._GET, path, {"expand": ["query", "features"]}
            )["items"]
        ]

    def get_by_name_version(self, name, version):
        path = self._base_path + [name, self._VERSION, version]
        return feature_view.FeatureView.from_response_json(
            self._client._send_request(
                self._GET, path, {"expand": ["query", "features"]}
            )
        )

    def delete_by_name(self, name):
        path = self._base_path + [name]
        self._client._send_request(self._DELETE, path)

    def delete_by_name_version(self, name, version):
        path = self._base_path + [name, self._VERSION, version]
        self._client._send_request(self._DELETE, path)

    def get_batch_query(
        self,
        name,
        version,
        start_time,
        end_time,
        with_label=False,
        is_python_engine=False,
    ):
        path = self._base_path + [
            name,
            self._VERSION,
            version,
            self._QUERY,
            self._BATCH,
        ]
        return query.Query.from_response_json(
            self._client._send_request(
                self._GET,
                path,
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "with_label": with_label,
                    "is_hive_engine": is_python_engine,
                },
            )
        )

    def get_serving_prepared_statement(self, name, version, batch):
        path = self._base_path + [
            name,
            self._VERSION,
            version,
            self._PREPARED_STATEMENT,
        ]
        headers = {"content-type": "application/json"}
        query_params = {"batch": batch}
        return serving_prepared_statement.ServingPreparedStatement.from_response_json(
            self._client._send_request("GET", path, query_params, headers=headers)
        )

    def get_attached_transformation_fn(self, name, version):
        path = self._base_path + [name, self._VERSION, version, self._TRANSFORMATION]
        return transformation_function_attached.TransformationFunctionAttached.from_response_json(
            self._client._send_request("GET", path)
        )

    def create_training_dataset(self, name, version, training_dataset_obj):
        path = self.get_training_data_base_path(name, version)
        headers = {"content-type": "application/json"}
        return training_dataset_obj.update_from_response_json(
            self._client._send_request(
                "POST", path, headers=headers, data=training_dataset_obj.json()
            )
        )

    def get_training_dataset_by_version(self, name, version, training_dataset_version):
        path = self.get_training_data_base_path(name, version, training_dataset_version)
        return training_dataset.TrainingDataset.from_response_json_single(
            self._client._send_request("GET", path)
        )

    def compute_training_dataset(
        self, name, version, training_dataset_version, td_app_conf
    ):
        path = self.get_training_data_base_path(
            name, version, training_dataset_version
        ) + [self._COMPUTE]
        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            self._client._send_request(
                "POST", path, headers=headers, data=td_app_conf.json()
            )
        )

    def delete_training_data(self, name, version):
        path = self.get_training_data_base_path(name, version)
        return self._client._send_request("DELETE", path)

    def delete_training_data_version(self, name, version, training_dataset_version):
        path = self.get_training_data_base_path(name, version, training_dataset_version)
        return self._client._send_request("DELETE", path)

    def delete_training_dataset_only(self, name, version):
        path = self.get_training_data_base_path(name, version) + [self._DATA]
        return self._client._send_request("DELETE", path)

    def delete_training_dataset_only_version(
        self, name, version, training_dataset_version
    ):
        path = self.get_training_data_base_path(
            name, version, training_dataset_version
        ) + [self._DATA]

        return self._client._send_request("DELETE", path)

    def get_training_data_base_path(self, name, version, training_data_version=None):
        if training_data_version:
            return self._base_path + [
                name,
                self._VERSION,
                version,
                self._TRAINING_DATASET,
                self._VERSION,
                training_data_version,
            ]
        else:
            return self._base_path + [
                name,
                self._VERSION,
                version,
                self._TRAINING_DATASET,
            ]
