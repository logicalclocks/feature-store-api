#
#   Copyright 2021 Logical Clocks AB
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

from hsfs import client, transformation_function, transformation_function_attached


class TransformationFunctionApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def register_transformation_fn(self, transformation_function_instance):
        """
        Register transformation function in backend
        Args:
        transformation_function_instance: TransformationFunction, required
            metadata object of transformation function.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "transformationfunctions",
        ]
        headers = {"content-type": "application/json"}
        return transformation_function.TransformationFunction.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=transformation_function_instance.json(),
            )
        )

    def get_transformation_fn(self, name, version):
        """
        Retrieve transformation function from backend
        Args:
        name: TransformationFunction name, required
            name of transformation function.
        version: TransformationFunction version, required
            version of transformation function.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "transformationfunctions",
        ]

        if name:
            query_params = {"name": name}
            if version:
                query_params["version"] = version
            return transformation_function.TransformationFunction.from_response_json(
                _client._send_request("GET", path_params, query_params)
            )
        else:
            return transformation_function.TransformationFunction.from_response_json(
                _client._send_request("GET", path_params)
            )

    def delete(self, transformation_function_instance):
        """Delete a transformation function.
        Args:
        transformation_function_instance: TransformationFunction, required
            metadata object of transformation function.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "transformationfunctions",
            transformation_function_instance.id,
        ]
        headers = {"content-type": "application/json"}
        _client._send_request("DELETE", path_params, headers=headers)

    def get_td_transformation_fn(self, training_dataset_instance):
        """
        Retrieve TransformationFunctionAttached instance
        Args:
        training_dataset_instance: TrainingDataset, required
            training dataset metadata object.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "transformationfunctions",
        ]

        return transformation_function_attached.TransformationFunctionAttached.from_response_json(
            _client._send_request("GET", path_params)
        )
