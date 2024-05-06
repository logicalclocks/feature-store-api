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
from __future__ import annotations

from typing import List, Optional, Union

from hsfs import (
    client,
    transformation_function,
)


class TransformationFunctionApi:
    def __init__(self, feature_store_id: int) -> None:
        self._feature_store_id = feature_store_id

    def register_transformation_fn(
        self,
        transformation_function_instance: transformation_function.TransformationFunction,
    ) -> transformation_function.TransformationFunction:
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

    def get_transformation_fn(
        self, name: Optional[str], version: Optional[int]
    ) -> Union[
        transformation_function.TransformationFunction,
        List[transformation_function.TransformationFunction],
    ]:
        """
        Retrieve transformation function from backend
        Args:
        name: TransformationFunction name, optional
            name of transformation function.
        version: TransformationFunction version, optional
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

    def delete(
        self,
        transformation_function_instance: transformation_function.TransformationFunction,
    ) -> None:
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
