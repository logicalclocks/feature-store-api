#
#   Copyright 2024 Hopsworks AB
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
from typing import Any
import json
from requests import Response

from hsfs.client import rondb_rest_client


class RondbRestApi:
    def get_single_raw_feature_vector(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Get a single feature vector from the feature store.

        Check the RonDB Rest Server documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        Args:
            payload: The payload to send to the RonDB REST Server Feature Store API.
                The payload should contains the following fields:
                    - "featureStoreName": The name of the feature store in which the feature view is registered.
                    - "featureViewName": The name of the feature view from which to retrieve the feature vector.
                    - "featureViewVersion": The version of the feature view from which to retrieve the feature vector.
                    - "entries": A dictionary with the feature names as keys and the primary key as values for the specific vector.
                    - "metadataOptions": Whether to include feature metadata in the response.
                        Keys are "featureName" and "featureType" and values are boolean.
                    - "passedFeatures": A dictionary with the feature names as keys and the values to substitute for this specific vector.

        Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": The status pertinent to this single feature vector.
                - "features": A list of the feature values.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.

        Raises:
            HTTPError: If the status code is not 200.
        """
        return self.handle_rdrs_feature_store_response(
            rondb_rest_client.get_instance()._send_request(
                "POST",
                path_params=["feature_store"],
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
            ),
        )

    def get_batch_raw_feature_vectors(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Get a list of feature vectors from the feature store.

        Check the RonDB Rest Server documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        Args:
            payload: The payload to send to the RonDB REST Server Feature Store API.
            The payload should contains the following fields:
                - "featureStoreName": The name of the feature store in which the feature view is registered.
                - "featureViewName": The name of the feature view from which to retrieve the feature vector.
                - "featureViewVersion": The version of the feature view from which to retrieve the feature vector.
                - "entries": A list of dictionaries with the feature names as keys and the primary key as values.
                - "passedFeatures": A list of dictionaries with the feature names as keys and the values to substitute.
                    Note that the list should be ordered in the same way as the entries list.
                - "metadataOptions": Whether to include feature metadata in the response.
                    Keys are "featureName" and "featureType" and values are boolean.

        Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": A list of the status for each feature vector retrieval.
                - "features": A list containing list of the feature values for each feature_vector.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.

        Raises:
            HTTPError: If the status code is not 200.
        """
        return self.handle_rdrs_feature_store_response(
            rondb_rest_client.get_instance()._send_request(
                "POST",
                path_params=["batch_feature_store"],
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
            ),
        )

    def ping_rondb_rest_server(self) -> int:
        """Ping the RonDB Rest Server to check if it is alive."""
        return rondb_rest_client.get_instance()._send_request(
            "GET", path_params=["ping"]
        )

    def handle_rdrs_feature_store_response(self, response: Response) -> dict[str, Any]:
        """Handle the response from the RonDB Rest Server.

        Args:
            response: The response from the RonDB Rest Server.

        Returns:
            The response json if the status code is 200, otherwise raises an error.

        Raises:
            HTTPError: If the status code is not 200.
        """
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
