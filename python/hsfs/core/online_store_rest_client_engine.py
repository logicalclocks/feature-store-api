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
from typing import Optional, Any, Union, Dict
from datetime import datetime

from hsfs.core import online_store_rest_client_api
from hsfs import util


class OnlineStoreRestClientEngine:
    RETURN_TYPE_FEATURE_VALUE_DICT = "feature_value_dict"
    RETURN_TYPE_RESPONSE_JSON = "response_json"  # as a python dict
    SQL_TIMESTAMP_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self):
        self._online_store_rest_client_api = (
            online_store_rest_client_api.OnlineStoreRestClientApi()
        )

    def _build_base_payload(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> Dict[str, Union[str, Dict[str, bool]]]:
        """Build the base payload for the RonDB REST Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        !!! warning
            featureName and featureType must be set to True to allow the response to be converted
            to a feature vector with convert_rdrs_response_to_dict_feature_vector.

        Args:
            feature_store_name: The name of the feature store in which the feature view is registered.
                The suffix '_featurestore' should be omitted.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_vector" or "response_json".
                If "feature_value_dict" is selected the payload will enforce fetching feature metadata.

        Returns:
            The payload to send to the RonDB REST Server Feature Store API.
        """
        return {
            "featureStoreName": util.strip_feature_store_suffix(feature_store_name),
            "featureViewName": feature_view_name,
            "featureViewVersion": feature_view_version,
            "metadataOptions": {
                "featureName": True
                if (
                    metadata_options is None
                    or return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT
                )
                else metadata_options.get("featureName", True),
                "featureType": True
                if (
                    metadata_options is None
                    or return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT
                )
                else metadata_options.get("featureType", True),
            },
        }

    def get_single_raw_feature_vector(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        entry: Dict[str, Any],
        passed_features: Optional[Dict[str, Any]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> Dict[str, Any]:
        """Get a single feature vector from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            feature_store_name: The name of the feature store in which the feature view is registered.
                The suffix '_featurestore' should be omitted.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            entry: A dictionary with the feature names as keys and the primary key as values.
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_value_dict" or "response_json".

        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": The status pertinent to this single feature vector.
                - "features": A list of the feature values.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.

        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            ValueError: If the length of the feature values and metadata in the reponse does not match.
        """
        payload = self._build_base_payload(
            feature_store_name=feature_store_name,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            metadata_options=metadata_options,
            return_type=return_type,
        )
        payload["entries"] = entry
        if isinstance(passed_features, dict):
            payload["passedFeatures"] = passed_features
        else:
            payload["passedFeatures"] = {}

        response = self._online_store_rest_client_api.get_single_raw_feature_vector(
            payload=payload
        )

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            # If the status is "MISSING" the response will not contain metadata
            if response["status"] == "MISSING":
                entry.update(passed_features)
                return entry
            return self.convert_rdrs_response_to_feature_value_dict(
                row_feature_values=response["features"], metadatas=response["metadata"]
            )
        else:
            return response

    def get_batch_raw_feature_vectors(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        entries: list[Dict[str, Any]],
        passed_features: Optional[list[Dict[str, Any]]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> list[Dict[str, Any]]:
        """Get a list of feature vectors from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            feature_store_name: The name of the feature store in which the feature view is registered.
                The suffix '_featurestore' should be omitted.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            entries: A list of dictionaries with the feature names as keys and the primary key as values.
            passed_features: A list of dictionaries with the feature names as keys and the values to substitute.
                Note that the list should be ordered in the same way as the entries list.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_value_dict" or "response_json".

        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": A list of the status for each feature vector retrieval.
                - "features": A list containing list of the feature values for each feature_vector.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.

        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            `ValueError`: If the length of the passed features does not match the length of the entries.
        """
        payload = self._build_base_payload(
            feature_store_name=feature_store_name,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            metadata_options=metadata_options,
            return_type=return_type,
        )
        payload["entries"] = entries
        if isinstance(passed_features, list) and (
            len(passed_features) == len(entries) or len(passed_features) == 0
        ):
            payload["passedFeatures"] = passed_features
        elif passed_features is None:
            payload["passedFeatures"] = []
        else:
            raise ValueError(
                "Length of passed features does not match the length of the entries."
            )

        response = self._online_store_rest_client_api.get_batch_raw_feature_vectors(
            payload=payload
        )

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            return self.convert_batch_response_to_feature_value_dict(
                batch_response=response
            )
        else:
            return response

    def convert_batch_response_to_feature_value_dict(
        self, batch_response: Dict[str, Any]
    ) -> list[Dict[str, Any]]:
        """Split the response from the RonDB Rest Server Feature Store API to convert each feature vector to a dictionary.

        Skip the feature vectors that have an error status.

        # Arguments:
            batch_response: The response from the RonDB Rest Server Feature Store API.

        # Returns:
            A list of dictionaries with the feature names as keys and the feature values as values.
        """
        return [
            self.convert_rdrs_response_to_feature_value_dict(
                row_feature_values=row, metadatas=batch_response["metadata"]
            )
            for row, status in zip(batch_response["features"], batch_response["status"])
            if status != "ERROR"
        ]

    def convert_rdrs_response_to_feature_value_dict(
        self, row_feature_values: list[Any], metadatas: list[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Convert the response from the RonDB Rest Server Feature Store API to a feature:value dict.

        # Arguments:
            row_feature_values: A list of the feature values.
            metadatas: A list of dictionaries with metadata for each feature. The order should match the order of the features.

        # Returns:
            A dictionary with the feature names as keys and the feature values as values. Values types are not guaranteed to
            match the feature type in the metadata. Timestamp SQL types are converted to python datetime.
        """
        return {
            metadata["featureName"]: (
                vector_value
                if (metadata["featureType"] != "timestamp" or vector_value is None)
                else datetime.strptime(vector_value, self.SQL_TIMESTAMP_STRING_FORMAT)
            )
            for vector_value, metadata in zip(row_feature_values, metadatas)
            if metadata is not None
        }