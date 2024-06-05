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
from __future__ import annotations

import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from hsfs import training_dataset_feature as td_feature_mod
from hsfs import util
from hsfs.core import online_store_rest_client_api


_logger = logging.getLogger(__name__)


class OnlineStoreRestClientEngine:
    RETURN_TYPE_FEATURE_VALUE_DICT = "feature_value_dict"
    RETURN_TYPE_FEATURE_VALUE_LIST = "feature_value_list"
    RETURN_TYPE_RESPONSE_JSON = "response_json"  # as a python dict
    MISSING_STATUS = "MISSING"

    def __init__(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        features: List[td_feature_mod.TrainingDatasetFeature],
    ):
        """Initialize the Online Store Rest Client Engine. This class contains the logic to mediate
        the interaction between the python client and the RonDB Rest Server Feature Store API.

        # Arguments:
            feature_store_name: The name of the feature store in which the feature view is registered.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            features: A list of features to be used for the feature vector conversion. Note that the features
                must be ordered according to the feature vector schema.
        """
        _logger.debug(
            f"Initializing Online Store Rest Client Engine for Feature View {feature_view_name}, version: {feature_view_version} in Feature Store {feature_store_name}."
        )
        self._online_store_rest_client_api = (
            online_store_rest_client_api.OnlineStoreRestClientApi()
        )
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._features = features
        self._ordered_feature_names = []
        self._inference_helpers_feature_names = []
        self._no_helpers_feature_names = []
        self._is_inference_helpers_list = []
        self._feature_names_per_fg_id: Dict[int, List[str]] = {}
        for feat in features:
            if not feat.label:
                self._ordered_feature_names.append(feat.name)
                if feat.feature_group.id not in self._feature_names_per_fg_id.keys():
                    self._feature_names_per_fg_id[feat.feature_group.id] = [feat.name]
                else:
                    self._feature_names_per_fg_id[feat.feature_group.id].append(
                        feat.name
                    )
                if feat.inference_helper_column:
                    self._is_inference_helpers_list.append(True)
                elif not feat.training_helper_column:
                    self._is_inference_helpers_list.append(False)
        _logger.debug(
            f"Mapping fg_id to feature names: {self._feature_names_per_fg_id}."
        )

    def build_base_payload(
        self,
        metadata_options: Optional[Dict[str, bool]] = None,
        validate_passed_features: bool = False,
        include_detailed_status: bool = False,
    ) -> Dict[str, Union[str, Dict[str, bool]]]:
        """Build the base payload for the RonDB REST Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            validate_passed_features: Whether to validate the passed features against
                the feature view schema on the RonDB Server.
            include_detailed_status: Whether to include detailed status information in the response.
                This is necessary to drop missing features from the feature vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.

        Returns:
            A payload dictionary containing metadata information to send to the RonDB REST Server Feature Store API.
        """
        _logger.debug(
            f"Building base payload for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        base_payload = {
            "featureStoreName": util.strip_feature_store_suffix(
                self._feature_store_name
            ),
            "featureViewName": self._feature_view_name,
            "featureViewVersion": self._feature_view_version,
            "options": {
                "validatePassedFeatures": validate_passed_features,
                "includeDetailedStatus": include_detailed_status,
            },
        }

        if metadata_options is not None:
            base_payload["metadataOptions"] = {
                "featureName": metadata_options.get("featureName", False),
                "featureType": metadata_options.get("featureType", False),
            }

        _logger.debug(f"Base payload: {base_payload}")
        return base_payload

    def get_single_feature_vector(
        self,
        entry: Dict[str, Any],
        passed_features: Optional[Dict[str, Any]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        drop_missing: bool = False,
        inference_helpers_only: bool = False,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> Union[
        Tuple[Union[List[Any], Dict[str, Any]], Optional[List[Dict[str, Any]]]],
        Dict[str, Any],
    ]:
        """Get a single feature vector from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            entry: A dictionary with the feature names as keys and the primary key as values.
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            drop_missing: Whether to drop missing features from the feature vector. Requires including detailed status.
            inference_helpers_only: Whether to return only the inference helper columns.
            return_type: The type of the return value. Either "feature_value_dict", "feature_value_list" or "response_json".

        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": The status pertinent to this single feature vector. One of COMPLETE, MISSING or ERROR.
                - "features": A list of the feature values.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.
                    Null if metadata options are not set or status is ERROR.
                - "detailedStatus": A list of dictionaries with detailed status information for each read operations.
                    Keys include operationId, featureGroupId, httpStatus and message.

        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            ValueError: If the length of the feature values and metadata in the reponse does not match.
        """
        _logger.debug(
            f"Getting single raw feature vector for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        _logger.debug(f"entry: {entry}, passed features: {passed_features}")
        payload = self.build_base_payload(
            metadata_options=metadata_options,
            # This ensures consistency with the sql client behaviour.
            validate_passed_features=False,
            # Only necessary to get the detailed status if we are not allowing missing features.
            include_detailed_status=drop_missing,
        )
        payload["entries"] = entry
        payload["passedFeatures"] = passed_features

        response = self._online_store_rest_client_api.get_single_raw_feature_vector(
            payload=payload
        )

        if return_type != self.RETURN_TYPE_RESPONSE_JSON:
            return self.convert_rdrs_response_to_feature_value_row(
                row_feature_values=response["features"],
                detailed_status=response.get("detailedStatus", None),
                drop_missing=drop_missing,
                inference_helpers_only=inference_helpers_only,
                return_type=return_type,
            )
        else:
            return response

    def get_batch_feature_vectors(
        self,
        entries: List[Dict[str, Any]],
        passed_features: Optional[List[Dict[str, Any]]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        drop_missing: bool = False,
        inference_helpers_only: bool = False,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> Union[
        Tuple[List[Union[List[Any], Dict[str, Any]]], List[Dict[str, Any]]],
        Dict[str, Any],
    ]:
        """Get a list of feature vectors from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            entries: A list of dictionaries with the feature names as keys and the primary key as values.
            passed_features: A list of dictionaries with the feature names as keys and the values to substitute.
                Note that the list should be ordered in the same way as the entries list.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            drop_missing: Whether to drop missing features from the feature vector. Requires including detailed status.
            helpers_only: Whether to return only the inference helper columns.
            return_type: The type of the return value. Either "feature_value_dict", "feature_value_list" or "response_json".

        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": A list of the status for each feature vector retrieval.
                    Possible values are COMPLETE, MISSING or ERROR.
                - "features": A list containing list of the feature values for each feature_vector.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.
                    Null if metadata options are not set or status is ERROR.
                - "detailedStatus": A list of dictionaries with detailed status information for each read operations.
                    Keys include operationId, featureGroupId, httpStatus and message.

        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            `ValueError`: If the length of the passed features does not match the length of the entries.
        """
        _logger.debug(
            f"Getting batch raw feature vectors for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        _logger.debug(f"entries: {entries}\npassed features: {passed_features}")
        payload = self.build_base_payload(
            metadata_options=metadata_options,
            # This ensures consistency with the sql client behaviour.
            validate_passed_features=False,
            # Only necessary to get the detailed status if we are not allowing missing features.
            include_detailed_status=drop_missing,
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
                "Length of passed features does not match the length of the entries. "
                "If some entries do not have passed features, pass an empty dict for those entries."
            )

        response = self._online_store_rest_client_api.get_batch_raw_feature_vectors(
            payload=payload
        )

        if return_type != self.RETURN_TYPE_RESPONSE_JSON:
            _logger.debug("Converting batch response to feature value rows for each.")
            return [
                self.convert_rdrs_response_to_feature_value_row(
                    row_feature_values=row,
                    detailed_status=detailed_status,
                    drop_missing=drop_missing,
                    return_type=return_type,
                    inference_helpers_only=inference_helpers_only,
                )
                for row, detailed_status in itertools.zip_longest(
                    response["features"], response.get("detailedStatus", []) or []
                )
            ]
        else:
            return response

    def convert_rdrs_response_to_feature_value_row(
        self,
        row_feature_values: Union[List[Any], None],
        drop_missing: bool,
        detailed_status: List[Dict[str, Any]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_LIST,
        inference_helpers_only: bool = False,
    ) -> Union[List[Any], Dict[str, Any]]:
        """Convert the response from the RonDB Rest Server Feature Store API to a feature:value dict.

        When RonDB Server encounter an error it may send a null value for the feature vector. This function
        will handle this case and return a dictionary with None values for all feature names.

        # Arguments:
            row_feature_values: A list of the feature values.
            drop_missing: Whether to drop missing features from the feature vector. Relies on detailed status.
            detailed_status: A list of dictionaries with detailed status information for each read operations.
                Keys include operationId, featureGroupId, httpStatus and message.
            helpers_only: Whether to return only the inference helper columns.
            return_type: The type of the return value. Either "feature_value_dict" or "feature_value_list".

        # Returns:
            A dictionary with the feature names as keys and the feature values as values. Values types are not guaranteed to
            match the feature type in the metadata. Timestamp SQL types are converted to python datetime.
        """
        if drop_missing and (
            detailed_status is None and row_feature_values is not None
        ):
            raise ValueError(
                "Detailed status is required to drop missing features from the feature vector."
            )
        failed_read_feature_names = []
        if detailed_status is not None and drop_missing:
            for operation_status in detailed_status:
                if operation_status["httpStatus"] != 200:
                    failed_read_feature_names.extend(
                        self.feature_names_per_fg_id[operation_status["featureGroupId"]]
                    )
            _logger.debug(
                f"Feature names which failed on read: {failed_read_feature_names}."
            )

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_LIST:
            if row_feature_values is None and drop_missing:
                _logger.debug("Convert null feature vector to empty list.")
                return []
            elif row_feature_values is None:
                _logger.debug(
                    "Feature vector is null, returning None for all features."
                )
                return [
                    None
                    for is_helper in self.is_inference_helpers_list
                    if is_helper is inference_helpers_only
                ]
            elif drop_missing:
                _logger.debug(
                    "Dropping missing features from the feature vector and return as list."
                )
                return [
                    value
                    for (name, value, is_helper) in zip(
                        self.ordered_feature_names,
                        row_feature_values,
                        self.is_inference_helpers_list,
                    )
                    if (
                        name not in failed_read_feature_names
                        and is_helper is inference_helpers_only
                    )
                ]
            _logger.debug("Returning feature vector as list.")
            return row_feature_values

        elif return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            if row_feature_values is None and drop_missing:
                _logger.debug("Convert null feature vector to empty dict.")
                return {}
            elif row_feature_values is None:
                _logger.debug(
                    "Feature vector is null, returning None for all features."
                )
                return {
                    name: None
                    for (name, is_helper) in zip(
                        self.ordered_feature_names, self.is_inference_helpers_list
                    )
                    if is_helper is inference_helpers_only
                }
            elif drop_missing:
                _logger.debug(
                    "Dropping missing features from the feature vector and return as dict."
                )
                return {
                    name: value
                    for (name, value, is_helper) in zip(
                        self.ordered_feature_names,
                        row_feature_values,
                        self.is_inference_helpers_list,
                    )
                    if (
                        name not in failed_read_feature_names
                        and is_helper is inference_helpers_only
                    )
                }
            else:
                _logger.debug("Returning feature vector as dict.")
                return dict(
                    {
                        name: value
                        for (name, value, is_helper) in zip(
                            self.ordered_feature_names,
                            row_feature_values,
                            self.is_inference_helpers_list,
                        )
                        if is_helper is inference_helpers_only
                    }
                )

    @property
    def feature_store_name(self) -> str:
        return self._feature_store_name

    @property
    def feature_view_name(self) -> str:
        return self._feature_view_name

    @property
    def feature_view_version(self) -> int:
        return self._feature_view_version

    @property
    def features(self) -> List[td_feature_mod.TrainingDatasetFeature]:
        return self._features

    @property
    def ordered_feature_names(self) -> List[str]:
        return self._ordered_feature_names

    @property
    def is_inference_helpers_list(self) -> List[bool]:
        return self._is_inference_helpers_list

    @property
    def feature_names_per_fg_id(self) -> Dict[int, List[str]]:
        return self._feature_names_per_fg_id

    @property
    def online_store_rest_client_api(
        self,
    ) -> online_store_rest_client_api.OnlineStoreRestClientApi:
        return self._online_store_rest_client_api
