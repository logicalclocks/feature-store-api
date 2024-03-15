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
import logging
from base64 import b64decode
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import hsfs
from avro.schema import Schema
from hsfs import serving_key, util
from hsfs.core import online_store_rest_client_api


HAS_FASTAVRO = False
try:
    from fastavro import schemaless_reader

    HAS_FASTAVRO = True
except ImportError:
    from avro.io import BinaryDecoder


_logger = logging.getLogger(__name__)


class OnlineStoreRestClientEngine:
    RETURN_TYPE_FEATURE_VALUE_DICT = "feature_value_dict"
    RETURN_TYPE_FEATURE_VALUE_LIST = "feature_value_list"
    RETURN_TYPE_RESPONSE_JSON = "response_json"  # as a python dict
    SQL_TIMESTAMP_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        features: List["hsfs.training_dataset_feature.TrainingDatasetFeature"],
        skip_fg_ids: Set[int],
        complex_features: Dict[str, Schema] = None,
        transformation_functions: Dict[str, Callable] = None,
        serving_keys: Optional[List[serving_key.ServingKey]] = None,
    ):
        """Initialize the Online Store Rest Client Engine. This class contains the logic to mediate
        the interaction between the python client and the RonDB Rest Server Feature Store API.
        # Arguments:
            feature_store_name: The name of the feature store in which the feature view is registered.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            features: A list of features to be used for the feature vector conversion. Note that the features
                must be ordered according to the feature vector schema.
            skip_fg_ids: A set of feature group ids to skip when inferring feature vector schema.
                These ids are linked to Feature Groups with embeddings and
                therefore stored in the embedding online store (see vector_db_client).
            serving_keys: Required serving keys, it allows us to fake the missing ones in entries.
        """
        _logger.info(
            f"Initializing Online Store Rest Client Engine for Feature View {feature_view_name}, version: {feature_view_version} in Feature Store {feature_store_name}."
        )
        self._online_store_rest_client_api = (
            online_store_rest_client_api.OnlineStoreRestClientApi()
        )
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._features = features
        _logger.debug(f"Features: {features}")
        self._skip_fg_ids = skip_fg_ids
        _logger.debug(
            f"These indexes must be skip as they correspond to features of an embedded client: {skip_fg_ids}"
        )

        if complex_features is None:
            self._complex_feature_decoders = {}
        else:
            _logger.debug(f"Complex feature schemas: {complex_features}")
            self._complex_feature_decoders = self.build_complex_feature_decoders(
                complex_feature_schemas=complex_features
            )

        if transformation_functions is None:
            self._transformation_fns = {}
        else:
            self._transformation_fns = transformation_functions
            _logger.debug(f"Transformation functions: {transformation_functions}")

        self.set_return_feature_value_handlers()

        # Temporary solution to handle missing serving keys in entries
        self._serving_keys = serving_keys

    def build_base_payload(
        self,
        metadata_options: Optional[Dict[str, bool]] = None,
        # Missing in current RonDB Server.
        # validate_passed_features: bool = False,
    ) -> Dict[str, Union[str, Dict[str, bool]]]:
        """Build the base payload for the RonDB REST Server Feature Store API.
        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server
        # Arguments:
            validate_passed_features: Whether to validate the passed features against
                the feature view schema on the RonDB Server.
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
            # "options": {
            #     "validatePassedFeatures": validate_passed_features,
            # },
        }

        if metadata_options is not None:
            base_payload["metadataOptions"] = {
                "featureName": metadata_options.get("featureName", False),
                "featureType": metadata_options.get("featureType", False),
            }

        _logger.debug(f"Base payload: {base_payload}")
        return base_payload

    def handle_missing_serving_keys_in_entry(
        self, entry: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], bool]:
        """Handle missing serving keys in the entry dictionary by filling them with a random unlikely value.
        This function is used to avoid backend errors on missing keys. This is a hacky solution and should be
        phased out as soon as possible with a RonDB Server upgrade.
        # Arguments:
            entry: A dictionary with the feature names as keys and the primary key as values.
        # Returns:
            The entry dictionary with the missing serving keys filled with None values.
        """
        _logger.debug(
            f"Handling missing serving keys in entry for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        use_batch_api = False
        entry = entry.copy()
        for sk in self._serving_keys:
            if sk.join_index in self.skip_fg_ids:
                # The FG is in Opensearch therefore the serving key is not relevant for RonDB Server.
                continue
            if (
                sk.feature_name not in entry.keys()
                and (sk.prefix + sk.feature_name) not in entry.keys()
            ):
                # _logger.debug(
                #     f"Adding missing serving key {sk.prefix + sk.feature_name} to entry with value `1`."
                # )
                # entry[sk.prefix + sk.feature_name] = 1
                _logger.debug(
                    f"Switching to batch API as {sk.prefix + sk.feature_name} is missing from entry."
                )
                use_batch_api = True
            elif (
                sk.feature_name in entry.keys()
                and (sk.prefix + sk.feature_name) not in entry.keys()
            ):
                # check if the feature name is already prefixed, if not fix it
                _logger.debug(
                    f"Fixing prefix serving key {sk.prefix + sk.feature_name} to entry with value {entry[sk.feature_name]}."
                )
                entry[sk.prefix + sk.feature_name] = entry[sk.feature_name]

                # check that there is no serving key with the same name but no prefix which also relies on this entry.
                # e.g joined fg_1, fg_2 on feature_1 which is pk for both fg_1 and fg_2. fg_1 has serving key feature_1
                # and fg_2 has serving key prefix + feature_1. Only {"feature_1": val} is provided. The correct entry would be
                # {"feature_1": val, prefix + "feature_1": val}. In this particular case we cannot remove entry["feature_1"]
                # as it is used by fg_1. We need to keep both entries. Otherwise we have to remove entry["feature_1"].
                if (
                    len(
                        [
                            serv_k
                            for serv_k in self._serving_keys
                            if (
                                serv_k.feature_name == sk.feature_name
                                and (serv_k.prefix == "" or serv_k.prefix is None)
                            )
                        ]
                    )
                    == 0
                ):
                    _logger.debug(f"Removing serving key {sk.feature_name} from entry.")
                    entry.pop(sk.feature_name)

        _logger.debug(f"entry with altered primary-key:value pair : {entry}")

        return entry, use_batch_api

    def handle_passed_features_dict(
        self, passed_features: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """This must remove all passed features for which the feature name is not part of the feature vector.
        # Arguments:
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.
        # Returns:
            A purged dictionary.
        """
        if passed_features is None:
            _logger.debug("No passed features to handle.")
            return {}

        _logger.debug("Purging passed features dictionary.")
        new_passed_features = {
            name: value
            for name, value in passed_features.items()
            if name in self._ordered_feature_names
        }
        _logger.debug(
            f"Purged following keys: {[key for key in passed_features.keys() if key not in new_passed_features.keys()]}"
        )
        return new_passed_features

    def get_single_feature_vector(
        self,
        entry: Dict[str, Any],
        passed_features: Optional[Dict[str, Any]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> Dict[str, Any]:
        """Get a single feature vector from the online feature store via RonDB Rest Server Feature Store API.
        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server
        # Arguments:
            entry: A dictionary with the feature names as keys and the primary key as values.
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_value_dict", "feature_value_list" or "response_json".
        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": The status pertinent to this single feature vector.
                - "features": A list of the feature values.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.
                    Null if metadata options are not set or status is ERROR.
        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            ValueError: If the length of the feature values and metadata in the reponse does not match.
        """
        _logger.info(
            f"Getting single raw feature vector for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        _logger.debug(f"entry: {entry}, passed features: {passed_features}")
        payload = self.build_base_payload(
            metadata_options=metadata_options,
            # This ensures consistency with the sql client behaviour. Missing in current RonDB Server.
            # validate_passed_features=False,
        )
        payload["entries"], use_batch_api = self.handle_missing_serving_keys_in_entry(
            entry
        )
        payload["passedFeatures"] = self.handle_passed_features_dict(passed_features)

        if use_batch_api:
            _logger.debug(
                "Switching to batch API as at least one serving key is missing from entry."
            )
            payload["entries"] = [payload["entries"]]
            payload["passedFeatures"] = [payload["passedFeatures"]]
            response = {
                "features": self._online_store_rest_client_api.get_batch_raw_feature_vectors(
                    payload=payload
                )[
                    "features"
                ][
                    0
                ]
            }
        else:
            response = self._online_store_rest_client_api.get_single_raw_feature_vector(
                payload=payload
            )

        if return_type != self.RETURN_TYPE_RESPONSE_JSON:
            return self.convert_rdrs_response_to_feature_value_row(
                row_feature_values=response["features"],
                passed_features=passed_features,
                return_type=return_type,
            )
        else:
            return response

    def get_batch_feature_vectors(
        self,
        entries: List[Dict[str, Any]],
        passed_features: Optional[List[Dict[str, Any]]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> List[Dict[str, Any]]:
        """Get a list of feature vectors from the online feature store via RonDB Rest Server Feature Store API.
        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server
        # Arguments:
            entries: A list of dictionaries with the feature names as keys and the primary key as values.
            passed_features: A list of dictionaries with the feature names as keys and the values to substitute.
                Note that the list should be ordered in the same way as the entries list.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_value_dict", "feature_value_list" or "response_json".
        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": A list of the status for each feature vector retrieval.
                - "features": A list containing list of the feature values for each feature_vector.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.
                    Null if metadata options are not set or status is ERROR.
        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            `ValueError`: If the length of the passed features does not match the length of the entries.
        """
        _logger.info(
            f"Getting batch raw feature vectors for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        _logger.debug(f"entries: {entries}\npassed features: {passed_features}")
        payload = self.build_base_payload(
            metadata_options=metadata_options,
            # This ensures consistency with the sql client behaviour. Missing in current RonDB Server.
            # validate_passed_features=False,
        )
        payload["entries"] = [
            self.handle_missing_serving_keys_in_entry(entry)[0] for entry in entries
        ]
        if isinstance(passed_features, list) and (
            len(passed_features) == len(entries) or len(passed_features) == 0
        ):
            payload["passedFeatures"] = [
                self.handle_passed_features_dict(passed_features=passed_feature)
                for passed_feature in passed_features
            ]
        elif passed_features is None:
            payload["passedFeatures"] = []
        else:
            raise ValueError(
                "Length of passed features does not match the length of the entries."
            )

        response = self._online_store_rest_client_api.get_batch_raw_feature_vectors(
            payload=payload
        )
        _logger.debug(response)

        if return_type != self.RETURN_TYPE_RESPONSE_JSON:
            _logger.debug("Converting batch response to feature value rows for each.")
            if passed_features is None or len(passed_features) == 0:
                passed_features = [None] * len(response["features"])
            return [
                self.convert_rdrs_response_to_feature_value_row(
                    row_feature_values=row,
                    passed_features=passed,
                    return_type=return_type,
                )
                for row, passed in zip(response["features"], passed_features)
            ]
        else:
            return response

    def convert_rdrs_response_to_feature_value_row(
        self,
        row_feature_values: Union[List[Any], None],
        passed_features: Dict[str, Any] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_LIST,
    ) -> Union[List[Any], Dict[str, Any]]:
        """Convert the response from the RonDB Rest Server Feature Store API to a feature:value dict.
        When RonDB Server encounter an error it may send a null value for the feature vector. This function
        will handle this case and return a dictionary with None values for all feature names.
        # Arguments:
            row_feature_values: A list of the feature values.
            return_type: The type of the return value. Either "feature_value_dict" or "feature_value_list".
        # Returns:
            A dictionary with the feature names as keys and the feature values as values. Values types are not guaranteed to
            match the feature type in the metadata. Timestamp SQL types are converted to python datetime.
        """
        if row_feature_values is None:
            _logger.debug("Feature vector is null, returning None for all features.")
            return (
                {name: None for name in self._ordered_feature_names}
                if return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT
                else [None for _ in self._ordered_feature_names]
            )

        # Merge with handlers if kept in the future.
        if passed_features is not None and len(passed_features.keys()) > 0:
            _logger.debug(
                f"Passed features: {passed_features} are merged with row feature {row_feature_values}."
            )
            row_feature_values = [
                passed_features.get(name, None)
                or passed_features.get(fg_feature_name, None)
                if value is None
                else value
                for name, fg_feature_name, value in zip(
                    self._ordered_feature_names,
                    self._ordered_feature_group_feature_names,
                    row_feature_values,
                )
            ]
        _logger.debug(f"Row feature values after passed features: {row_feature_values}")

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_LIST:
            _logger.debug(
                f"Apply deserializer or transformation function to feature value list : {row_feature_values}."
            )
            return [
                self._return_feature_value_handlers[name](value)
                if value is not None
                else None
                for name, value in zip(
                    self.ordered_feature_names_and_dtypes,
                    row_feature_values,
                )
            ]
        elif return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            _logger.debug(
                f"Apply deserializer or transformation function and convert feature values to dictionary : {row_feature_values}."
            )
            return {
                name: (
                    self._return_feature_value_handlers[name](value)
                    if value is not None
                    else None
                )
                for (name, value) in zip(
                    self._ordered_feature_names,
                    row_feature_values,
                )
            }

    def build_complex_feature_decoders(
        self, complex_feature_schemas: Dict[str, Schema]
    ) -> Dict[str, Callable]:
        """Build a dictionary of functions to deserialize the complex feature values returned from RonDB Server."""
        if HAS_FASTAVRO:
            return {
                f_name: (
                    lambda feature_value, avro_schema=schema: schemaless_reader(
                        BytesIO(b64decode(feature_value)),
                        avro_schema.writers_schema.to_json(),
                    )
                    # embedded features are deserialized already but not complex features stored in Opensearch
                    if isinstance(feature_value, str)
                    else feature_value
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }
        else:
            return {
                f_name: lambda feature_value, avro_schema=schema: avro_schema.read(
                    BinaryDecoder(BytesIO(b64decode(feature_value)))
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }

    def set_return_feature_value_handlers(self) -> List[Callable]:
        """Build a dictionary of functions to convert/deserialize/transform the feature values returned from RonDB Server.
        TODO: This function will be refactored with sql client to avoid duplicating logic and code.
        Re-using the current logic from the vector server means that we currently iterate over the feature vectors
        and values multiple times, as well as converting the feature values to a dictionary and then back to a list.
        - The handlers do not need to handle the case where the feature value is None.
        - The handlers should first convert/deserialize the feature values and then transform them if necessary.
        - The handlers should be reusable without paying overhead for rebuilding them for each feature vector.
        """
        self._return_feature_value_handlers = {}
        self._ordered_feature_names = []
        self._ordered_feature_group_feature_names = []
        _logger.debug(
            f"Setting return feature value handlers for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        for feature in self._features:
            # These features are not part of the feature vector.
            if feature.label:
                _logger.debug(
                    f"Skipping Feature {feature.name} as it is a label therefore not part of feature vector."
                )
                continue
            if feature.training_helper_column:
                _logger.debug(
                    f"Skipping Feature {feature.name} as it is a training helper column therefore not part of feature vector."
                )
                continue

            self._ordered_feature_names.append(feature.name)
            self._ordered_feature_group_feature_names.append(
                feature.feature_group_feature_name
            )
            if feature.is_complex() and feature.name in self._transformation_fns.keys():
                # deserialize and then transform
                _logger.debug(
                    f"Adding return feature value deserializer for complex feature {feature.name} with transformation function."
                )
                self._return_value_handlers[
                    feature.name
                ] = lambda feature_value, feature_name=feature.name: _logger.debug(
                    f"Deserialize and transform value of feature {feature_name}"
                ) or self.transformation_fns[
                    feature_name
                ].transformation_fn(
                    self._complex_feature_decoder[feature_name](feature_value)
                )
            elif feature.is_complex():
                # deserialize only
                _logger.debug(
                    f"Adding return feature value deserializer for complex feature {feature.name}."
                )
                self._return_feature_value_handlers[
                    feature.name
                ] = lambda feature_value, feature_name=feature.name: _logger.debug(
                    f"Transform value of feature {feature_name}"
                ) or self._complex_feature_decoders[
                    feature_name
                ](
                    feature_value
                )
            elif (
                feature.type == "timestamp"
                and feature.name in self._transformation_fns.keys()
            ):
                # convert and then transform
                _logger.debug(
                    f"Adding return feature value converter for timestamp feature {feature.name} with transformation function."
                )
                self._return_feature_value_handlers[
                    feature.name
                ] = lambda feature_value, feature_name=feature.name: _logger.debug(
                    f"Convert and transform timestamp feature {feature_name}"
                ) or self._transformation_fns[
                    feature_name
                ].transformation_fn(
                    self._handle_timestamp_based_on_dtype(feature_value)
                )
            elif feature.type == "timestamp":
                # convert only
                _logger.debug(
                    f"Adding return feature value converter for timestamp feature {feature.name}."
                )
                self._return_feature_value_handlers[
                    feature.name
                ] = self._handle_timestamp_based_on_dtype
            elif feature.name in self._transformation_fns.keys():
                # transform only
                _logger.debug(
                    f"Adding return feature value transformation for feature {feature.name}."
                )
                self._return_feature_value_handlers[
                    feature.name
                ] = lambda feature_value, feature_name=feature.name: _logger.debug(
                    f"Transform value of feature {feature_name}"
                ) or self._transformation_fns[
                    feature_name
                ].transformation_fn(
                    feature_value
                )
            else:
                # no transformation
                _logger.debug(
                    f"Adding return feature value handler for feature {feature.name}."
                )
                self._return_feature_value_handlers[feature.name] = (
                    lambda feature_value, feature_name=feature.name: _logger.debug(
                        f"Applying identity to value of {feature_name}"
                    )
                    or feature_value
                )
        _logger.debug(
            f"Ordered feature names, skipping label or training helper columns: {self._ordered_feature_names}"
        )

    def _handle_timestamp_based_on_dtype(
        self, timestamp_value: Union[str, int]
    ) -> Optional[datetime]:
        """Handle the timestamp based on the dtype which is returned.
        Currently timestamp which are in the database are returned as string. Whereas
        passed features which were given as datetime are returned as integer timestamp.
        # Arguments:
            timestamp_value: The timestamp value to be handled, either as int or str.
        """
        if isinstance(timestamp_value, int):
            _logger.debug(
                f"Converting timestamp {timestamp_value} to datetime from UNIX timestamp."
            )
            return datetime.fromtimestamp(
                timestamp_value / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
        elif isinstance(timestamp_value, str):
            _logger.debug(
                f"Parsing timestamp {timestamp_value} to datetime from SQL timestamp string."
            )
            return datetime.strptime(timestamp_value, self.SQL_TIMESTAMP_STRING_FORMAT)
        elif isinstance(timestamp_value, datetime):
            _logger.debug(
                f"Returning passed timestamp {timestamp_value} as it is already a datetime."
            )
            return timestamp_value
        else:
            raise ValueError(
                f"Timestamp value {timestamp_value} was expected to be of type int or str."
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
    def features(self) -> List["hsfs.training_dataset_feature.TrainingDatasetFeature"]:
        return self._features

    @property
    def skip_fg_ids(self) -> List[int]:
        """A list of feature ids to skip when inferring feature vector schema.
        These ids are linked to features which are part of a Feature Group with embeddings and
        therefore stored in the embedding online store (see vector_db_client).
        """
        return self._skip_fg_ids

    @property
    def ordered_feature_names_and_dtypes(self) -> List[Tuple[str, str]]:
        return self._ordered_feature_names

    @property
    def complex_feature_decoders(self) -> Dict[str, Callable]:
        """A dictionary of functions to deserialize the complex feature values returned from RonDB Server.
        Empty if there are no complex features. Using the Avro Schema to deserialize the complex feature values.
        """
        return self._complex_feature_decoders

    @property
    def transformation_fns(self) -> Dict[str, Callable]:
        """A dictionary of functions to transform the feature values returned from RonDB Server.
        Empty if there are no transformation functions. The keys are the feature names and the values are the transformation functions.
        Note that as of now the logic to build the partial transformation functions where the statistics are already pre-populated is
        implemented in the `hsfs.core.vector_server.VectorServer` class.
        """
        return self._transformation_fns

    @property
    def return_feature_value_handlers(self) -> Dict[str, Callable]:
        """A dictionary of functions to convert/deserialize/transform the feature values returned from RonDB Server."""
        return self._return_feature_value_handlers

    @property
    def online_store_rest_client_api(
        self,
    ) -> "online_store_rest_client_api.OnlineStoreRestClientApi":
        return self._online_store_rest_client_api
