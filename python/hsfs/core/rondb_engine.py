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
from typing import Optional, Any, Union
import requests

from hsfs.core import rondb_rest_api


class RondbEngine:
    RETURN_TYPE_FEATURE_VECTOR = "feature_vector_dict"
    RETURN_TYPE_DICT_RESPONSE = "response_dict"
    RETURN_TYPE_RESPONSE = "response"

    def __init__(self):
        self._rondb_rest_api = rondb_rest_api.RondbRestApi()

    def _build_base_payload(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        metadata_options: Optional[dict[str, bool]] = None,
    ) -> dict[str, Union[str, dict[str, bool]]]:
        return {
            "featureStoreName": feature_store_name,
            "featureViewName": feature_view_name,
            "featureViewVersion": feature_view_version,
            "metadataOptions": {
                "featureName": True
                if metadata_options is None
                else metadata_options.get("featureName", True),
                "featureType": True
                if metadata_options is None
                else metadata_options.get("featureType", True),
            },
        }

    def get_single_raw_feature_vector(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        primary_keys: dict[str, Any],
        passed_features: Optional[dict[str, Any]] = None,
        metadata_options: Optional[dict[str, bool]] = None,
    ) -> dict[str, Any]:
        payload = self._build_base_payload(
            feature_store_name=feature_store_name,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            metadata_options=metadata_options,
        )
        payload["entries"] = primary_keys
        if isinstance(passed_features, dict):
            payload["passedFeatures"] = passed_features
        else:
            payload["passedFeatures"] = {}

        return self._rondb_rest_api.get_single_raw_feature_vector(payload=payload)

    def get_batch_raw_feature_vectors(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        primary_keys: list[dict[str, Any]],
        passed_features: Optional[list[dict[str, Any]]] = None,
        metadata_options: Optional[dict[str, bool]] = None,
    ):
        payload = self._build_base_payload(
            feature_store_name=feature_store_name,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            metadata_options=metadata_options,
        )
        payload["entries"] = primary_keys
        if isinstance(passed_features, list) and (
            len(passed_features) == len(primary_keys) or len(passed_features) == 0
        ):
            payload["passedFeatures"] = passed_features
        else:
            payload["passedFeatures"] = []

        return self._rondb_rest_api.get_batch_raw_feature_vectors(payload=payload)

    def convert_rdrs_response_to_dict_feature_vector(
        self, response: requests.Response
    ) -> Union[list[dict[str, Any]], dict[str, Any]]:
        assert (
            response.status_code == 200
        ), f"Invalid response status code: {response.status_code}"

        json = response.json()
        if isinstance(json["entries"], list):
            return [
                self.convert_json_entry_and_metadata_feature_vector(
                    entry=entry, metadata=metadata
                )
                for entry, metadata in zip(json["entries"], json["metadata"])
            ]
        else:
            return self.convert_json_entry_and_metadata_feature_vector(
                entry=json["entries"], metadata=json["metadata"]
            )

    def convert_json_entry_and_metadata_feature_vector(
        self, entry: list[Any], metadata: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            metadata["featureName"]: vector_value
            for vector_value, metadata in zip(entry, metadata)
        }
