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
from typings import Optional, Any, Union

from hsfs.core import rondb_rest_api


class RondbEngine:
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

    def get_raw_feature_vector_via_rest(
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

        return self._rondb_api.get_raw_feature_vector(payload=payload)

    def get_raw_feature_vectors_via_rest(
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
        if isinstance(passed_features, list) and len(passed_features) == len(
            primary_keys
        ):
            payload["passedFeatures"] = passed_features
        else:
            payload["passedFeatures"] = []

        return self._rondb_rest_api.get_rest_batch_raw_feature_vector(payload=payload)
