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
import humps
import pytest
from datetime import datetime

from hsfs.core import rondb_engine

RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS = (
    "hsfs.core.rondb_rest_api.RondbRestApi.get_batch_raw_feature_vectors"
)


class TestRondbEngine:
    @pytest.mark.parametrize(
        "keys", [["featureName", "featureType"], ["featureType", "featureName"]]
    )
    def test_build_base_payload_response_json(self, keys, backend_fixtures):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        kwargs = {
            "feature_store_name": "test_store_featurestore",
            "feature_view_name": "test_feature_view",
            "feature_view_version": 2,
            "return_type": rondb_engine.RondbEngine.RETURN_TYPE_RESPONSE_JSON,
        }

        # Act
        kwargs["metadata_options"] = {keys[0]: False}
        payload = rondb_engine_instance._build_base_payload(**kwargs)

        # Assert
        for (key, value) in payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )

        # featureName/featureType is missing, insert key with default value True
        assert payload["metadataOptions"][keys[1]] is True
        # does not enforce featureType/featureName to be true if return type is response json
        assert payload["metadataOptions"][keys[0]] is False

    @pytest.mark.parametrize(
        "metadata_options", [{"featureType": False, "featureName": False}, {}]
    )
    def test_build_base_payload_feature_vector(
        self, metadata_options, backend_fixtures
    ):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        kwargs = {
            "feature_store_name": "test_store_featurestore",
            "feature_view_name": "test_feature_view",
            "feature_view_version": 2,
            "return_type": rondb_engine.RondbEngine.RETURN_TYPE_FEATURE_VECTOR,
        }

        # Act
        payload = rondb_engine_instance._build_base_payload(
            **kwargs, metadata_options=metadata_options
        )

        # Assert
        for (key, value) in payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )
        # enforce featureType/featureName to be true if return type is feature vector to enable conversion
        assert payload["metadataOptions"]["featureType"] is True
        assert payload["metadataOptions"]["featureName"] is True

    def test_convert_rdrs_response_to_feature_vector_dict_single_complete_response(
        self, backend_fixtures
    ):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        response = backend_fixtures["rondb_server"][
            "get_single_vector_response_json_complete"
        ]
        reference_feature_vector = {
            "ticker": "APPL",
            "when": datetime.strptime(
                "2022-01-01 00:00:00", rondb_engine_instance.SQL_TIMESTAMP_STRING_FORMAT
            ),
            "price": 21.3,
            "volume": 10,
        }

        # Act
        feature_vector_dict = (
            rondb_engine_instance.convert_rdrs_response_to_dict_feature_vector(
                row_feature_values=response["features"], metadatas=response["metadata"]
            )
        )

        # Assert
        assert feature_vector_dict == reference_feature_vector

    @pytest.mark.skip(
        reason="Unclear what is desired behaviour. Should we raise an error?"
    )
    def test_convert_rdrs_response_to_feature_vector_dict_pk_value_no_match(
        self, backend_fixtures
    ):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        response = backend_fixtures["rondb_server"][
            "get_single_vector_response_json_pk_value_no_match"
        ]
        reference_feature_vector = {
            "ticker": "APPL",
            "when": None,
            "price": None,
            "volume": None,
        }

        # Act
        feature_vector_dict = (
            rondb_engine_instance.convert_rdrs_response_to_dict_feature_vector(
                row_feature_values=response["features"], metadatas=response["metadata"]
            )
        )

        # Assert
        assert feature_vector_dict == reference_feature_vector

    @pytest.mark.parametrize("passed_features", [{"price": 12.4}, {}])
    def test_get_single_raw_feature_vector_response_json(
        self, mocker, passed_features, backend_fixtures
    ):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        payload = backend_fixtures["rondb_server"]["get_single_vector_payload"].copy()
        payload["passed_features"] = passed_features

        mock_rondb_api = mocker.patch(
            "hsfs.core.rondb_rest_api.RondbRestApi.get_single_raw_feature_vector",
            return_value=backend_fixtures["rondb_server"][
                "get_single_vector_response_json_complete"
            ],
        )

        # Act
        response_json = rondb_engine_instance.get_single_raw_feature_vector(
            **humps.decamelize(payload),
            return_type=rondb_engine.RondbEngine.RETURN_TYPE_RESPONSE_JSON,
        )

        # Assert
        # Check that the response was not converted to a feature vector if return_type is response json
        assert (
            response_json
            == backend_fixtures["rondb_server"][
                "get_single_vector_response_json_complete"
            ]
        )
        assert mock_rondb_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_vectors_response_json(
        self, mocker, backend_fixtures
    ):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()

        mock_rondb_api = mocker.patch(
            RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ],
        )

        # Act
        response_json = rondb_engine_instance.get_batch_raw_feature_vectors(
            **humps.decamelize(payload),
            return_type=rondb_engine.RondbEngine.RETURN_TYPE_RESPONSE_JSON,
        )

        # Assert
        # Check that the response was not converted to a feature vector if return_type is response json
        assert (
            response_json
            == backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ]
        )
        assert mock_rondb_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_vectors_as_dict(self, mocker, backend_fixtures):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_rondb_api = mocker.patch(
            RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ],
        )

        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rondb_engine_instance.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 21.3,
                "volume": 10,
            },
            {
                "ticker": "GOOG",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rondb_engine_instance.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rondb_engine_instance.get_batch_raw_feature_vectors(
            **humps.decamelize(payload),
            return_type=rondb_engine.RondbEngine.RETURN_TYPE_FEATURE_VECTOR,
        )

        # Assert
        assert feature_vector_dict == reference_batch_vectors
        assert mock_rondb_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_partial_pk_missing_vectors_as_dict(
        self, mocker, backend_fixtures
    ):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_rondb_api = mocker.patch(
            RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_pk_value_no_match"
            ],
        )

        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": None,
                "price": None,
                "volume": None,
            },
            {
                "ticker": "GOOG",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rondb_engine_instance.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rondb_engine_instance.get_batch_raw_feature_vectors(
            **humps.decamelize(payload),
            return_type=rondb_engine.RondbEngine.RETURN_TYPE_FEATURE_VECTOR,
        )

        # Assert
        assert feature_vector_dict == reference_batch_vectors
        assert mock_rondb_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_partial_error(self, mocker, backend_fixtures):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_rondb_api = mocker.patch(
            "hsfs.core.rondb_rest_api.RondbRestApi.get_batch_raw_feature_vectors",
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_error"
            ],
        )
        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rondb_engine_instance.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 21.3,
                "volume": 10,
            },
            {
                "ticker": "GOOG",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rondb_engine_instance.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        batch_vectors = rondb_engine_instance.get_batch_raw_feature_vectors(
            **humps.decamelize(payload),
            return_type=rondb_engine.RondbEngine.RETURN_TYPE_FEATURE_VECTOR,
        )

        # Assert
        assert batch_vectors == reference_batch_vectors
        assert mock_rondb_api.called_once_with(payload=payload)
