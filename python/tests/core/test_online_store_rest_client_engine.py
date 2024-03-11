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
from datetime import datetime

import pytest
from hsfs import training_dataset_feature
from hsfs.core import online_store_rest_client_engine
from hsfs.util import convert_event_time_to_timestamp


RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS = "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi.get_batch_raw_feature_vectors"


class TestOnlineRestClientEngine:
    @pytest.fixture()
    def training_dataset_features_online(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_fraud_online_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_features_ticker(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_ticker_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def rest_client_engine_base(self):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=[],
            skip_fg_ids=[],
        )

    @pytest.fixture()
    def rest_client_engine_ticker(self, training_dataset_features_ticker):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_ticker,
            skip_fg_ids=[],
        )

    def test_build_base_payload_no_metadata_options(
        self, rest_client_engine_base, backend_fixtures
    ):
        # Act
        payload = rest_client_engine_base.build_base_payload()

        # Assert
        for key, value in payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )

        assert ("metadataOptions" in payload.keys()) is False

    @pytest.mark.parametrize(
        "keys", [["featureName", "featureType"], ["featureType", "featureName"]]
    )
    def test_build_base_payload_with_metadata_options(
        self, keys, rest_client_engine_base, backend_fixtures
    ):
        # Act
        payload = rest_client_engine_base.build_base_payload(
            metadata_options={keys[0]: True, keys[1]: False}
        )

        # Assert
        for key, value in payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )

        assert payload["metadataOptions"][keys[1]] is False
        assert payload["metadataOptions"][keys[0]] is True

    @pytest.mark.parametrize(
        "fixture_key",
        [
            "get_single_vector_response_json_complete",
            "get_single_vector_response_json_complete_int_timestamp",
            "get_single_vector_response_json_complete_no_metadata",
        ],
    )
    def test_convert_rdrs_response_to_feature_vector_row_single_complete_response(
        self, backend_fixtures, rest_client_engine_ticker, fixture_key
    ):
        # Arrange
        if "int_timestamp" in fixture_key:
            response = backend_fixtures["rondb_server"][
                fixture_key.replace("_int_timestamp", "")
            ]
            response["features"][1] = convert_event_time_to_timestamp(
                "2022-01-01 00:00:00"
            )
        else:
            response = backend_fixtures["rondb_server"][fixture_key]
        reference_feature_vector = {
            "ticker": "APPL",
            "when": datetime.strptime(
                "2022-01-01 00:00:00",
                rest_client_engine_ticker.SQL_TIMESTAMP_STRING_FORMAT,
            ),
            "price": 21.3,
            "volume": 10,
        }

        # Act
        feature_vector_dict = rest_client_engine_ticker.convert_rdrs_response_to_feature_value_row(
            row_feature_values=response["features"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )

        # Assert
        assert feature_vector_dict == reference_feature_vector

    @pytest.mark.parametrize(
        "passed_features, fixture_key",
        [
            (
                {"price": 12.4},
                "get_single_vector_response_json_pk_value_no_match_with_passed_price",
            ),
            ({}, "get_single_vector_response_json_pk_value_no_match"),
        ],
    )
    def test_get_single_raw_feature_vector_pk_value_no_match(
        self,
        mocker,
        passed_features,
        fixture_key,
        backend_fixtures,
        rest_client_engine_ticker,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_single_vector_payload"].copy()
        payload["passed_features"] = passed_features

        mock_online_rest_api = mocker.patch(
            "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi.get_single_raw_feature_vector",
            return_value=backend_fixtures["rondb_server"][fixture_key],
        )
        reference_vector = payload["entry"]
        reference_vector.update({"when": None, "price": None, "volume": None})
        reference_vector.update(passed_features)

        # Act
        response_json = rest_client_engine_ticker.get_single_raw_feature_vector(
            entry=payload["entry"],
            passed_features=passed_features,
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )

        # Assert
        # Check that the response was not converted to a feature vector if return_type is response json
        print("resp :", response_json)
        print("ref :", reference_vector)
        assert response_json == reference_vector
        assert mock_online_rest_api.called_once_with(payload=payload)

    @pytest.mark.parametrize("passed_features", [{"price": 12.4}, {}])
    def test_get_single_raw_feature_vector_response_json(
        self, mocker, passed_features, backend_fixtures, rest_client_engine_ticker
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_single_vector_payload"].copy()
        payload["passed_features"] = passed_features

        mock_online_rest_api = mocker.patch(
            "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi.get_single_raw_feature_vector",
            return_value=backend_fixtures["rondb_server"][
                "get_single_vector_response_json_complete"
            ],
        )
        # Act
        response_json = rest_client_engine_ticker.get_single_raw_feature_vector(
            entry=payload["entry"],
            passed_features=passed_features,
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_RESPONSE_JSON,
        )

        # Assert
        # Check that the response was not converted to a feature vector if return_type is response json
        assert (
            response_json
            == backend_fixtures["rondb_server"][
                "get_single_vector_response_json_complete"
            ]
        )
        assert mock_online_rest_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_vectors_response_json(
        self,
        mocker,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()

        mock_online_rest_api = mocker.patch(
            RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ],
        )

        # Act
        response_json = rest_client_engine_ticker.get_batch_raw_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_RESPONSE_JSON,
        )

        # Assert
        # Check that the response was not converted to a feature vector if return_type is response json
        assert (
            response_json
            == backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ]
        )
        assert mock_online_rest_api.called_once_with(payload=payload)

    @pytest.mark.parametrize(
        "fixture_key",
        [
            "get_batch_vector_response_json_complete",
            "get_batch_vector_response_json_complete_no_metadata",
        ],
    )
    def test_get_batch_raw_feature_vectors_as_dict(
        self,
        mocker,
        backend_fixtures,
        fixture_key,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_online_rest_api = mocker.patch(
            RONDB_REST_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][fixture_key],
        )

        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rest_client_engine_ticker.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 21.3,
                "volume": 10,
            },
            {
                "ticker": "GOOG",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rest_client_engine_ticker.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rest_client_engine_ticker.get_batch_raw_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )

        # Assert
        assert feature_vector_dict == reference_batch_vectors
        assert mock_online_rest_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_partial_pk_missing_vectors_as_dict(
        self,
        mocker,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_online_rest_api = mocker.patch(
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
                    rest_client_engine_ticker.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rest_client_engine_ticker.get_batch_raw_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )

        # Assert
        assert feature_vector_dict == reference_batch_vectors
        assert mock_online_rest_api.called_once_with(payload=payload)

    def test_get_batch_raw_feature_partial_error(
        self, mocker, backend_fixtures, rest_client_engine_ticker
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_online_rest_api = mocker.patch(
            "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi.get_batch_raw_feature_vectors",
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_error"
            ],
        )
        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rest_client_engine_ticker.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 21.3,
                "volume": 10,
            },
            {
                "ticker": None,
                "when": None,
                "price": None,
                "volume": None,
            },
            {
                "ticker": "GOOG",
                "when": datetime.strptime(
                    "2022-01-01 00:00:00",
                    rest_client_engine_ticker.SQL_TIMESTAMP_STRING_FORMAT,
                ),
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        batch_vectors = rest_client_engine_ticker.get_batch_raw_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )

        # Assert
        assert batch_vectors == reference_batch_vectors
        assert mock_online_rest_api.called_once_with(payload=payload)
