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

import pytest
from hsfs import training_dataset_feature
from hsfs.core import online_store_rest_client_engine


ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS = "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi.get_batch_raw_feature_vectors"
ONLINE_STORE_REST_CLIENT_API_GET_SINGLE_RAW_FEATURE_VECTOR = "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi.get_single_raw_feature_vector"


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
    def training_dataset_complex_features(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_complex_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_features_mix_rondb_and_opensearch(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get_profile_fraud_tid_fg"][
            "response"
        ]
        embedded_feature_group = backend_fixtures["feature_group"]["get_embedded_fg"][
            "response"
        ]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_mix_rondb_and_opensearch_training_dataset_features"
        ]["response"]:
            if feat["featuregroup"]["name"] == feature_group["name"]:
                feat["featuregroup"] = feature_group
            else:
                feat["featuregroup"] = embedded_feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_features_composite_keys(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_composite_keys_training_dataset_features"
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
        )

    @pytest.fixture()
    def rest_client_engine_ticker(self, training_dataset_features_ticker):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_ticker,
        )

    @pytest.fixture()
    def rest_client_engine_composite_keys(
        self, training_dataset_features_composite_keys
    ):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_composite_keys,
        )

    @pytest.fixture()
    def rest_client_engine_mix_rondb_and_opensearch(
        self, training_dataset_features_mix_rondb_and_opensearch
    ):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_mix_rondb_and_opensearch,
        )

    @pytest.fixture()
    def rest_client_engine_complex_features(self, training_dataset_complex_features):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_complex_features,
        )

    def test_build_base_payload_default_options(
        self, rest_client_engine_base, backend_fixtures
    ):
        # Act
        payload = rest_client_engine_base.build_base_payload()

        # Assert
        for key, value in payload.items():
            if key != "metadataOptions" and key != "options":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )

        assert ("metadataOptions" in payload.keys()) is False
        assert ("options" in payload.keys()) is True
        assert payload["options"] == {
            "validatePassedFeatures": False,
            "includeDetailedStatus": False,
        }

    def test_build_base_payload_with_metadata_and_options(
        self,
        rest_client_engine_base,
    ):
        # Act
        payload = rest_client_engine_base.build_base_payload(
            metadata_options={"featureName": True, "featureType": False},
            validate_passed_features=True,  # not default
            include_detailed_status=True,  # not default
        )

        # Assert
        assert payload["metadataOptions"]["featureName"] is True
        assert payload["metadataOptions"]["featureType"] is False
        assert payload["options"] == {
            "validatePassedFeatures": True,
            "includeDetailedStatus": True,
        }

    @pytest.mark.parametrize("drop_missing", [True, False])
    def test_convert_rdrs_response_to_feature_vector_if_null(
        self,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
        drop_missing: bool,
    ):
        # Act
        feature_vector_dict = rest_client_engine_ticker.convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=drop_missing,
        )
        feature_vector_list = rest_client_engine_ticker.convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=drop_missing,
        )

        # Assert
        if drop_missing:
            assert feature_vector_dict == {}
            assert feature_vector_list == []
        else:
            assert feature_vector_dict == {
                "ticker": None,
                "when": None,
                "price": None,
                "volume": None,
            }
            assert feature_vector_list == [None, None, None, None]

    @pytest.mark.parametrize(
        "fixture_key",
        [
            "get_single_vector_response_json_complete",
            "get_single_vector_response_json_complete_no_metadata",
        ],
    )
    def test_convert_rdrs_response_to_feature_vector_row_single_complete_response(
        self,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
        fixture_key,
    ):
        # Arrange
        response = backend_fixtures["rondb_server"][fixture_key]
        reference_feature_vector = {
            "ticker": "APPL",
            "when": "2022-01-01 00:00:00",
            "price": 21.3,
            "volume": 10,
        }

        # Act
        feature_vector_dict = rest_client_engine_ticker.convert_rdrs_response_to_feature_value_row(
            row_feature_values=response["features"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )

        # Assert
        assert feature_vector_dict == reference_feature_vector

    def test_get_batch_feature_vectors_response_json(
        self,
        mocker,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()

        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ],
        )

        # Act
        response_json = rest_client_engine_ticker.get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_RESPONSE_JSON,
            drop_missing=False,
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
    def test_get_batch_feature_vectors_as_dict(
        self,
        mocker,
        backend_fixtures,
        fixture_key,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][fixture_key],
        )

        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": "2022-01-01 00:00:00",
                "price": 21.3,
                "volume": 10,
            },
            {
                "ticker": "GOOG",
                "when": "2022-01-01 00:00:00",
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rest_client_engine_ticker.get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )

        # Assert
        assert feature_vector_dict == reference_batch_vectors
        assert mock_online_rest_api.called_once_with(payload=payload)

    def test_get_batch_feature_partial_pk_missing_vectors_as_dict(
        self,
        mocker,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_pk_value_no_match"
            ],
        )

        reference_batch_vectors = [
            {},
            {
                "ticker": "GOOG",
                "when": "2022-01-01 00:00:00",
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rest_client_engine_ticker.get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=True,
        )

        # Assert
        assert feature_vector_dict == reference_batch_vectors
        assert mock_online_rest_api.called_once_with(payload=payload)

    def test_get_batch_feature_partial_error(
        self, mocker, backend_fixtures, rest_client_engine_ticker
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_error"
            ],
        )
        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": "2022-01-01 00:00:00",
                "price": 21.3,
                "volume": 10,
            },
            {},
            {
                "ticker": "GOOG",
                "when": "2022-01-01 00:00:00",
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        batch_vectors = rest_client_engine_ticker.get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=True,
        )

        # Assert
        assert batch_vectors == reference_batch_vectors
        assert mock_online_rest_api.called_once_with(payload=payload)
