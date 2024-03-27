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
import sys

import hsfs
import hsfs.constructor
import hsfs.constructor.serving_prepared_statement
import pytest


SINGLE_VECTOR_KEY = (
    hsfs.core.online_store_sql_client.OnlineStoreSqlClient.SINGLE_VECTOR_KEY
)
BATCH_VECTOR_KEY = (
    hsfs.core.online_store_sql_client.OnlineStoreSqlClient.BATCH_VECTOR_KEY
)
SINGLE_HELPER_KEY = (
    hsfs.core.online_store_sql_client.OnlineStoreSqlClient.SINGLE_HELPER_KEY
)
BATCH_HELPER_KEY = (
    hsfs.core.online_store_sql_client.OnlineStoreSqlClient.BATCH_HELPER_KEY
)
MOCK_API_ATTRIBUTE_GET_SERVING_PREPARED_STATEMENT = "get_serving_prepared_statement"


_logger = logging.getLogger("hsfs.core.online_store_sql_client")
_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
_logger.addHandler(handler)


class TestOnlineStoreSqlClient:
    @pytest.fixture
    def fv(self, mocker, backend_fixtures):
        mocker.patch.object(
            hsfs.feature_store.FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi.get")

        return hsfs.feature_view.FeatureView.from_response_json(
            backend_fixtures["feature_view"][
                "get_with_complete_training_dataset_features"
            ]["response"]
        )

    @pytest.fixture
    def training_dataset(self, backend_fixtures):
        return hsfs.training_dataset.TrainingDataset.from_response_json(
            backend_fixtures["training_dataset"]["get"]["response"][0]
        )

    @pytest.fixture(scope="function")
    def online_store_sql_client(self, mocker):
        feature_store_id = 1
        skip_fg_ids = set()
        mocker.patch("hsfs.client.get_instance")

        return hsfs.core.online_store_sql_client.OnlineStoreSqlClient(
            feature_store_id=feature_store_id, skip_fg_ids=skip_fg_ids
        )

    @pytest.mark.parametrize("skip_fg_ids", [None, set(), set([0])])
    def test_init(self, mocker, skip_fg_ids):
        # Arrange
        feature_store_id = 1
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        # Act
        online_store_sql_client = (
            hsfs.core.online_store_sql_client.OnlineStoreSqlClient(
                feature_store_id=feature_store_id,
                skip_fg_ids=skip_fg_ids,
            )
        )

        # Assert
        assert online_store_sql_client._feature_store_id == feature_store_id
        if skip_fg_ids is None:
            assert online_store_sql_client._skip_fg_ids == set()
        else:
            assert online_store_sql_client._skip_fg_ids == skip_fg_ids

    def test_init_parametrize_and_serving_utils(
        self, backend_fixtures, online_store_sql_client
    ):
        # Arrange
        prepared_statements = hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
            json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                "response"
            ]
        )

        # Act
        online_store_sql_client.init_parametrize_and_serving_utils(prepared_statements)

        # Assert
        assert isinstance(online_store_sql_client.serving_keys, set)
        assert len(online_store_sql_client.serving_keys) == 1

    def test_fetch_prepared_statements_feature_view_no_helper_columns(
        self, mocker, backend_fixtures, online_store_sql_client, fv
    ):
        # Arrange
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi.get_serving_prepared_statement",
            return_value=hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
                json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                    "response"
                ]
            ),
        )

        # Act
        online_store_sql_client.fetch_prepared_statements(
            entity=fv,
            inference_helper_columns=False,
        )

        # Assert
        serving_statements = online_store_sql_client.prepared_statements
        assert isinstance(online_store_sql_client._prepared_statements, dict)
        assert len(serving_statements) == 2
        assert len(serving_statements[SINGLE_VECTOR_KEY]) == 1
        assert len(serving_statements[BATCH_VECTOR_KEY]) == 1

    def test_fetch_prepared_statements_feature_view_with_helper_columns(
        self, mocker, backend_fixtures, online_store_sql_client, fv
    ):
        # Arrange
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi.get_serving_prepared_statement",
            return_value=hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
                json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                    "response"
                ]
            ),
        )

        # Act
        online_store_sql_client.fetch_prepared_statements(
            entity=fv,
            inference_helper_columns=True,
        )

        # Assert
        serving_statements = online_store_sql_client.prepared_statements
        assert isinstance(online_store_sql_client.prepared_statements, dict)
        assert len(serving_statements) == 4
        assert len(serving_statements[SINGLE_VECTOR_KEY]) == 1
        assert len(serving_statements[BATCH_VECTOR_KEY]) == 1
        assert len(serving_statements[SINGLE_HELPER_KEY]) == 1
        assert len(serving_statements[BATCH_HELPER_KEY]) == 1

    def test_fetch_prepared_statements_training_dataset(
        self, mocker, backend_fixtures, online_store_sql_client, training_dataset
    ):
        # Arrange
        mocker.patch(
            "hsfs.core.training_dataset_api.TrainingDatasetApi.get_serving_prepared_statement",
            return_value=hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
                json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                    "response"
                ]
            ),
        )

        # Act
        online_store_sql_client.fetch_prepared_statements(
            entity=training_dataset,
            inference_helper_columns=False,
        )

        # Assert
        serving_statements = online_store_sql_client.prepared_statements
        assert isinstance(online_store_sql_client.prepared_statements, dict)
        assert len(serving_statements) == 2
        assert len(serving_statements[SINGLE_VECTOR_KEY]) == 1
        assert len(serving_statements[BATCH_VECTOR_KEY]) == 1

    def test_init_prepared_statements(
        self, mocker, backend_fixtures, fv, online_store_sql_client
    ):
        # Arrange
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi.get_serving_prepared_statement",
            return_value=hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
                json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                    "response"
                ]
            ),
        )

        # Act
        online_store_sql_client.init_prepared_statements(
            entity=fv, external=True, inference_helper_columns=False
        )

        # Assert
        serving_statements = online_store_sql_client.prepared_statements
        assert isinstance(serving_statements, dict)
        assert (
            len(serving_statements) == 2
            and len(serving_statements[SINGLE_VECTOR_KEY]) == 1
        )

        parametrised_statements = online_store_sql_client.parametrised_statements
        assert isinstance(parametrised_statements, dict)
        assert (
            len(parametrised_statements) == 2
            and len(parametrised_statements[SINGLE_VECTOR_KEY]) == 1
            and len(parametrised_statements[BATCH_VECTOR_KEY]) == 1
        )
