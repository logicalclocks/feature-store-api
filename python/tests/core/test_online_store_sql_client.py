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

import hsfs
import hsfs.constructor
import hsfs.constructor.serving_prepared_statement
import pytest


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
    def online_store_sql_client(self, mocker):
        feature_store_id = 1
        skip_fg_ids = set()
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

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
        online_store_sql_client._prepared_statements = hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
            json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                "response"
            ]
        )

        # Act
        online_store_sql_client.init_parametrize_and_serving_utils()

        # Assert
        assert isinstance(online_store_sql_client.serving_keys, set)
        assert len(online_store_sql_client.serving_keys) == 1

    def test_init_serving_feature_view_no_helper_columns(
        self, monkeypatch, backend_fixtures, online_store_sql_client, fv
    ):
        # Arrange
        monkeypatch.setattr(
            hsfs.core.feature_view_api.FeatureViewApi,
            "get_serving_prepared_statements",
            hsfs.constructor.serving_prepared_statement.ServingPreparedStatement.from_response_json(
                json_dict=backend_fixtures["serving_prepared_statement"]["get_list"][
                    "response"
                ]
            ),
        )

        # Act
        online_store_sql_client.init_prepared_statement(
            entity=fv,
            external=True,
            inference_helper_columns=False,
        )

        # Assert
        assert isinstance(online_store_sql_client._prepared_statements, dict)
        assert len(online_store_sql_client._prepared_statements) == 1
        assert online_store_sql_client._helper_column_prepared_statement is None
