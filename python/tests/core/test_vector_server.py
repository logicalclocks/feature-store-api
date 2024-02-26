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

import hsfs
from hsfs import feature_view
from hsfs.core import vector_server


def init_kwargs_fixtures(fv: feature_view.FeatureView):
    return {
        "feature_store_id": fv.featurestore_id,
        "feature_view_name": fv.name,
        "feature_view_version": fv.version,
        "training_dataset_version": 1,
    }


init_prepared_statement_mock_path = (
    "hsfs.core.vector_server.VectorServer.init_prepared_statement"
)


class TestVectorServer:
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

        return feature_view.FeatureView.from_response_json(
            backend_fixtures["feature_view"]["get"]["response"]
        )

    @pytest.fixture
    def single_server(self, mocker, fv):
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        return vector_server.VectorServer(
            init_kwargs_fixtures(fv), features=fv._features
        )

    @pytest.fixture
    def batch_server(self, mocker, fv):
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        return vector_server.VectorServer(
            **init_kwargs_fixtures(fv), features=fv._features
        )

    def test_init_online_store_rest_client(
        self, mocker, monkeypatch, fv, single_server, batch_server
    ):
        # Arrange
        init_online_store_rest_client = mocker.Mock()
        monkeypatch.setattr(
            hsfs.client.online_store_rest_client,
            "init_or_reset_online_store_rest_client",
            init_online_store_rest_client,
        )

        # Act
        batch_server.init_serving(
            entity=fv,
            batch=True,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
        )
        single_server.init_serving(
            entity=fv,
            batch=False,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
        )

        # Assert
        assert batch_server._online_store_rest_client_engine is not None
        assert single_server._online_store_rest_client_engine is not None
        assert batch_server._prepared_statements is None
        assert single_server._prepared_statements is None
        assert init_online_store_rest_client.call_count == 2

    def test_default_init_serving_is_sql(self, mocker, fv, single_server, batch_server):
        # Arrange
        set_sql_client_mock = mocker.patch(init_prepared_statement_mock_path)

        # Act
        single_server.init_serving(
            entity=fv, batch=False, external=True, inference_helper_columns=True
        )
        batch_server.init_serving(
            entity=fv, batch=True, external=True, inference_helper_columns=True
        )

        # Assert
        assert single_server._online_store_rest_client_engine is None
        assert batch_server._online_store_rest_client_engine is None
        assert set_sql_client_mock.call_count == 2

    def test_init_serving_both_sql_and_rest_client(
        self, mocker, monkeypatch, fv, single_server, batch_server
    ):
        # Arrange
        set_sql_client_mock = mocker.patch(init_prepared_statement_mock_path)
        init_online_store_rest_client = mocker.Mock()
        monkeypatch.setattr(
            hsfs.client.online_store_rest_client,
            "init_or_reset_online_store_rest_client",
            init_online_store_rest_client,
        )

        # Act
        single_server.init_serving(
            entity=fv,
            batch=False,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=True,
            init_online_store_rest_client=True,
        )
        batch_server.init_serving(
            entity=fv,
            batch=True,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=True,
            init_online_store_rest_client=True,
        )

        # Assert
        assert single_server._online_store_rest_client_engine is not None
        assert batch_server._online_store_rest_client_engine is not None
        assert set_sql_client_mock.call_count == 2
        assert init_online_store_rest_client.call_count == 2

    def test_init_serving_sql_client(self, mocker, fv, single_server, batch_server):
        # Arrange
        set_sql_client_mock = mocker.patch(init_prepared_statement_mock_path)

        # Act
        single_server.init_serving(
            entity=fv,
            batch=False,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=True,
            init_online_store_rest_client=False,
        )
        batch_server.init_serving(
            entity=fv,
            batch=True,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=True,
            init_online_store_rest_client=False,
        )

        # Assert
        assert single_server._online_store_rest_client_engine is None
        assert batch_server._online_store_rest_client_engine is None
        assert set_sql_client_mock.call_count == 2

    def test_init_serving_no_client(self, fv, single_server, batch_server):
        # Arrange

        # Act
        with pytest.raises(ValueError):
            single_server.init_serving(
                entity=fv,
                batch=False,
                external=True,
                inference_helper_columns=True,
                init_online_store_sql_client=False,
                init_online_store_rest_client=False,
            )
        with pytest.raises(ValueError):
            batch_server.init_serving(
                entity=fv,
                batch=True,
                external=True,
                inference_helper_columns=True,
                init_online_store_sql_client=False,
                init_online_store_rest_client=False,
            )

    def test_rest_client_config_on_serving(
        self, mocker, monkeypatch, fv, single_server, batch_server
    ):
        # Arrange
        optional_config = {
            "timeout": 1000,
            "max_retries": 1,
            "verify": False,
            "host": "localhost",
            "port": 3434,
        }
        init_online_store_rest_client = mocker.Mock()
        monkeypatch.setattr(
            hsfs.client.online_store_rest_client,
            "init_or_reset_online_store_rest_client",
            init_online_store_rest_client,
        )

        # Act
        single_server.init_serving(
            entity=fv,
            batch=False,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
            options={"config_online_store_rest_client": optional_config},
        )
        batch_server.init_serving(
            entity=fv,
            batch=True,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
            options={"config_online_store_rest_client": optional_config},
        )

        print(init_online_store_rest_client.call_args_list)

        # Assert
        assert single_server._online_store_rest_client_engine is not None
        assert batch_server._online_store_rest_client_engine is not None
        assert init_online_store_rest_client.call_count == 2
        assert (
            init_online_store_rest_client.call_args_list[0][1]["optional_config"]
            == optional_config
        )
        assert (
            init_online_store_rest_client.call_args_list[1][1]["optional_config"]
            == optional_config
        )
        assert (
            init_online_store_rest_client.call_args_list[0][1]["reset_client"] is False
        )
        assert (
            init_online_store_rest_client.call_args_list[1][1]["reset_client"] is False
        )

    def test_reset_connection(
        self, mocker, monkeypatch, fv, single_server, batch_server
    ):
        # Arrange
        init_online_store_rest_client = mocker.Mock()
        monkeypatch.setattr(
            hsfs.client.online_store_rest_client,
            "init_or_reset_online_store_rest_client",
            init_online_store_rest_client,
        )

        # Act
        single_server.init_serving(
            entity=fv,
            batch=False,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
            options={"reset_online_store_connection": True},
        )
        batch_server.init_serving(
            entity=fv,
            batch=True,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
            options={"reset_online_store_connection": True},
        )

        # Assert
        assert single_server._online_store_rest_client_engine is not None
        assert batch_server._online_store_rest_client_engine is not None
        assert init_online_store_rest_client.call_count == 2
        assert (
            init_online_store_rest_client.call_args_list[0][1]["reset_connection"]
            is True
        )
        assert (
            init_online_store_rest_client.call_args_list[1][1]["reset_connection"]
            is True
        )

    # def test_get_feature_vector_defaults_to_initialised_client_if_rest(self, mocker, monkeypatch, fv, single_server, batch_server):
    #     # Arrange
    #     init_rond_client_mock = mocker.Mock()
    #     monkeypatch.setattr(hsfs.client.online_store_rest_client, "init_or_reset_online_store_rest_client", init_rond_client_mock)
    #     single_server.init_serving(
    #         entity=fv,
    #         batch=False,
    #         external=True,
    #         inference_helper_columns=True,
    #         init_online_store_sql_client=False,
    #         init_online_store_rest_client=True,
    #     )
    #     batch_server.init_serving(
    #         entity=fv,
    #         batch=True,
    #         external=True,
    #         inference_helper_columns=True,
    #         init_online_store_sql_client=False,
    #         init_online_store_rest_client=True,
    #     )

    #     # Act
    #     feature_vector = single_server.get_feature_vector(
    #         entry={"intt": 2},
    #         return_type="list",
    #     )
    #     feature_vectors_batch = batch_server.get_feature_vectors()

    #     # Assert

    # def test_get_feature_vector_defaults_to_initialised_client_if_sql(self, mocker, monkeypatch, fv, single_server, batch_server):
    #     # Arrange
    #     init_rond_client_mock = mocker.Mock()
    #     monkeypatch.setattr(hsfs.client.online_store_rest_client, "init_or_reset_online_store_rest_client", init_rond_client_mock)
    #     single_server.init_serving(
    #         entity=fv,
    #         batch=False,
    #         external=True,
    #         inference_helper_columns=True,
    #         init_online_store_sql_client=True,
    #         init_online_store_rest_client=True,
    #     )
    #     batch_server.init_serving(
    #         entity=fv,
    #         batch=True,
    #         external=True,
    #         inference_helper_columns=True,
    #         init_online_store_sql_client=True,
    #         init_online_store_rest_client=True,
    #     )

    #     # Assert
    #     assert single_server._online_store_rest_client_engine is not None
    #     assert batch_server._online_store_rest_client_engine is not None
    #     assert init_rond_client_mock.call_count == 2
