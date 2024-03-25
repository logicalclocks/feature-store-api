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
import datetime
import logging
import sys

import hsfs
import numpy as np
import pandas as pd
import polars as pl
import pytest
from hsfs import feature_view
from hsfs.core import log_util, vector_server


def init_kwargs_fixtures(fv: feature_view.FeatureView):
    return {
        "feature_store_id": fv.featurestore_id,
        "feature_view_name": fv.name,
        "feature_view_version": fv.version,
        "training_dataset_version": 1,
    }


setup_online_store_sql_client_mock_path = (
    "hsfs.core.vector_server.VectorServer.setup_online_store_sql_client"
)

log_util._enable_single_class_logger(
    "hsfs.core.vector_server",
    logging.DEBUG,
    log_stream=logging.StreamHandler(sys.stdout),
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
            backend_fixtures["feature_view"][
                "get_with_complete_training_dataset_features"
            ]["response"]
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

    @pytest.fixture(scope="function")
    def base_feature_names(self):
        return ["int-like", "string-like", "datetime-like", "float-like"]

    @pytest.fixture(scope="function")
    def complete_feature_names(self):
        return [
            "int-like",
            "string-like",
            "datetime-like",
            "float-like",
            "dict-like",
            "long-like",
            "empty-like",
            "bool-like",
        ]

    @pytest.fixture(scope="function")
    def base_feature_name_with_embedding(self):
        return [
            "int-like",
            "string-like",
            "datetime-like",
            "float-like",
            "embedding-like",
        ]

    @pytest.fixture(scope="function")
    def base_feature_name_with_list(self):
        return ["int-like", "string-like", "datetime-like", "float-like", "list-like"]

    @pytest.fixture(scope="function")
    def single_base_feature_vectorz(self):
        return [1, "string", datetime.datetime.now(), 3.0]

    @pytest.fixture(scope="function")
    def single_complete_feature_vectorz(self, single_base_feature_vectorz):
        return single_base_feature_vectorz + [{"a": 1}, 1e12, None, True]

    @pytest.fixture(scope="function")
    def single_feature_vectorz_with_embedding(self, single_base_feature_vectorz):
        return single_base_feature_vectorz + [np.array([1, 2, 3])]

    @pytest.fixture(scope="function")
    def single_feature_vectorz_with_list(self, single_base_feature_vectorz):
        return single_base_feature_vectorz + [[1, 2, 3]]

    @pytest.fixture(scope="function")
    def single_feature_vectorz_partial(self, single_base_feature_vectorz):
        return [1, None, datetime.datetime.now(), 3.0, None, 1e12, None, None]

    @pytest.fixture(scope="function")
    def batch_base_feature_vectorz(self):
        return [
            [1, "string", datetime.datetime.now(), 3.0],
            [2, "string-ish", datetime.datetime.today(), 3.0],
        ]

    @pytest.fixture
    def batch_complete_feature_vectorz(self, batch_base_feature_vectorz):
        return [
            batch_base_feature_vectorz[0] + [{"a": 1}, 1e12, None, True],
            batch_base_feature_vectorz[1] + [{"b": 2}, 1e13, 2, False],
        ]

    @pytest.fixture
    def batch_feature_vectorz_with_embedding(self, batch_base_feature_vectorz):
        return [
            batch_base_feature_vectorz[0] + [np.array([1, 2, 3])],
            batch_base_feature_vectorz[1] + [np.array([4, 5, 6])],
        ]

    @pytest.fixture
    def batch_feature_vectorz_with_list(self, batch_base_feature_vectorz):
        return [
            batch_base_feature_vectorz[0] + [[1, 2, 3]],
            batch_base_feature_vectorz[1] + [[4, 5, 6]],
        ]

    @pytest.fixture
    def batch_feature_vectorz_partial(self, batch_base_feature_vectorz):
        batch_with_partials = [
            batch_base_feature_vectorz[0] + [None, 1e12, None, None],
            batch_base_feature_vectorz[1] + [None, 1e13, None, None],
        ]
        batch_with_partials[0][1] = None
        batch_with_partials[1][1] = None
        return batch_with_partials

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
        assert batch_server._online_store_sql_client is None
        assert single_server._online_store_sql_client is None
        assert init_online_store_rest_client.call_count == 2

    def test_default_init_serving_is_sql(self, mocker, fv, single_server, batch_server):
        # Arrange
        set_sql_client_mock = mocker.patch(setup_online_store_sql_client_mock_path)

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
        set_sql_client_mock = mocker.patch(setup_online_store_sql_client_mock_path)
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
        set_sql_client_mock = mocker.patch(setup_online_store_sql_client_mock_path)

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
            options={"reset_online_store_rest_client": True},
        )
        batch_server.init_serving(
            entity=fv,
            batch=True,
            external=True,
            inference_helper_columns=True,
            init_online_store_sql_client=False,
            init_online_store_rest_client=True,
            options={"reset_online_store_rest_client": True},
        )

        # Assert
        assert single_server._online_store_rest_client_engine is not None
        assert batch_server._online_store_rest_client_engine is not None
        assert init_online_store_rest_client.call_count == 2
        assert (
            init_online_store_rest_client.call_args_list[0][1]["reset_client"] is True
        )
        assert (
            init_online_store_rest_client.call_args_list[1][1]["reset_client"] is True
        )

    @pytest.mark.parametrize(
        "feature_vectorz",
        [
            "single_base_feature_vectorz",
            "single_complete_feature_vectorz",
            "single_feature_vectorz_with_embedding",
            "single_feature_vectorz_partial",
        ],
    )
    def test_handle_single_feature_vector_return_type_list(
        self, request, single_server, feature_vectorz
    ):
        # Arrange
        batch = False
        inference_helper = False
        return_type = "list"
        feature_vectorz = request.getfixturevalue(feature_vectorz)

        # Act
        result = single_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        assert result == feature_vectorz

    def test_handle_single_inference_helper_return_type_list(self, single_server):
        # Arrange
        feature_vectorz = [1, 2, 3]
        batch = False
        inference_helper = True
        return_type = "list"

        # Act
        with pytest.raises(ValueError):
            single_server.handle_feature_vector_return_type(
                feature_vectorz, batch, inference_helper, return_type
            )

    @pytest.mark.parametrize(
        "feature_vectorz",
        [
            "single_base_feature_vectorz",
            "single_complete_feature_vectorz",
            "single_feature_vectorz_partial",
            # embedding and list are not supported when converting to numpy
        ],
        ids=["base", "complete", "partial"],
    )
    def test_handle_single_feature_vector_return_type_numpy(
        self, request, single_server, feature_vectorz
    ):
        # Arrange
        batch = False
        inference_helper = False
        return_type = "numpy"
        feature_vectorz = request.getfixturevalue(feature_vectorz)

        # Act
        result = single_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        assert np.array_equal(result, np.array(feature_vectorz))

    @pytest.mark.parametrize(
        "feature_vectorz, column_names",
        [
            ("single_base_feature_vectorz", "base_feature_names"),
            ("single_complete_feature_vectorz", "complete_feature_names"),
            (
                "single_feature_vectorz_with_embedding",
                "base_feature_name_with_embedding",
            ),
            ("single_feature_vectorz_partial", "complete_feature_names"),
            ("single_feature_vectorz_with_list", "base_feature_name_with_list"),
        ],
        ids=["base", "complete", "embedding", "partial", "list"],
    )
    def test_handle_feature_vector_return_type_pandas(
        self, request, single_server, feature_vectorz, column_names
    ):
        # Arrange
        batch = False
        inference_helper = False
        return_type = "pandas"
        column_names = request.getfixturevalue(column_names)
        feature_vectorz = request.getfixturevalue(feature_vectorz)
        print(f"column_names: {column_names}, feature_vectorz: {feature_vectorz}")
        single_server._feature_vector_col_name = column_names

        # Act
        result = single_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        expected_df = pd.DataFrame(feature_vectorz).transpose()
        expected_df.columns = single_server._feature_vector_col_name
        assert result.equals(expected_df)

    @pytest.mark.parametrize(
        "feature_vectorz, column_names",
        [
            ("single_base_feature_vectorz", "base_feature_names"),
            ("single_complete_feature_vectorz", "complete_feature_names"),
            (
                "single_feature_vectorz_with_embedding",
                "base_feature_name_with_embedding",
            ),
            ("single_feature_vectorz_partial", "complete_feature_names"),
            ("single_feature_vectorz_with_list", "base_feature_name_with_list"),
        ],
        ids=["base", "complete", "embedding", "partial", "list"],
    )
    def test_handle_feature_vector_return_type_polars(
        self, request, single_server, feature_vectorz, column_names
    ):
        # Arrange
        batch = False
        inference_helper = False
        return_type = "polars"
        column_names = request.getfixturevalue(column_names)
        feature_vectorz = request.getfixturevalue(feature_vectorz)
        print(f"column_names: {column_names}, feature_vectorz: {feature_vectorz}")
        single_server._feature_vector_col_name = column_names

        # Act
        result = single_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        expected_df = pl.DataFrame(
            [feature_vectorz],
            schema=single_server._feature_vector_col_name,
            orient="row",
        )

        # To avoid not implemented error
        assert result.to_pandas().equals(expected_df.to_pandas())

    @pytest.mark.parametrize(
        "feature_vectorz, batch, inference_helper",
        [
            (single_base_feature_vectorz, False, False),
            (single_complete_feature_vectorz, True, False),
            (single_feature_vectorz_with_embedding, False, True),
            (single_feature_vectorz_partial, True, True),
        ],
    )
    def test_handle_feature_vector_return_type_unknown(
        self, single_server, feature_vectorz, batch, inference_helper
    ):
        # Arrange
        return_type = "unknown"

        # Act & Assert
        with pytest.raises(ValueError):
            single_server.handle_feature_vector_return_type(
                feature_vectorz, batch, inference_helper, return_type
            )

    @pytest.mark.parametrize(
        "feature_vectorz",
        [
            "batch_base_feature_vectorz",
            "batch_complete_feature_vectorz",
            "batch_feature_vectorz_with_embedding",
            "batch_feature_vectorz_partial",
            "batch_feature_vectorz_with_list",
        ],
        ids=["base", "complete", "embedding", "partial", "list"],
    )
    def test_handle_batch_feature_vector_return_type_list(
        self, request, batch_server, feature_vectorz
    ):
        # Arrange
        batch = True
        inference_helper = False
        return_type = "list"
        feature_vectorz = request.getfixturevalue(feature_vectorz)

        # Act
        result = batch_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        assert result == feature_vectorz

    def test_handle_batch_inference_helper_return_type_list(self, batch_server):
        # Arrange
        feature_vectorz = [[1, 2, 3], [4, 5, 6]]
        batch = True
        inference_helper = True
        return_type = "list"

        # Act
        with pytest.raises(ValueError):
            batch_server.handle_feature_vector_return_type(
                feature_vectorz, batch, inference_helper, return_type
            )

    @pytest.mark.parametrize(
        "feature_vectorz",
        [
            "batch_base_feature_vectorz",
            "batch_complete_feature_vectorz",
            "batch_feature_vectorz_partial",
            # embedding and list are not supported when converting to numpy
        ],
        ids=["base", "complete", "partial"],
    )
    def test_handle_batch_feature_vector_return_type_numpy(
        self, request, batch_server, feature_vectorz
    ):
        # Arrange
        batch = True
        inference_helper = False
        return_type = "numpy"
        feature_vectorz = request.getfixturevalue(feature_vectorz)

        # Act
        result = batch_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        assert np.array_equal(result, np.array(feature_vectorz))

    @pytest.mark.parametrize(
        "feature_vectorz, column_names",
        [
            ("batch_base_feature_vectorz", "base_feature_names"),
            ("batch_complete_feature_vectorz", "complete_feature_names"),
            (
                "batch_feature_vectorz_with_embedding",
                "base_feature_name_with_embedding",
            ),
            ("batch_feature_vectorz_partial", "complete_feature_names"),
            ("batch_feature_vectorz_with_list", "base_feature_name_with_list"),
        ],
        ids=["base", "complete", "embedding", "partial", "list"],
    )
    def test_handle_batch_feature_vector_return_type_pandas(
        self, request, batch_server, feature_vectorz, column_names
    ):
        # Arrange
        batch = True
        inference_helper = False
        return_type = "pandas"
        column_names = request.getfixturevalue(column_names)
        feature_vectorz = request.getfixturevalue(feature_vectorz)
        batch_server._feature_vector_col_name = column_names

        # Act
        result = batch_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        expected_df = pd.DataFrame(feature_vectorz)
        expected_df.columns = batch_server._feature_vector_col_name
        assert result.equals(expected_df)

    @pytest.mark.parametrize(
        "feature_vectorz, column_names",
        [
            ("batch_base_feature_vectorz", "base_feature_names"),
            ("batch_complete_feature_vectorz", "complete_feature_names"),
            (
                "batch_feature_vectorz_with_embedding",
                "base_feature_name_with_embedding",
            ),
            ("batch_feature_vectorz_partial", "complete_feature_names"),
            ("batch_feature_vectorz_with_list", "base_feature_name_with_list"),
        ],
        ids=["base", "complete", "embedding", "partial", "list"],
    )
    def test_handle_batch_feature_vector_return_type_polars(
        self, request, batch_server, feature_vectorz, column_names
    ):
        # Arrange
        batch = True
        inference_helper = False
        return_type = "polars"
        column_names = request.getfixturevalue(column_names)
        feature_vectorz = request.getfixturevalue(feature_vectorz)
        batch_server._feature_vector_col_name = column_names

        # Act
        result = batch_server.handle_feature_vector_return_type(
            feature_vectorz, batch, inference_helper, return_type
        )

        # Assert
        expected_df = pl.DataFrame(
            feature_vectorz, schema=batch_server._feature_vector_col_name
        )

        # To avoid not implemented error
        assert result.to_pandas().equals(expected_df.to_pandas())

    @pytest.mark.parametrize(
        "feature_vectorz, batch",
        [
            ("single_feature_vectorz_with_list", False),
            ("single_feature_vectorz_with_embedding", False),
            ("batch_feature_vectorz_with_list", True),
            ("batch_feature_vectorz_with_embedding", True),
        ],
    )
    def test_handle_feature_vector_return_type_numpy_with_list_or_array(
        self, request, batch_server, feature_vectorz, batch
    ):
        # Arrange
        inference_helper = False
        return_type = "numpy"
        feature_vectorz = request.getfixturevalue(feature_vectorz)

        # Act
        with pytest.raises(ValueError):
            batch_server.handle_feature_vector_return_type(
                feature_vectorz, batch, inference_helper, return_type
            )
