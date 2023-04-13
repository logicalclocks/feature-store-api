#
#   Copyright 2023 Hopsworks AB
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
import pandas as pd

from hsfs import feature_group, feature_view, training_dataset
from hsfs.constructor import fs_query
from hsfs.engine import python
from hsfs.storage_connector import HopsFSConnector


class TestArrowFlightClient:
    def _arrange_engine_mocks(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        python_engine._arrow_flight_client._is_enabled = True
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["fs_query"]["get_basic_info"]["response"]
        q = fs_query.FsQuery.from_response_json(json)
        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi.construct_query",
            return_value=q,
        )

    def _arrange_featuregroup_mocks(self, mocker, backend_fixtures):
        json = backend_fixtures["feature_group"]["get_stream_list"]["response"]
        fg_list = feature_group.FeatureGroup.from_response_json(json)
        fg = fg_list[0]
        return fg

    def _arrange_featureview_mocks(self, mocker, backend_fixtures):
        json_fv = backend_fixtures["feature_view"]["get"]["response"]
        fv = feature_view.FeatureView.from_response_json(json_fv)
        json_td = backend_fixtures["training_dataset"]["get_basic_info"]["response"]
        td = training_dataset.TrainingDataset.from_response_json(json_td)[0]
        td.training_dataset_type = "IN_MEMORY_TRAINING_DATASET"
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata",
            return_value=td,
        )

        fg = self._arrange_featuregroup_mocks(mocker, backend_fixtures)
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query",
            return_value=fg.select_all(),
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions"
        )
        mocker.patch("hsfs.engine.python.Engine._apply_transformation_function")

        # required for batch query
        batch_scoring_server = mocker.MagicMock()
        batch_scoring_server.training_dataset_version = 1
        batch_scoring_server._transformation_functions = None
        fv._batch_scoring_server = batch_scoring_server
        mocker.patch("hsfs.feature_view.FeatureView.init_batch_scoring")

        return fv

    def _arrange_dataset_reads(self, mocker, backend_fixtures, data_format):
        # required for reading tds from path
        json_td = backend_fixtures["training_dataset"]["get_basic_info"]["response"]
        td_hopsfs = training_dataset.TrainingDataset.from_response_json(json_td)[0]
        td_hopsfs.training_dataset_type = "HOPSFS_TRAINING_DATASET"
        td_hopsfs.storage_connector = HopsFSConnector(0, "", "")
        td_hopsfs.data_format = data_format
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata",
            return_value=td_hopsfs,
        )
        mocker.patch("hsfs.storage_connector.StorageConnector.refetch")
        inode_path = mocker.MagicMock()
        inode_path.path = "/path/test.parquet"
        mocker.patch(
            "hsfs.core.dataset_api.DatasetApi.list_files",
            return_value=(1, [inode_path]),
        )
        mocker.patch("hsfs.engine.python.Engine.split_labels", return_value=None)

    def test_read_feature_group(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fg = self._arrange_featuregroup_mocks(mocker, backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )

        # Act
        fg.read()

        # Assert
        assert mock_read_query.call_count == 1

    def test_read_feature_group_spark(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fg = self._arrange_featuregroup_mocks(mocker, backend_fixtures)
        mock_creat_hive_connection = mocker.patch(
            "hsfs.engine.python.Engine._create_hive_connection"
        )

        # Act
        fg.read(read_options={"use_spark": True})

        # Assert
        assert mock_creat_hive_connection.call_count == 1

    def test_read_query(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fg = self._arrange_featuregroup_mocks(mocker, backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )
        query = fg.select_all()

        # Act
        query.read()

        # Assert
        assert mock_read_query.call_count == 1

    def test_read_query_spark(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fg = self._arrange_featuregroup_mocks(mocker, backend_fixtures)
        mock_creat_hive_connection = mocker.patch(
            "hsfs.engine.python.Engine._create_hive_connection"
        )
        query = fg.select_all()

        # Act
        query.read(read_options={"use_spark": True})

        # Assert
        assert mock_creat_hive_connection.call_count == 1

    def test_training_data_featureview(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )

        # Act
        fv.training_data()

        # Assert
        assert mock_read_query.call_count == 1

    def test_training_data_featureview_spark(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        mock_creat_hive_connection = mocker.patch(
            "hsfs.engine.python.Engine._create_hive_connection"
        )

        # Act
        fv.training_data(read_options={"use_spark": True})

        # Assert
        assert mock_creat_hive_connection.call_count == 1

    def test_batch_data_featureview(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )

        # Act
        fv.get_batch_data()

        # Assert
        assert mock_read_query.call_count == 1

    def test_batch_data_featureview_spark(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        mock_creat_hive_connection = mocker.patch(
            "hsfs.engine.python.Engine._create_hive_connection"
        )

        # Act
        fv.get_batch_data(read_options={"use_spark": True})

        # Assert
        assert mock_creat_hive_connection.call_count == 1

    def test_get_training_data_featureview(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        self._arrange_dataset_reads(mocker, backend_fixtures, "parquet")
        mock_read_path = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_path",
            return_value=pd.DataFrame(),
        )

        # Act
        fv.get_training_data(1)

        # Assert
        assert mock_read_path.call_count == 1

    def test_get_training_data_featureview_spark(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        self._arrange_dataset_reads(mocker, backend_fixtures, "parquet")
        stream = mocker.MagicMock()
        stream.content = b""
        mock_read_file = mocker.patch(
            "hsfs.core.dataset_api.DatasetApi.read_content", return_value=stream
        )
        mock_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas", return_value=pd.DataFrame()
        )

        # Act
        fv.get_training_data(1, read_options={"use_spark": True})

        # Assert
        assert mock_read_file.call_count == 1
        assert mock_read_pandas.call_count == 1
