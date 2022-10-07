#
#   Copyright 2022 Hopsworks AB
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

from hsfs.client import exceptions
from hsfs import training_dataset, feature_group, feature_view, statistics_config
from hsfs.core import statistics_engine
from hsfs import engine

engine._engine_type = "python"
fg = feature_group.FeatureGroup(
    name="test",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    id=10,
    stream=False,
)
query = fg.select_all()


class TestStatisticsEngine:
    def test_compute_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value=None,
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=None,
            feature_view_obj=None,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 0
        assert mock_statistics_engine_save_statistics.call_count == 0
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 1

    def test_compute_statistics_get_type_spark(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value=None,
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=None,
            feature_view_obj=None,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 1
        assert mock_statistics_engine_save_statistics.call_count == 0
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 0

    def test_compute_statistics_feature_view_obj(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value=None,
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=None,
            feature_view_obj=fv,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 1
        assert mock_statistics_engine_save_statistics.call_count == 0
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 0

    def test_compute_statistics_get_type_spark_content_str(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value="test",
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=None,
            feature_view_obj=None,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 1
        assert mock_statistics_engine_save_statistics.call_count == 1
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 0

    def test_compute_statistics_feature_view_obj_content_str(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value="test",
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=None,
            feature_view_obj=fv,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 1
        assert mock_statistics_engine_save_statistics.call_count == 1
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 0

    def test_compute_statistics_get_type_spark_feature_group_commit_id(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value=None,
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=1,
            feature_view_obj=None,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 1
        assert mock_statistics_engine_save_statistics.call_count == 0
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 0

    def test_compute_statistics_feature_view_obj_feature_group_commit_id(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")
        mock_statistics_engine_profile_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_statistics",
            return_value=None,
        )
        mocker.patch("hsfs.statistics.Statistics")
        mocker.patch("hsfs.feature_group.FeatureGroup.select_all")
        mocker.patch("hsfs.feature_group.FeatureGroup.read")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        s_engine.compute_statistics(
            metadata_instance=fg,
            feature_dataframe=None,
            feature_group_commit_id=1,
            feature_view_obj=fv,
        )

        # Assert
        assert mock_statistics_engine_profile_statistics.call_count == 1
        assert mock_statistics_engine_save_statistics.call_count == 0
        assert mock_engine_get_instance.return_value.profile_by_spark.call_count == 0

    def test_profile_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_warning = mocker.patch("warnings.warn")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        feature_dataframe = mocker.Mock()
        feature_dataframe.head.return_value = []

        # Act
        s_engine.profile_statistics(
            metadata_instance=fg, feature_dataframe=feature_dataframe
        )

        # Assert
        assert mock_engine_get_instance.return_value.profile.call_count == 0
        assert mock_warning.call_count == 1
        assert (
            mock_warning.call_args[0][0]
            == "There is no data in the entity that you are trying to compute statistics "
            "for. A possible cause might be that you inserted only data to the online "
            "storage of a feature group."
        )

    def test_profile_statistics_head(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        sc = statistics_config.StatisticsConfig(
            correlations=False, histograms=False, exact_uniqueness=False, columns=[]
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
            statistics_config=sc,
        )

        feature_dataframe = mocker.Mock()
        feature_dataframe.head.return_value = [1]

        # Act
        s_engine.profile_statistics(
            metadata_instance=fg, feature_dataframe=feature_dataframe
        )

        # Assert
        assert mock_engine_get_instance.return_value.profile.call_count == 1
        assert (
            mock_engine_get_instance.return_value.profile.call_args[0][1] == sc.columns
        )
        assert (
            mock_engine_get_instance.return_value.profile.call_args[0][2]
            == sc.correlations
        )
        assert (
            mock_engine_get_instance.return_value.profile.call_args[0][3]
            == sc.histograms
        )
        assert (
            mock_engine_get_instance.return_value.profile.call_args[0][4]
            == sc.exact_uniqueness
        )

    def test_profile_transformation_fn_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_statistics_engine_profile_unique_values = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_unique_values"
        )

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        feature_dataframe = mocker.Mock()
        feature_dataframe.head.return_value = []

        # Act
        s_engine.profile_transformation_fn_statistics(
            feature_dataframe=feature_dataframe, columns=[], label_encoder_features=None
        )

        # Assert
        assert mock_engine_get_instance.return_value.profile.call_count == 1
        assert mock_statistics_engine_profile_unique_values.call_count == 1

    def test_profile_transformation_fn_statistics_get_type_python(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_statistics_engine_profile_unique_values = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_unique_values"
        )

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        feature_dataframe = mocker.Mock()
        feature_dataframe.head.return_value = []
        mock_engine_get_type.return_value = "python"

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            s_engine.profile_transformation_fn_statistics(
                feature_dataframe=feature_dataframe,
                columns=[],
                label_encoder_features=None,
            )

        # Assert
        assert mock_engine_get_instance.return_value.profile.call_count == 0
        assert mock_statistics_engine_profile_unique_values.call_count == 0
        assert (
            str(e_info.value)
            == "There is no data in the entity that you are trying to compute "
            "statistics for. A possible cause might be that you inserted only data "
            "to the online storage of a feature group."
        )

    def test_profile_transformation_fn_statistics_get_type_hive(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_statistics_engine_profile_unique_values = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_unique_values"
        )

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        feature_dataframe = mocker.Mock()
        feature_dataframe.head.return_value = []
        mock_engine_get_type.return_value = "hive"

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            s_engine.profile_transformation_fn_statistics(
                feature_dataframe=feature_dataframe,
                columns=[],
                label_encoder_features=None,
            )

        # Assert
        assert mock_engine_get_instance.return_value.profile.call_count == 0
        assert mock_statistics_engine_profile_unique_values.call_count == 0
        assert (
            str(e_info.value)
            == "There is no data in the entity that you are trying to compute "
            "statistics for. A possible cause might be that you inserted only data "
            "to the online storage of a feature group."
        )

    def test_profile_transformation_fn_statistics_get_type_spark(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_statistics_engine_profile_unique_values = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_unique_values"
        )

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        feature_dataframe = mocker.Mock()
        feature_dataframe.select.return_value.head.return_value = []
        mock_engine_get_type.return_value = "spark"

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            s_engine.profile_transformation_fn_statistics(
                feature_dataframe=feature_dataframe,
                columns=[],
                label_encoder_features=None,
            )

        # Assert
        assert mock_engine_get_instance.return_value.profile.call_count == 0
        assert mock_statistics_engine_profile_unique_values.call_count == 0
        assert (
            str(e_info.value)
            == "There is no data in the entity that you are trying to compute "
            "statistics for. A possible cause might be that you inserted only data "
            "to the online storage of a feature group."
        )

    def test_register_split_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine.profile_statistics")
        mock_split_statistics = mocker.patch("hsfs.split_statistics.SplitStatistics")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_td_read = mocker.patch("hsfs.training_dataset.TrainingDataset.read")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"split_name": "split_value"},
        )

        # Act
        s_engine.register_split_statistics(
            td_metadata_instance=td, feature_view_obj=None, feature_dataframes=None
        )

        # Assert
        assert mock_statistics_engine_save_statistics.call_count == 1
        assert mock_td_read.call_count == 1
        assert mock_split_statistics.call_args[0][0] == "split_name"

    def test_register_split_statistics_feature_dataframes(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine.profile_statistics")
        mock_split_statistics = mocker.patch("hsfs.split_statistics.SplitStatistics")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )
        mock_td_read = mocker.patch("hsfs.training_dataset.TrainingDataset.read")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"split_name": "split_value"},
        )

        # Act
        s_engine.register_split_statistics(
            td_metadata_instance=td,
            feature_view_obj=None,
            feature_dataframes={"split_name": "value"},
        )

        # Assert
        assert mock_statistics_engine_save_statistics.call_count == 1
        assert mock_td_read.call_count == 0
        assert mock_split_statistics.call_args[0][0] == "split_name"

    def test_compute_transformation_fn_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.profile_transformation_fn_statistics"
        )
        mocker.patch("hsfs.statistics.Statistics")
        mock_statistics_engine_save_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine._save_statistics"
        )

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        # Act
        s_engine.compute_transformation_fn_statistics(
            td_metadata_instance=None,
            columns=None,
            label_encoder_features=None,
            feature_dataframe=None,
            feature_view_obj=None,
        )

        # Assert
        assert mock_statistics_engine_save_statistics.call_count == 1

    def test_get_last(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_statistics_api_get_last = mocker.patch(
            "hsfs.core.statistics_api.StatisticsApi.get_last"
        )

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        # Act
        s_engine.get_last(
            metadata_instance=None,
            for_transformation=None,
            training_dataset_version=None,
        )

        # Assert
        assert mock_statistics_api_get_last.call_count == 1

    def test_get(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_statistics_api = mocker.patch("hsfs.core.statistics_api.StatisticsApi")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        # Act
        s_engine.get(
            metadata_instance=None,
            commit_time=None,
            for_transformation=None,
            training_dataset_version=None,
        )

        # Assert
        assert mock_statistics_api.return_value.get.call_count == 1

    def test_save_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mock_statistics_api = mocker.patch("hsfs.core.statistics_api.StatisticsApi")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            id=1,
        )

        # Act
        s_engine._save_statistics(
            stats=None, td_metadata_instance=td, feature_view_obj=None
        )

        # Assert
        assert mock_statistics_api.return_value.post.call_count == 1
        assert mock_statistics_api.return_value.post.call_args[0][0] == td

    def test_save_statistics_fv(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mock_statistics_api = mocker.patch("hsfs.core.statistics_api.StatisticsApi")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            id=1,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        s_engine._save_statistics(
            stats=None, td_metadata_instance=td, feature_view_obj=fv
        )

        # Assert
        assert mock_statistics_api.return_value.post.call_count == 1
        assert mock_statistics_api.return_value.post.call_args[0][0] == fv

    def test_profile_unique_values(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        mock_engine_get_instance.return_value.get_unique_values.return_value = [
            "value_1",
            "value_2",
            "value_3",
        ]

        # Act
        result = s_engine.profile_unique_values(
            feature_dataframe=None,
            label_encoder_features=["column_1", "column_2"],
            content_str="{}",
        )

        # Assert
        assert (
            result
            == '{"columns": [{"column": "column_1", "unique_values": ["value_1", "value_2", '
            '"value_3"]}, {"column": "column_2", "unique_values": ["value_1", "value_2", '
            '"value_3"]}]}'
        )

    def test_profile_unique_values_content_str(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        s_engine = statistics_engine.StatisticsEngine(feature_store_id, "featuregroup")

        mock_engine_get_instance.return_value.get_unique_values.return_value = [
            "value_1",
            "value_2",
            "value_3",
        ]

        # Act
        result = s_engine.profile_unique_values(
            feature_dataframe=None,
            label_encoder_features=["column_1", "column_2"],
            content_str='{"columns": [], "test_name": "test_value"}',
        )

        # Assert
        assert (
            result
            == '{"columns": [{"column": "column_1", "unique_values": ["value_1", "value_2", '
            '"value_3"]}, {"column": "column_2", "unique_values": ["value_1", "value_2", '
            '"value_3"]}], "test_name": "test_value"}'
        )
