#
#   Copyright 2021 Logical Clocks AB
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

from hsfs import (
    feature_view,
    transformation_function_attached,
    training_dataset,
    split_statistics,
)
from hsfs.constructor import fs_query, query
from hsfs.core import feature_view_engine


class TestFeatureViewEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_view_url = "test_url"

        mock_tf_engine = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_url = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value=feature_view_url,
        )
        mock_print = mocker.patch("builtins.print")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=["label1", "label2"],
        )

        # Act
        fv_engine.save(fv)

        # Assert
        assert len(fv._features) == 2
        assert mock_tf_engine.return_value.attach_transformation_fn.call_count == 1
        assert mock_fv_api.return_value.post.call_count == 1
        assert mock_fv_engine_get_url.call_count == 1
        assert mock_print.call_count == 1
        assert mock_print.call_args.args[
            0
        ] == "Feature view created successfully, explore it at \n{}".format(
            feature_view_url
        )

    def test_get_name(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_attached_transformation_fn = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_attached_transformation_fn"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )
        fv1 = feature_view.FeatureView(
            name="fv_name",
            version=2,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_api.return_value.get_by_name.return_value = [fv, fv1]

        # Act
        result = fv_engine.get(name="test")

        # Assert
        assert mock_fv_api.return_value.get_by_name_version.call_count == 0
        assert mock_fv_api.return_value.get_by_name.call_count == 1
        assert mock_fv_engine_get_attached_transformation_fn.call_count == 2
        assert len(result) == 2

    def test_get_name_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_attached_transformation_fn = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_attached_transformation_fn"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_api.return_value.get_by_name.return_value = fv

        # Act
        fv_engine.get(name="test", version=1)

        # Assert
        assert mock_fv_api.return_value.get_by_name_version.call_count == 1
        assert mock_fv_api.return_value.get_by_name.call_count == 0
        assert mock_fv_engine_get_attached_transformation_fn.call_count == 1

    def test_delete_name(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        result = fv_engine.delete(name="test")

        # Assert
        assert mock_fv_api.return_value.delete_by_name_version.call_count == 0
        assert mock_fv_api.return_value.delete_by_name.call_count == 1

    def test_delete_name_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.delete(name="test", version=1)

        # Assert
        assert mock_fv_api.return_value.delete_by_name_version.call_count == 1
        assert mock_fv_api.return_value.delete_by_name.call_count == 0

    def test_get_batch_query(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch("hsfs.engine.get_type")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.get_batch_query(
            feature_view_obj=fv, start_time=1, end_time=2, with_label=False
        )

        # Assert
        assert mock_fv_api.return_value.get_batch_query.call_count == 1

    def test_get_batch_query_string(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_qc_api_construct_query = mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi.construct_query"
        )
        mocker.patch("hsfs.engine.get_type")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )
        query = fs_query.FsQuery(
            query="query",
            on_demand_feature_groups=None,
            hudi_cached_feature_groups=None,
            pit_query=None,
        )
        mock_qc_api_construct_query.return_value = query

        # Act
        result = fv_engine.get_batch_query_string(
            feature_view_obj=fv, start_time=1, end_time=2
        )

        # Assert
        assert "query" == result
        assert mock_fv_api.return_value.get_batch_query.call_count == 1
        assert mock_qc_api_construct_query.call_count == 1

    def test_get_batch_query_string_pit_query(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_qc_api_construct_query = mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi.construct_query"
        )
        mocker.patch("hsfs.engine.get_type")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )
        query = fs_query.FsQuery(
            query="query",
            on_demand_feature_groups=None,
            hudi_cached_feature_groups=None,
            pit_query="pit_query",
        )

        mock_qc_api_construct_query.return_value = query

        # Act
        result = fv_engine.get_batch_query_string(
            feature_view_obj=fv, start_time=1, end_time=2
        )

        # Assert
        assert "pit_query" == result
        assert mock_fv_api.return_value.get_batch_query.call_count == 1
        assert mock_qc_api_construct_query.call_count == 1

    def test_get_attached_transformation_fn(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        def testFunction():
            print("Test")

        tf = transformation_function_attached.TransformationFunctionAttached(
            name="tf_name", transformation_function=testFunction
        )

        mock_fv_api.return_value.get_attached_transformation_fn.return_value = tf

        # Act
        result = fv_engine.get_attached_transformation_fn(name="fv_name", version=1)

        # Assert
        assert "tf_name" in result
        assert mock_fv_api.return_value.get_attached_transformation_fn.call_count == 1

    def test_get_attached_transformation_fn(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        def testFunction():
            print("Test")

        tf = transformation_function_attached.TransformationFunctionAttached(
            name="tf_name", transformation_function=testFunction
        )
        tf1 = transformation_function_attached.TransformationFunctionAttached(
            name="tf1_name", transformation_function=testFunction
        )

        mock_fv_api.return_value.get_attached_transformation_fn.return_value = [tf, tf1]

        # Act
        result = fv_engine.get_attached_transformation_fn(name="fv_name", version=1)

        # Assert
        assert "tf_name" in result
        assert "tf1_name" in result
        assert mock_fv_api.return_value.get_attached_transformation_fn.call_count == 1

    def test_create_training_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_fv_engine_compute_training_dataset = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.create_training_dataset(
            feature_view_obj=None, training_dataset_obj=None, user_write_options=None
        )

        # Assert
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_compute_training_dataset.call_count == 1

    def test_get_training_data(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = []

        # Act
        fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_engine_get_instance.return_value.read_options.call_count == 1
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 0
        assert mock_engine_get_instance.return_value.split_labels.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_td_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_get_training_data_metadata.return_value.splits = []

        # Act
        fv_engine.get_training_data(feature_view_obj=fv, training_dataset_version=1)

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 1
        assert mock_fv_engine_create_training_data_metadata.call_count == 0
        assert mock_engine_get_instance.return_value.read_options.call_count == 1
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 0
        assert mock_engine_get_instance.return_value.split_labels.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_type_in_memory(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.training_dataset_type = (
            training_dataset.TrainingDataset.IN_MEMORY
        )
        mock_fv_engine_create_training_data_metadata.return_value.IN_MEMORY = (
            training_dataset.TrainingDataset.IN_MEMORY
        )
        mock_fv_engine_create_training_data_metadata.return_value.splits = []

        # Act
        fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_engine_get_instance.return_value.read_options.call_count == 1
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 1
        assert mock_engine_get_instance.return_value.split_labels.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 1
        assert mock_fv_engine_get_batch_query.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_get_training_data_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(name="ss", content={})
        ss1 = split_statistics.SplitStatistics(name="ss1", content={})
        splits = [ss, ss1]
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        fv_engine.get_training_data(feature_view_obj=fv, splits=splits)

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_engine_get_instance.return_value.read_options.call_count == 1
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 0
        assert mock_engine_get_instance.return_value.split_labels.call_count == 2
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_check_splits_0(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(name="ss", content={})
        splits = []
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.get_training_data(feature_view_obj=fv, splits=[ss])

        # Assert
        assert (
            str(e_info.value)
            == "Incorrect `get` method is used. Use `feature_view.get_training_data` instead."
        )
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_engine_get_instance.return_value.read_options.call_count == 0
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 0
        assert mock_engine_get_instance.return_value.split_labels.call_count == 0
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_check_splits_2(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(name="ss", content={})
        ss1 = split_statistics.SplitStatistics(name="ss1", content={})
        splits = [ss, ss1]
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Incorrect `get` method is used. Use `feature_view.get_train_test_split` instead."
        )
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count
        assert mock_engine_get_instance.return_value.read_options.call_count == 0
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 0
        assert mock_engine_get_instance.return_value.split_labels.call_count == 0
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_check_splits_3(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(name="ss", content={})
        ss1 = split_statistics.SplitStatistics(name="ss1", content={})
        ss2 = split_statistics.SplitStatistics(name="ss2", content={})
        splits = [ss, ss1, ss2]
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Incorrect `get` method is used. Use `feature_view.get_train_validation_test_split` instead."
        )
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count
        assert mock_engine_get_instance.return_value.read_options.call_count == 0
        assert mock_engine_get_instance.return_value.get_training_data.call_count == 0
        assert mock_engine_get_instance.return_value.split_labels.call_count == 0
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_recreate_training_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_compute_training_dataset = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.recreate_training_dataset(
            feature_view_obj=None,
            training_dataset_version=None,
            user_write_options=None,
        )

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 1
        assert mock_fv_engine_compute_training_dataset.call_count == 1

    def test_read_from_storage_connector(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_read_dir_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_dir_from_storage_connector"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine._read_from_storage_connector(
            training_data_obj=td, splits=None, read_options=None
        )

        # Assert
        assert mock_fv_engine_read_dir_from_storage_connector.call_count == 1
        assert (
            mock_fv_engine_read_dir_from_storage_connector.call_args.args[1]
            == "location/test"
        )

    def test_read_from_storage_connector_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_read_dir_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_dir_from_storage_connector"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )
        ss = split_statistics.SplitStatistics(name="ss", content={})
        ss1 = split_statistics.SplitStatistics(name="ss1", content={})
        splits = [ss, ss1]

        # Act
        fv_engine._read_from_storage_connector(
            training_data_obj=td, splits=splits, read_options=None
        )

        # Assert
        assert mock_fv_engine_read_dir_from_storage_connector.call_count == 2
        assert (
            mock_fv_engine_read_dir_from_storage_connector.mock_calls[0].args[1]
            == "location/ss"
        )
        assert (
            mock_fv_engine_read_dir_from_storage_connector.mock_calls[1].args[1]
            == "location/ss1"
        )

    def test_read_dir_from_storage_connector(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_sc_read = mocker.patch("hsfs.storage_connector.StorageConnector.read")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine._read_dir_from_storage_connector(
            training_data_obj=td, path="test", read_options=None
        )

        # Assert
        assert mock_sc_read.call_count == 1

    def test_read_dir_from_storage_connector_file_not_found(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_sc_read = mocker.patch(
            "hsfs.storage_connector.StorageConnector.read",
            side_effect=FileNotFoundError(),
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        with pytest.raises(FileNotFoundError) as e_info:
            fv_engine._read_dir_from_storage_connector(
                training_data_obj=td, path="test", read_options=None
            )

        # Assert
        assert (
            str(e_info.value)
            == "Failed to read dataset from test. Check if path exists or recreate a training dataset."
        )
        assert mock_sc_read.call_count == 1

    def test_compute_training_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_code_engine_save_code = mocker.patch(
            "hsfs.core.code_engine.CodeEngine.save_code"
        )
        mock_td_engine_read = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine.read"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.compute_training_dataset(
                feature_view_obj=None,
                user_write_options=None,
                training_dataset_obj=None,
                training_dataset_version=None,
            )

        # Assert
        assert str(e_info.value) == "No training dataset object or version is provided"
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 0
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 0
        )
        assert mock_code_engine_save_code.call_count == 0
        assert mock_engine_get_type.call_count == 0
        assert mock_td_engine_read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_compute_training_dataset_td(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_code_engine_save_code = mocker.patch(
            "hsfs.core.code_engine.CodeEngine.save_code"
        )
        mock_td_engine_read = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine.read"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=None,
            user_write_options=None,
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 1
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 1
        )
        assert mock_code_engine_save_code.call_count == 1
        assert mock_engine_get_type.call_count == 2
        assert mock_td_engine_read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_compute_training_dataset_td_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_code_engine_save_code = mocker.patch(
            "hsfs.core.code_engine.CodeEngine.save_code"
        )
        mock_td_engine_read = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine.read"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_get_training_data_metadata.return_value = td

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=None,
            user_write_options=None,
            training_dataset_obj=None,
            training_dataset_version=1,
        )

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 1
        assert mock_fv_engine_get_batch_query.call_count == 1
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 1
        )
        assert mock_code_engine_save_code.call_count == 1
        assert mock_engine_get_type.call_count == 2
        assert mock_td_engine_read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_compute_training_dataset_td_spark_type_split(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_type = mocker.patch(
            "hsfs.engine.get_type", return_value="spark"
        )
        mock_code_engine_save_code = mocker.patch(
            "hsfs.core.code_engine.CodeEngine.save_code"
        )
        mock_td_engine_read = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine.read"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=None,
            user_write_options=None,
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 1
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 1
        )
        assert mock_code_engine_save_code.call_count == 1
        assert mock_engine_get_type.call_count == 2
        assert mock_td_engine_read.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_compute_training_dataset_td_spark_type(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_type = mocker.patch(
            "hsfs.engine.get_type", return_value="spark"
        )
        mock_code_engine_save_code = mocker.patch(
            "hsfs.core.code_engine.CodeEngine.save_code"
        )
        mock_td_engine_read = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine.read"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"1": 1, "2": 2},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=None,
            user_write_options=None,
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        # Assert
        assert mock_fv_engine_get_training_data_metadata.call_count == 0
        assert mock_fv_engine_get_batch_query.call_count == 1
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 1
        )
        assert mock_code_engine_save_code.call_count == 1
        assert mock_engine_get_type.call_count == 2
        assert mock_td_engine_read.call_count == 2
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_compute_training_dataset_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine_register_split_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.register_split_statistics"
        )
        mock_s_engine_compute_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df=None, calc_stat=False
        )

        # Assert
        assert mock_s_engine_register_split_statistics.call_count == 0
        assert mock_s_engine_compute_statistics.call_count == 0

    def test_compute_training_dataset_statistics_enabled(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine_register_split_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.register_split_statistics"
        )
        mock_s_engine_compute_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )
        td.statistics_config.enabled = True

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df=None, calc_stat=False
        )

        # Assert
        assert mock_s_engine_register_split_statistics.call_count == 0
        assert mock_s_engine_compute_statistics.call_count == 0

    def test_compute_training_dataset_statistics_enabled_calc_stat(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine_register_split_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.register_split_statistics"
        )
        mock_s_engine_compute_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )
        td.statistics_config.enabled = True

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df=None, calc_stat=True
        )

        # Assert
        assert mock_s_engine_register_split_statistics.call_count == 0
        assert mock_s_engine_compute_statistics.call_count == 1

    def test_compute_training_dataset_statistics_enabled_calc_stat_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine_register_split_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.register_split_statistics"
        )
        mock_s_engine_compute_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"1": 1, "2": 2},
        )
        td.statistics_config.enabled = True

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.compute_training_dataset_statistics(
                feature_view_obj=None,
                training_dataset_obj=td,
                td_df=None,
                calc_stat=True,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Provided dataframes should be in dict format 'split': dataframe"
        )
        assert mock_s_engine_register_split_statistics.call_count == 0
        assert mock_s_engine_compute_statistics.call_count == 0

    def test_compute_training_dataset_statistics_enabled_calc_stat(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine_register_split_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.register_split_statistics"
        )
        mock_s_engine_compute_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )
        td.statistics_config.enabled = True

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df=None, calc_stat=True
        )

        # Assert
        assert mock_s_engine_register_split_statistics.call_count == 0
        assert mock_s_engine_compute_statistics.call_count == 1

    def test_compute_training_dataset_statistics_enabled_calc_stat_splits_td_df(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine_register_split_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.register_split_statistics"
        )
        mock_s_engine_compute_statistics = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"1": 1, "2": 2},
        )
        td.statistics_config.enabled = True

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df={}, calc_stat=True
        )

        # Assert
        assert mock_s_engine_register_split_statistics.call_count == 1
        assert mock_s_engine_compute_statistics.call_count == 0

    def test_get_training_data_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )
        fv.schema = "schema"
        fv.transformation_functions = "transformation_functions"

        mock_fv_api.return_value.get_training_dataset_by_version.return_value = td

        # Act
        result = fv_engine._get_training_data_metadata(
            feature_view_obj=fv, training_dataset_version=None
        )

        # Assert
        assert mock_fv_api.return_value.get_training_dataset_by_version.call_count == 1
        assert result.schema == fv.schema
        assert result.transformation_functions == fv.transformation_functions

    def test_create_training_data_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )
        fv.schema = "schema"
        fv.transformation_functions = "transformation_functions"

        mock_fv_api.return_value.create_training_dataset.return_value = td

        # Act
        result = fv_engine._create_training_data_metadata(
            feature_view_obj=fv, training_dataset_obj=None
        )

        # Assert
        assert mock_fv_api.return_value.create_training_dataset.call_count == 1
        assert result.schema == fv.schema
        assert result.transformation_functions == fv.transformation_functions

    def test_delete_training_data(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_data(feature_view_obj=fv, training_data_version=None)

        # Assert
        assert mock_fv_api.return_value.delete_training_data_version.call_count == 0
        assert mock_fv_api.return_value.delete_training_data.call_count == 1

    def test_delete_training_data_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_data(feature_view_obj=fv, training_data_version=1)

        # Assert
        assert mock_fv_api.return_value.delete_training_data_version.call_count == 1
        assert mock_fv_api.return_value.delete_training_data.call_count == 0

    def test_delete_training_dataset_only(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_dataset_only(
            feature_view_obj=fv, training_data_version=None
        )

        # Assert
        assert (
            mock_fv_api.return_value.delete_training_dataset_only_version.call_count
            == 0
        )
        assert mock_fv_api.return_value.delete_training_dataset_only.call_count == 1

    def test_delete_training_dataset_only_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_dataset_only(
            feature_view_obj=fv, training_data_version=1
        )

        # Assert
        assert (
            mock_fv_api.return_value.delete_training_dataset_only_version.call_count
            == 1
        )
        assert mock_fv_api.return_value.delete_training_dataset_only.call_count == 0

    def test_get_batch_data(self, mocker):
        # Arrange
        feature_store_id = 99
        tf_value = "123"

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_check_feature_group_accessibility = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mock_fv_engine_get_batch_query = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query"
        )
        mock_fv_engine_get_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_data_metadata"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.get_batch_data(
            feature_view_obj=None,
            start_time=None,
            end_time=None,
            training_dataset_version=None,
            transformation_functions=tf_value,
            read_options=None,
        )

        # Assert
        assert (
            mock_engine_get_instance.return_value._apply_transformation_function.call_args.args[
                0
            ].transformation_functions
            == tf_value
        )
        assert mock_fv_engine_check_feature_group_accessibility.call_count == 1
        assert mock_fv_engine_get_batch_query.call_count == 1
        assert mock_fv_engine_get_training_data_metadata.call_count == 1
        assert mock_engine_get_instance.call_count == 1

    def test_add_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api_add = mocker.patch("hsfs.core.tags_api.TagsApi.add")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.add_tag(
            feature_view_obj=None, name=None, value=None, training_dataset_version=None
        )

        # Assert
        assert mock_tags_api_add.call_count == 1

    def test_delete_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api_delete = mocker.patch("hsfs.core.tags_api.TagsApi.delete")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.delete_tag(
            feature_view_obj=None, name=None, training_dataset_version=None
        )

        # Assert
        assert mock_tags_api_delete.call_count == 1

    def test_get_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api_get = mocker.patch("hsfs.core.tags_api.TagsApi.get")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.get_tag(
            feature_view_obj=None, name=None, training_dataset_version=None
        )

        # Assert
        assert mock_tags_api_get.call_count == 1

    def test_get_tags(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api_get = mocker.patch("hsfs.core.tags_api.TagsApi.get")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.get_tags(feature_view_obj=None, training_dataset_version=None)

        # Assert
        assert mock_tags_api_get.call_count == 1

    def test_check_feature_group_accessibility(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.from_cache_feature_group_only.return_value = False

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 2

    def test_check_feature_group_accessibility_cache_feature_group(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.from_cache_feature_group_only.return_value = True

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 2

    def test_check_feature_group_accessibility_get_type_python(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.from_cache_feature_group_only.return_value = False
        mock_engine_get_type.return_value = "python"

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Python kernel can only read from cached feature group. Please use `feature_view.create_training_data` instead."
        )
        assert mock_engine_get_type.call_count == 1

    def test_check_feature_group_accessibility_get_type_hive(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.from_cache_feature_group_only.return_value = False
        mock_engine_get_type.return_value = "hive"

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Python kernel can only read from cached feature group. Please use `feature_view.create_training_data` instead."
        )
        assert mock_engine_get_type.call_count == 2

    def test_check_feature_group_accessibility_cache_feature_group_get_type_python(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.from_cache_feature_group_only.return_value = True
        mock_engine_get_type.return_value = "python"

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 1

    def test_check_feature_group_accessibility_cache_feature_group_get_type_hive(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.from_cache_feature_group_only.return_value = True
        mock_engine_get_type.return_value = "hive"

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 2

    def test_get_feature_view_url(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hsfs.util.get_hostname_replaced_url"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query="fv_query",
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_client_get_instance.return_value._project_id = 50

        # Act
        fv_engine._get_feature_view_url(feature_view=fv)

        # Assert
        assert mock_client_get_instance.call_count == 1
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args.args[0]
            == "/p/50/fs/99/fv/fv_name/version/1"
        )
