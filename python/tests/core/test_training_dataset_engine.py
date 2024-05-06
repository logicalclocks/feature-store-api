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
from hsfs import (
    feature_group,
    training_dataset,
    training_dataset_feature,
    transformation_function,
)
from hsfs.constructor import query
from hsfs.core import training_dataset_engine
from hsfs.hopsworks_udf import hopsworks_udf


class TestTrainingDatasetEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            label=["f", "f_wrong"],
        )

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        f = training_dataset_feature.TrainingDatasetFeature(
            name="f", type="str", label=False
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="f1", type="int", label=False
        )

        features = [f, f1]

        mock_engine_get_instance.return_value.parse_schema_training_dataset.return_value = features

        # Act
        td_engine.save(training_dataset=td, features=None, user_write_options=None)

        # Assert
        assert mock_td_api.return_value.post.call_count == 1
        assert len(td._features) == 2
        assert td._features[0].label is True
        assert td._features[1].label is False

    def test_save_query(self, mocker, backend_fixtures):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            label=["f", "f_wrong"],
        )

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(left_feature_group=fg, left_features=fg.features)

        # Act
        td_engine.save(training_dataset=td, features=q, user_write_options=None)

        # Assert
        assert mock_td_api.return_value.post.call_count == 1
        assert len(td._features) == 2
        assert td._features[0].label is True
        assert td._features[1].label is True

    def test_save_transformation_functions(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        @hopsworks_udf(int)
        def plus_one(a):
            return a + 1

        tf = transformation_function.TransformationFunction(
            hopsworks_udf=plus_one, featurestore_id=99
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            label=["f", "f_wrong"],
            transformation_functions=tf,
        )

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        f = training_dataset_feature.TrainingDatasetFeature(
            name="f", type="str", label=False
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="f1", type="int", label=False
        )

        features = [f, f1]

        mock_engine_get_instance.return_value.parse_schema_training_dataset.return_value = features

        # Act
        with pytest.raises(ValueError) as e_info:
            td_engine.save(training_dataset=td, features=None, user_write_options=None)

        # Assert
        assert mock_td_api.return_value.post.call_count == 0
        assert len(td._features) == 2
        assert td._features[0].label is True
        assert td._features[1].label is False
        assert (
            str(e_info.value)
            == "Transformation functions can only be applied to training datasets generated from Query object"
        )

    def test_save_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mock_warning = mocker.patch("warnings.warn")

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={"name": "value"},
            label=["f", "f_wrong"],
        )

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        f = training_dataset_feature.TrainingDatasetFeature(
            name="f", type="str", label=False
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="f1", type="int", label=False
        )

        features = [f, f1]

        mock_engine_get_instance.return_value.parse_schema_training_dataset.return_value = features

        # Act
        td_engine.save(training_dataset=td, features=None, user_write_options=None)

        # Assert
        assert mock_td_api.return_value.post.call_count == 1
        assert len(td._features) == 2
        assert td._features[0].label is True
        assert td._features[1].label is False
        assert td.train_split == "train"
        assert (
            mock_warning.call_args[0][0]
            == "Training dataset splits were defined but no `train_split` (the name of the split that is going to be "
            "used for training) was provided. Setting this property to `train`. The statistics of this "
            "split will be used for transformation functions."
        )

    def test_insert(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.insert(
            training_dataset=None,
            dataset=None,
            user_write_options=None,
            overwrite=False,
        )

        # Assert
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 1
        )
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_args[0][3]
            == td_engine.APPEND
        )

    def test_insert_overwrite(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.insert(
            training_dataset=None, dataset=None, user_write_options=None, overwrite=True
        )

        # Assert
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_count == 1
        )
        assert (
            mock_engine_get_instance.return_value.write_training_dataset.call_args[0][3]
            == td_engine.OVERWRITE
        )

    def test_read(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_instance")
        mock_storage_connector_read = mocker.patch(
            "hsfs.storage_connector.StorageConnector.read"
        )

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            location="td_location",
            splits={},
            label=[],
        )

        # Act
        td_engine.read(training_dataset=td, split=None, user_read_options=None)

        # Assert
        assert mock_storage_connector_read.call_count == 1
        assert mock_storage_connector_read.call_args[1]["path"] == "td_location/test"

    def test_read_split(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_instance")
        mock_storage_connector_read = mocker.patch(
            "hsfs.storage_connector.StorageConnector.read"
        )

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            location="td_location",
            splits={},
            label=[],
        )

        # Act
        td_engine.read(training_dataset=td, split="split", user_read_options=None)

        # Assert
        assert mock_storage_connector_read.call_count == 1
        assert mock_storage_connector_read.call_args[1]["path"] == "td_location/split"

    def test_query(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        mock_td_api.return_value.get_query.return_value.pit_query = None

        # Act
        result = td_engine.query(
            training_dataset=None, online=None, with_label=None, is_hive_query=None
        )

        # Assert
        assert mock_td_api.return_value.get_query.call_count == 1
        assert (
            mock_td_api.return_value.get_query.return_value.register_external.call_count
            == 1
        )
        assert (
            mock_td_api.return_value.get_query.return_value.register_hudi_tables.call_count
            == 1
        )
        assert result == mock_td_api.return_value.get_query.return_value.query

    def test_query_pit(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        result = td_engine.query(
            training_dataset=None, online=None, with_label=None, is_hive_query=None
        )

        # Assert
        assert mock_td_api.return_value.get_query.call_count == 1
        assert (
            mock_td_api.return_value.get_query.return_value.register_external.call_count
            == 1
        )
        assert (
            mock_td_api.return_value.get_query.return_value.register_hudi_tables.call_count
            == 1
        )
        assert result == mock_td_api.return_value.get_query.return_value.pit_query

    def test_query_online(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        result = td_engine.query(
            training_dataset=None, online=True, with_label=None, is_hive_query=None
        )

        # Assert
        assert mock_td_api.return_value.get_query.call_count == 1
        assert (
            mock_td_api.return_value.get_query.return_value.register_external.call_count
            == 0
        )
        assert (
            mock_td_api.return_value.get_query.return_value.register_hudi_tables.call_count
            == 0
        )
        assert result == mock_td_api.return_value.get_query.return_value.query_online

    def test_add_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.add_tag(training_dataset=None, name=None, value=None)

        # Assert
        assert mock_tags_api.return_value.add.call_count == 1

    def test_delete_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.delete_tag(training_dataset=None, name=None)

        # Assert
        assert mock_tags_api.return_value.delete.call_count == 1

    def test_get_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.get_tag(training_dataset=None, name=None)

        # Assert
        assert mock_tags_api.return_value.get.call_count == 1

    def test_get_tags(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.get_tags(training_dataset=None)

        # Assert
        assert mock_tags_api.return_value.get.call_count == 1

    def test_update_statistics_config(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")

        td_engine = training_dataset_engine.TrainingDatasetEngine(feature_store_id)

        # Act
        td_engine.update_statistics_config(training_dataset=None)

        # Assert
        assert mock_td_api.return_value.update_metadata.call_count == 1
        assert (
            mock_td_api.return_value.update_metadata.call_args[0][2]
            == "updateStatsConfig"
        )
