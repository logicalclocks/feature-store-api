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


from hsfs import (
    statistics_config,
    storage_connector,
    training_dataset,
    training_dataset_feature,
    training_dataset_split,
)


class TestTrainingDataset:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["training_dataset"]["get"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 1
        td = td_list[0]
        assert td.id == 11
        assert td.name == "test_name"
        assert td.version == 1
        assert td.description == "test_description"
        assert td.data_format == "hudi"
        assert td._start_time == 1646438400000
        assert td._end_time == 1646697600000
        assert td.validation_size == 0.0
        assert td.test_size == 0.5
        assert td.train_start == 4
        assert td.train_end == 5
        assert td.validation_start == 6
        assert td.validation_end == 7
        assert td.test_start == 8
        assert td.test_end == 9
        assert td.coalesce is True
        assert td.seed == 123
        assert td.location == "test_location"
        assert td._from_query == "test_from_query"
        assert td._querydto == "test_querydto"
        assert td.feature_store_id == 22
        assert td.train_split == "test_train_split"
        assert td.training_dataset_type == "HOPSFS_TRAINING_DATASET"
        assert isinstance(td.storage_connector, storage_connector.JdbcConnector)
        assert len(td._features) == 1
        assert isinstance(
            td._features[0], training_dataset_feature.TrainingDatasetFeature
        )
        assert len(td.splits) == 1
        assert isinstance(td.splits[0], training_dataset_split.TrainingDatasetSplit)
        assert isinstance(td.statistics_config, statistics_config.StatisticsConfig)
        assert td.label == ["test_name"]

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["training_dataset"]["get_basic_info"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 1
        td = td_list[0]
        assert td.id is None
        assert td.name == "test_name"
        assert td.version == 1
        assert td.description is None
        assert td.data_format == "hudi"
        assert td._start_time is None
        assert td._end_time is None
        assert td.validation_size is None
        assert td.test_size is None
        assert td.train_start is None
        assert td.train_end is None
        assert td.validation_start is None
        assert td.validation_end is None
        assert td.test_start is None
        assert td.test_end is None
        assert td.coalesce is False
        assert td.seed is None
        assert td.location == ""
        assert td._from_query is None
        assert td._querydto is None
        assert td.feature_store_id == 22
        assert td.train_split is None
        assert td.training_dataset_type is None
        assert isinstance(td.storage_connector, storage_connector.JdbcConnector)
        assert len(td._features) == 0
        assert len(td.splits) == 0
        assert isinstance(td.statistics_config, statistics_config.StatisticsConfig)
        assert td.label == []

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["training_dataset"]["get_empty"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 0
