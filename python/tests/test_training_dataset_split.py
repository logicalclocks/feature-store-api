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


from hsfs import training_dataset_split


class TestTrainingDatasetSplit:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["training_dataset_split"]["get"]["response"]

        # Act
        td_split = training_dataset_split.TrainingDatasetSplit.from_response_json(json)

        # Assert
        assert td_split.name == "test_name"
        assert td_split.percentage == "test_percentage"
        assert td_split.split_type == "test_split_type"
        assert td_split.start_time == "test_start_time"
        assert td_split.end_time == "test_end_time"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["training_dataset_split"]["get_basic_info"]["response"]

        # Act
        td_split = training_dataset_split.TrainingDatasetSplit.from_response_json(json)

        # Assert
        assert td_split.name == "test_name"
        assert td_split.percentage is None
        assert td_split.split_type == "test_split_type"
        assert td_split.start_time is None
        assert td_split.end_time is None
