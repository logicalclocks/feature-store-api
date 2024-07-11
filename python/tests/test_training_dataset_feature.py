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


from hsfs import feature_group, training_dataset_feature


class TestTrainingDatasetFeature:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["training_dataset_feature"]["get"]["response"]

        # Act
        td_feature = training_dataset_feature.TrainingDatasetFeature.from_response_json(
            json
        )

        # Assert
        assert td_feature.name == "test_name"
        assert td_feature.type == "test_type"
        assert td_feature.index == "test_index"
        assert isinstance(td_feature._feature_group, feature_group.FeatureGroup)
        assert (
            td_feature._feature_group_feature_name == "test_feature_group_feature_name"
        )
        assert td_feature.label == "test_label"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["training_dataset_feature"]["get_basic_info"][
            "response"
        ]

        # Act
        td_feature = training_dataset_feature.TrainingDatasetFeature.from_response_json(
            json
        )

        # Assert
        assert td_feature.name == "test_name"
        assert td_feature.type is None
        assert td_feature.index is None
        assert td_feature._feature_group is None
        assert td_feature._feature_group_feature_name is None
        assert td_feature.label is False
