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

from hsfs import feature, feature_group
from hsfs.core import feature_group_base_engine


class TestFeatureGroupBaseEngine:
    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_base_engine.delete(feature_group=None)

        # Assert
        assert mock_fg_api.return_value.delete.call_count == 1

    def test_add_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_base_engine.add_tag(feature_group=None, name=None, value=None)

        # Assert
        assert mock_tags_api.return_value.add.call_count == 1

    def test_delete_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_base_engine.delete_tag(feature_group=None, name=None)

        # Assert
        assert mock_tags_api.return_value.delete.call_count == 1

    def test_get_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_base_engine.get_tag(feature_group=None, name=None)

        # Assert
        assert mock_tags_api.return_value.get.call_count == 1

    def test_get_tags(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_base_engine.get_tags(feature_group=None)

        # Assert
        assert mock_tags_api.return_value.get.call_count == 1

    def test_update_statistics_config(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_base_engine.update_statistics_config(feature_group=None)

        # Assert
        assert mock_fg_api.return_value.update_metadata.call_count == 1

    def test_new_feature_list(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")

        fg_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f")
        f1 = feature.Feature(name="f1")
        f2 = feature.Feature(name="f2")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[f, f1],
        )

        # Act
        result = fg_base_engine.new_feature_list(
            feature_group=fg, updated_features=[f1, f2]
        )

        # Assert
        assert len(result) == 3
        assert f in result
        assert f1 in result
        assert f2 in result
