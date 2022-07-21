#
#   Copyright 2022 Logical Clocks AB
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

import unittest
from unittest.mock import patch

from python.hsfs import engine, feature_group, feature
from python.hsfs.core import feature_group_api, tags_api, feature_group_base_engine


class TestFeatureGroupBase(unittest.TestCase):
    def test_feature_group_base_delete(self):
        with patch.object(
            feature_group_api.FeatureGroupApi, "delete"
        ) as mock_feature_group_api, patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)

            # Act
            fgBaseEngine.delete(fg)

            # Assert
            self.assertEqual(1, mock_feature_group_api.call_count)

    def test_feature_group_base_add_tag(self):
        with patch.object(tags_api.TagsApi, "add") as mock_tags_api, patch.object(
            engine, "get_type"
        ):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)

            # Act
            fgBaseEngine.add_tag(fg, "name", "value")

            # Assert
            self.assertEqual(1, mock_tags_api.call_count)

    def test_feature_group_base_delete_tag(self):
        with patch.object(tags_api.TagsApi, "delete") as mock_tags_api, patch.object(
            engine, "get_type"
        ):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)

            # Act
            fgBaseEngine.delete_tag(fg, "name")

            # Assert
            self.assertEqual(1, mock_tags_api.call_count)

    def test_feature_group_base_get_tag(self):
        with patch.object(tags_api.TagsApi, "get") as mock_tags_api, patch.object(
            engine, "get_type"
        ):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)

            # Act
            fgBaseEngine.get_tag(fg, "name")

            # Assert
            self.assertEqual(1, mock_tags_api.call_count)

    def test_feature_group_base_get_tags(self):
        with patch.object(tags_api.TagsApi, "get") as mock_tags_api, patch.object(
            engine, "get_type"
        ):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)

            # Act
            fgBaseEngine.get_tags(fg)

            # Assert
            self.assertEqual(1, mock_tags_api.call_count)

    def test_feature_group_base_update_statistics_config(self):
        with patch.object(
            feature_group_api.FeatureGroupApi, "update_metadata"
        ) as mock_feature_group_api, patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)

            # Act
            fgBaseEngine.update_statistics_config(fg)

            # Assert
            self.assertEqual(1, mock_feature_group_api.call_count)
            self.assertIn("updateStatsConfig", mock_feature_group_api.call_args.args)

    def test_feature_group_base_new_feature_list(self):
        with patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
                features=[feature.Feature("test1"), feature.Feature("test2")],
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)
            feature_list = [feature.Feature("test3")]

            # Act
            new_feature_list = fgBaseEngine.new_feature_list(fg, feature_list)

            # Assert
            self.assertEqual(3, len(new_feature_list))

    def test_feature_group_base_new_feature_list_overlap(self):
        with patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
                features=[feature.Feature("test1"), feature.Feature("test2")],
            )
            fgBaseEngine = feature_group_base_engine.FeatureGroupBaseEngine(99)
            feature_list = [feature.Feature("test2"), feature.Feature("test3")]

            # Act
            new_feature_list = fgBaseEngine.new_feature_list(fg, feature_list)

            # Assert
            self.assertEqual(3, len(new_feature_list))
