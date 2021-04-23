#
#   Copyright 2020 Logical Clocks AB
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

from hsfs import feature_group as fg
from hsfs.core import feature_group_api, storage_connector_api, tags_api, kafka_api


class FeatureGroupBaseEngine:
    ENTITY_TYPE = "featuregroups"

    def __init__(self, feature_store_id):
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._kafka_api = kafka_api.KafkaApi()

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)

    def add_tag(self, feature_group, name, value):
        """Attach a name/value tag to a feature group."""
        self._tags_api.add(feature_group, name, value)

    def delete_tag(self, feature_group, name):
        """Remove a tag from a feature group."""
        self._tags_api.delete(feature_group, name)

    def get_tag(self, feature_group, name):
        """Get tag with a certain name."""
        return self._tags_api.get(feature_group, name)[name]

    def get_tags(self, feature_group):
        """Get all tags for a feature group."""
        return self._tags_api.get(feature_group)

    def update_statistics_config(self, feature_group):
        """Update the statistics configuration of a feature group."""
        self._feature_group_api.update_metadata(
            feature_group, feature_group, "updateStatsConfig"
        )

    def update_features(self, feature_group, updated_features):
        """Updates features safely."""
        # perform changes on copy in case the update fails, so we don't leave
        # the user object in corrupted state
        new_features = []
        for feature in feature_group.features:
            match = False
            for updated_feature in updated_features:
                if updated_feature.name.lower() == feature.name:
                    match = True
                    new_features.append(updated_feature)
                    break
            if not match:
                new_features.append(feature)

        copy_feature_group = fg.FeatureGroup(
            None,
            None,
            None,
            None,
            id=feature_group.id,
            features=new_features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )

    def append_features(self, feature_group, new_features):
        """Appends features to a feature group."""
        # perform changes on copy in case the update fails, so we don't leave
        # the user object in corrupted state
        copy_feature_group = fg.FeatureGroup(
            None,
            None,
            None,
            None,
            id=feature_group.id,
            features=feature_group.features + new_features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )

    def update_description(self, feature_group, description):
        """Updates the description of a feature group."""
        copy_feature_group = fg.FeatureGroup(
            None,
            None,
            description,
            None,
            id=feature_group.id,
            features=feature_group.features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )
