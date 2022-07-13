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

    def new_feature_list(self, feature_group, updated_features):
        # take original schema and replaces the updated features and returns the new list
        new_features = []
        for feature in feature_group.features:
            if not any(
                updated.name.lower() == feature.name for updated in updated_features
            ):
                new_features.append(feature)
        return new_features + updated_features
