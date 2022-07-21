#
#   Copyright 2020 Logical Clocks AB
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

from hsfs import engine
from hsfs import feature_group as fg
from hsfs.core import feature_group_base_engine


class ExternalFeatureGroupEngine(feature_group_base_engine.FeatureGroupBaseEngine):
    def save(self, feature_group):
        if len(feature_group.features) == 0:
            # If the user didn't specify the schema, parse it from the query
            external_dataset = engine.get_instance().register_external_temporary_table(
                feature_group, "read_ondmd"
            )
            feature_group._features = engine.get_instance().parse_schema_feature_group(
                external_dataset
            )

        # set primary and partition key columns
        # we should move this to the backend
        for feat in feature_group.features:
            if feat.name in feature_group.primary_key:
                feat.primary = True

        self._feature_group_api.save(feature_group)

    def _update_features_metadata(self, feature_group, features):
        # perform changes on copy in case the update fails, so we don't leave
        # the user object in corrupted state
        copy_feature_group = fg.ExternalFeatureGroup(
            storage_connector=feature_group.storage_connector,
            id=feature_group.id,
            features=features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )

    def update_features(self, feature_group, updated_features):
        """Updates features safely."""
        self._update_features_metadata(
            feature_group, self.new_feature_list(feature_group, updated_features)
        )

    def append_features(self, feature_group, new_features):
        """Appends features to a feature group."""
        self._update_features_metadata(
            feature_group, feature_group.features + new_features
        )

    def update_description(self, feature_group, description):
        """Updates the description of a feature group."""
        copy_feature_group = fg.ExternalFeatureGroup(
            storage_connector=feature_group.storage_connector,
            id=feature_group.id,
            description=description,
            features=feature_group.features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )
