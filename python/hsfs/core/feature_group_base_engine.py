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
from __future__ import annotations

from hsfs import util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import feature_group_api, kafka_api, storage_connector_api, tags_api


class FeatureGroupBaseEngine:
    ENTITY_TYPE = "featuregroups"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
        self._feature_group_api = feature_group_api.FeatureGroupApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()
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

    def get_parent_feature_groups(self, feature_group):
        """Get the parents of this feature group, based on explicit provenance.
        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only a minimal information is
        returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ProvenanceLinks`:  the feature groups used to generate this feature group
        """
        return self._feature_group_api.get_parent_feature_groups(feature_group)

    def get_storage_connector_provenance(self, feature_group):
        """Get the parents of this feature group, based on explicit provenance.
        Parents are storage connectors. These storage connector can be accessible,
        deleted or inaccessible.
        For deleted and inaccessible storage connector, only a minimal information is
        returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ExplicitProvenance.Links`: the storage connector used to generated this
            feature group
        """
        return self._feature_group_api.get_storage_connector_provenance(feature_group)

    def get_generated_feature_views(self, feature_group):
        """Get the generated feature view using this feature group, based on explicit
        provenance. These feature views can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature view links, so deleted
        will always be empty.
        For inaccessible feature views, only a minimal information is returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ProvenanceLinks`:  the feature views generated using this feature group
        """
        return self._feature_group_api.get_generated_feature_views(feature_group)

    def get_generated_feature_groups(self, feature_group):
        """Get the generated feature groups using this feature group, based on explicit
        provenance. These feature groups can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature group links, so deleted
        will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ProvenanceLinks`:  the feature groups generated using this feature group
        """
        return self._feature_group_api.get_generated_feature_groups(feature_group)

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
                util.autofix_feature_name(updated.name) == feature.name
                for updated in updated_features
            ):
                new_features.append(feature)
        return new_features + updated_features

    def _verify_schema_compatibility(self, feature_group_features, dataframe_features):
        err = []
        feature_df_dict = {
            util.autofix_feature_name(feat.name): feat.type
            for feat in dataframe_features
        }
        for feature_fg in feature_group_features:
            name = util.autofix_feature_name(feature_fg.name)
            fg_type = feature_fg.type.lower().replace(" ", "")
            # check if feature exists dataframe
            if name in feature_df_dict:
                df_type = feature_df_dict[name].lower().replace(" ", "")
                # remove match from lookup table
                del feature_df_dict[name]

                # check if types match
                if fg_type != df_type:
                    # don't check structs for exact match
                    if fg_type.startswith("struct") and df_type.startswith("struct"):
                        continue

                    err += [
                        f"{name} (expected type: '{fg_type}', "
                        f"derived from input: '{df_type}') has the wrong type."
                    ]

            else:
                err += [
                    f"{name} (type: '{feature_fg.type}') is missing from "
                    f"input dataframe."
                ]

        # any features that are left in lookup table are superfluous
        for feature_df_name, feature_df_type in feature_df_dict.items():
            err += [
                f"{util.autofix_feature_name(feature_df_name)} (type: '{feature_df_type}') does not exist "
                f"in feature group."
            ]

        # raise exception if any errors were found.
        if len(err) > 0:
            raise FeatureStoreException(
                "Features are not compatible with Feature Group schema: "
                + "".join(["\n - " + e for e in err])
                + "\nNote that feature (or column) names are case insensitive and "
                "spaces are automatically replaced with underscores."
            )

    def get_subject(self, feature_group):
        return self._kafka_api.get_subject(
            feature_group._feature_store_id,
            feature_group.get_fg_name(),
        )
