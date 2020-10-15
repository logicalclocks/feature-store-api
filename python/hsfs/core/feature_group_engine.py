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

from hsfs import engine
from hsfs import feature_group as fg
from hsfs.core import feature_group_api, storage_connector_api, tags_api, hudi_engine
from hsfs.client import exceptions


class FeatureGroupEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"
    ENTITY_TYPE = "featuregroups"

    def __init__(self, feature_store_id):
        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)

    def save(self, feature_group, feature_dataframe, write_options):

        if len(feature_group.features) == 0:
            # User didn't provide a schema. extract it from the dataframe
            feature_group._features = engine.get_instance().parse_schema_feature_group(
                feature_dataframe
            )

        # set primary and partition key columns
        # we should move this to the backend
        for feat in feature_group.features:
            if feat.name in feature_group.primary_key:
                feat.primary = True
            if feat.name in feature_group.partition_key:
                feat.partition = True

        self._feature_group_api.save(feature_group)

        offline_write_options = write_options
        online_write_options = write_options

        table_name = self._get_table_name(feature_group)

        if feature_group.online_enabled:
            # Add JDBC connection configuration in case of online feature group
            online_conn = self._storage_connector_api.get_online_connector()

            jdbc_options = online_conn.spark_options()
            jdbc_options["dbtable"] = self._get_online_table_name(feature_group)

            online_write_options = {**jdbc_options, **online_write_options}

        engine.get_instance().save_dataframe(
            table_name,
            feature_group,
            feature_dataframe,
            self.APPEND,
            hudi_engine.HudiEngine.HUDI_BULK_INSERT if feature_group.time_travel_format == "HUDI" else None,
            feature_group.online_enabled,
            None,
            offline_write_options,
            online_write_options,
        )

    def insert(
        self,
        feature_group,
        feature_dataframe,
        overwrite,
        operation,
        storage,
        write_options,
    ):
        offline_write_options = write_options
        online_write_options = write_options

        if not feature_group.online_enabled and storage == "online":
            raise exceptions.FeatureStoreException(
                "Online storage is not enabled for this feature group."
            )
        elif (
            feature_group.online_enabled and storage != "offline"
        ) or storage == "online":
            # Add JDBC connection configuration in case of online feature group
            online_conn = self._storage_connector_api.get_online_connector()

            jdbc_options = online_conn.spark_options()
            jdbc_options["dbtable"] = self._get_online_table_name(feature_group)

            online_write_options = {**jdbc_options, **online_write_options}

        if overwrite:
            self._feature_group_api.delete_content(feature_group)

        engine.get_instance().save_dataframe(
            self._get_table_name(feature_group),
            feature_group,
            feature_dataframe,
            self.APPEND,
            operation,
            feature_group.online_enabled,
            storage,
            offline_write_options,
            online_write_options,
        )

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)

    @staticmethod
    def commit_delete(feature_group, delete_df, write_options):
        hudi_engine_instance = hudi_engine.HudiEngine(
            feature_group.feature_store_id,
            feature_group.feature_store_name,
            feature_group,
            engine.get_instance()._spark_context,
            engine.get_instance()._spark_session,
        )
        return hudi_engine_instance.delete_record(delete_df, write_options)

    def update_statistics_config(self, feature_group):
        """Update the statistics configuration of a feature group."""
        self._feature_group_api.update_statistics_config(
            feature_group, feature_group, "updateStatsSettings"
        )

    def _get_table_name(self, feature_group):
        return (
            feature_group.feature_store_name
            + "."
            + feature_group.name
            + "_"
            + str(feature_group.version)
        )

    def _get_online_table_name(self, feature_group):
        return feature_group.name + "_" + str(feature_group.version)

    def add_tag(self, feature_group, name, value):
        """Attach a name/value tag to a feature group."""
        self._tags_api.add(feature_group, name, value)

    def delete_tag(self, feature_group, name):
        """Remove a tag from a feature group."""
        self._tags_api.delete(feature_group, name)

    def get_tags(self, feature_group, name):
        """Get tag with a certain name or all tags for a feature group."""
        return [tag.to_dict() for tag in self._tags_api.get(feature_group, name)]

    def sql(self, query, feature_store_name, dataframe_type, online):
        if online:
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            online_conn = None
        return engine.get_instance().sql(
            query, feature_store_name, online_conn, dataframe_type
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
