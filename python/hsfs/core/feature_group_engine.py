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

import datetime

from hsfs import engine, statistics
from hsfs.core import feature_group_api, storage_connector_api, tags_api, statistics_api


class FeatureGroupEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"

    def __init__(self, feature_store_id):
        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._tags_api = tags_api.TagsApi(feature_store_id, "featuregroups")
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, "featuregroups"
        )

    def save(self, feature_group, feature_dataframe, storage, write_options):

        if len(feature_group.features) == 0:
            # User didn't provide a schema. extract it from the dataframe
            feature_group._features = engine.get_instance().parse_schema(
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

        if storage == "online" or storage == "all":
            # Add JDBC connection configuration in case of online feature group
            online_conn = self._storage_connector_api.get_online_connector()

            jdbc_options = online_conn.spark_options()
            jdbc_options["dbtable"] = self._get_online_table_name(feature_group)

            online_write_options = {**jdbc_options, **online_write_options}

        engine.get_instance().save_dataframe(
            table_name,
            feature_group.partition_key,
            feature_dataframe,
            self.APPEND,
            storage,
            offline_write_options,
            online_write_options,
        )

    def insert(
        self, feature_group, feature_dataframe, overwrite, storage, write_options
    ):
        offline_write_options = write_options
        online_write_options = write_options

        if storage == "online" or storage == "all":
            # Add JDBC connection configuration in case of online feature group
            online_conn = self._storage_connector_api.get_online_connector()

            jdbc_options = online_conn.spark_options()
            jdbc_options["dbtable"] = self._get_online_table_name(feature_group)

            online_write_options = {**jdbc_options, **online_write_options}

        if (storage == "offline" or storage == "all") and overwrite:
            self._feature_group_api.delete_content(feature_group)

        engine.get_instance().save_dataframe(
            self._get_table_name(feature_group),
            feature_group.partition_key,
            feature_dataframe,
            self.APPEND,
            storage,
            offline_write_options,
            online_write_options,
        )

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)

    def update(self, feature_group):
        self._feature_group_api.update(feature_group)

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

    def sql(self, query, feature_store_name, dataframe_type, storage):
        if storage == "online":
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            online_conn = None
        return engine.get_instance().sql(
            query, feature_store_name, online_conn, dataframe_type
        )

    def compute_statistics(self, feature_group, feature_dataframe):
        """Compute statistics for a dataframe and send the result json to Hopsworks."""
        commit_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        content_str = engine.get_instance().profile_df(
            feature_dataframe,
            feature_group.statistics_config.columns,
            feature_group.statistics_config.correlations,
            feature_group.statistics_config.histograms,
        )
        stats = statistics.Statistics(commit_str, content_str)
        self._statistics_api.post(feature_group, stats)
