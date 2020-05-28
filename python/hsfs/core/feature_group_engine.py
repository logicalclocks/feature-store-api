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
from hsfs.core import feature_group_api
from hsfs.core import storage_connector_api


class FeatureGroupEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"

    def __init__(self, feature_store_id):
        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )

    def save(self, feature_group, feature_dataframe, storage, write_options):

        if len(feature_group._features) == 0:
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

        if storage == "online" or storage == "all":
            # Add JDBC connection configuration in case of online feature group
            jdbc_options = self._get_online_opts(feature_group)
            online_write_options = {**jdbc_options, **online_write_options}

        engine.get_instance().save_dataframe(
            self._get_table_name(feature_group),
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
            jdbc_options = self._get_online_opts(feature_group)
            online_write_options = {**jdbc_options, **online_write_options}

        if (storage == "online" or storage == "all") and overwrite:
            self._feature_group_api.delete_content(feature_group)

        engine.get_instance().save_dataframe(
            self._get_table_name(feature_group),
            feature_group.partition_key,
            feature_dataframe,
            self.OVERWRITE if overwrite else self.APPEND,
            storage,
            offline_write_options,
            online_write_options,
        )

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)

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

    def _get_online_opts(self, feature_group):
        online_storage_connector = self._storage_connector_api.get_online_connector()
        args = [
            arg.split("=") for arg in online_storage_connector.arguments().split(",")
        ]

        return {
            "url": online_storage_connector.connection_string(),
            "user": [arg[1] for arg in args if arg[0] == "user"][0],
            "password": [arg[1] for arg in args if arg[0] == "passowrd"][0],
            "dbtable": self._get_online_table_name(feature_group),
        }
