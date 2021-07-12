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

import warnings

from hsfs import engine, client, util
from hsfs import feature_group as fg
from hsfs.client import exceptions
from hsfs.core import feature_group_base_engine, hudi_engine


class FeatureGroupEngine(feature_group_base_engine.FeatureGroupBaseEngine):
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
            if (
                feature_group.hudi_precombine_key is not None
                and feat.name == feature_group.hudi_precombine_key
            ):
                feat.hudi_precombine_key = True

        self._feature_group_api.save(feature_group)
        validation_id = None
        if feature_group.validation_type != "NONE" and engine.get_type() == "spark":
            # If the engine is Hive, the validation will be executed by
            # the Hopsworks job ingesting the data
            validation = feature_group.validate(feature_dataframe)
            validation_id = validation.validation_id

        offline_write_options = write_options
        online_write_options = self.get_kafka_config(write_options)

        return engine.get_instance().save_dataframe(
            feature_group,
            feature_dataframe,
            hudi_engine.HudiEngine.HUDI_BULK_INSERT
            if feature_group.time_travel_format == "HUDI"
            else None,
            feature_group.online_enabled,
            None,
            offline_write_options,
            online_write_options,
            validation_id,
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
        validation_id = None
        if feature_group.validation_type != "NONE" and engine.get_type() == "spark":
            # If the engine is Hive, the validation will be executed by
            # the Hopsworks job ingesting the data
            validation = feature_group.validate(feature_dataframe)
            validation_id = validation.validation_id

        offline_write_options = write_options
        online_write_options = self.get_kafka_config(write_options)

        if not feature_group.online_enabled and storage == "online":
            raise exceptions.FeatureStoreException(
                "Online storage is not enabled for this feature group."
            )

        if overwrite:
            self._feature_group_api.delete_content(feature_group)

        return engine.get_instance().save_dataframe(
            feature_group,
            feature_dataframe,
            "bulk_insert" if overwrite else operation,
            feature_group.online_enabled,
            storage,
            offline_write_options,
            online_write_options,
            validation_id,
        )

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)

    def commit_details(self, feature_group, wallclock_time, limit):
        if (
            feature_group._time_travel_format is None
            or feature_group._time_travel_format.upper() != "HUDI"
        ):
            raise exceptions.FeatureStoreException(
                "commit_details can only be used on time travel enabled feature groups"
            )

        wallclock_timestamp = (
            util.get_timestamp_from_date_string(wallclock_time)
            if wallclock_time is not None
            else None
        )
        feature_group_commits = self._feature_group_api.get_commit_details(
            feature_group, wallclock_timestamp, limit
        )
        commit_details = {}
        for feature_group_commit in feature_group_commits:
            commit_details[feature_group_commit.commitid] = {
                "committedOn": util.get_hudi_datestr_from_timestamp(
                    feature_group_commit.commitid
                ),
                "rowsUpdated": feature_group_commit.rows_updated,
                "rowsInserted": feature_group_commit.rows_inserted,
                "rowsDeleted": feature_group_commit.rows_deleted,
            }
        return commit_details

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

    def sql(self, query, feature_store_name, dataframe_type, online, read_options):
        if online:
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            online_conn = None
        return engine.get_instance().sql(
            query, feature_store_name, online_conn, dataframe_type, read_options
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

    def get_avro_schema(self, feature_group):
        return self._kafka_api.get_topic_subject(feature_group._online_topic_name)

    def get_kafka_config(self, online_write_options):
        config = {
            "kafka.bootstrap.servers": ",".join(
                [
                    endpoint.replace("INTERNAL://", "")
                    for endpoint in self._kafka_api.get_broker_endpoints()
                ]
            ),
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": client.get_instance()._get_jks_trust_store_path(),
            "kafka.ssl.truststore.password": client.get_instance()._cert_key,
            "kafka.ssl.keystore.location": client.get_instance()._get_jks_key_store_path(),
            "kafka.ssl.keystore.password": client.get_instance()._cert_key,
            "kafka.ssl.key.password": client.get_instance()._cert_key,
            "kafka.ssl.endpoint.identification.algorithm": "",
        }
        return {**online_write_options, **config}

    def insert_stream(
        self,
        feature_group,
        dataframe,
        query_name,
        output_mode,
        await_termination,
        timeout,
        write_options,
    ):
        if not feature_group.online_enabled:
            raise exceptions.FeatureStoreException(
                "Online storage is not enabled for this feature group. "
                "It is currently only possible to stream to the online storage."
            )

        if feature_group.validation_type != "NONE":
            warnings.warn(
                "Stream ingestion for feature group `{}`, with version `{}` will not perform validation.".format(
                    feature_group.name, feature_group.version
                ),
                util.ValidationWarning,
            )

        return engine.get_instance().save_stream_dataframe(
            feature_group,
            dataframe,
            query_name,
            output_mode,
            await_termination,
            timeout,
            self.get_kafka_config(write_options),
        )
