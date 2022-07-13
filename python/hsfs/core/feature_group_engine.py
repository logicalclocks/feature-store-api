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
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import feature_group_base_engine, hudi_engine


class FeatureGroupEngine(feature_group_base_engine.FeatureGroupBaseEngine):
    def __init__(self, feature_store_id):
        super().__init__(feature_store_id)

        # cache online feature store connector
        self._online_conn = None

    def save(self, feature_group, feature_dataframe, write_options, validation_options):

        dataframe_features = engine.get_instance().parse_schema_feature_group(
            feature_dataframe, feature_group.time_travel_format
        )

        self._save_feature_group_metadata(
            feature_group, dataframe_features, write_options
        )

        # ge validation on python and non stream feature groups on spark
        ge_report = feature_group._great_expectation_engine.validate(
            feature_group, feature_dataframe, True, validation_options
        )

        if ge_report is not None and ge_report.ingestion_result == "REJECTED":
            return None, ge_report

        offline_write_options = write_options
        online_write_options = self.get_kafka_config(write_options)

        return (
            engine.get_instance().save_dataframe(
                feature_group,
                feature_dataframe,
                hudi_engine.HudiEngine.HUDI_BULK_INSERT
                if feature_group.time_travel_format == "HUDI"
                else None,
                feature_group.online_enabled,
                None,
                offline_write_options,
                online_write_options,
            ),
            ge_report,
        )

    def insert(
        self,
        feature_group,
        feature_dataframe,
        overwrite,
        operation,
        storage,
        write_options,
        validation_options,
    ):

        dataframe_features = engine.get_instance().parse_schema_feature_group(
            feature_dataframe, feature_group.time_travel_format
        )

        if not feature_group._id:
            # only save metadata if feature group does not exist
            self._save_feature_group_metadata(
                feature_group, dataframe_features, write_options
            )
        else:
            # else, just verify that feature group schema matches user-provided dataframe
            self._verify_schema_compatibility(
                feature_group.features, dataframe_features
            )

        # ge validation on python and non stream feature groups on spark
        ge_report = feature_group._great_expectation_engine.validate(
            feature_group, feature_dataframe, True, validation_options
        )

        if ge_report is not None and ge_report.ingestion_result == "REJECTED":
            return None, ge_report

        offline_write_options = write_options
        online_write_options = self.get_kafka_config(write_options)

        if not feature_group.online_enabled and storage == "online":
            raise exceptions.FeatureStoreException(
                "Online storage is not enabled for this feature group."
            )

        if overwrite:
            self._feature_group_api.delete_content(feature_group)

        return (
            engine.get_instance().save_dataframe(
                feature_group,
                feature_dataframe,
                "bulk_insert" if overwrite else operation,
                feature_group.online_enabled,
                storage,
                offline_write_options,
                online_write_options,
            ),
            ge_report,
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
        if online and self._online_conn is None:
            self._online_conn = self._storage_connector_api.get_online_connector()
        return engine.get_instance().sql(
            query,
            feature_store_name,
            self._online_conn if online else None,
            dataframe_type,
            read_options,
        )

    def _update_features_metadata(self, feature_group, features):
        # perform changes on copy in case the update fails, so we don't leave
        # the user object in corrupted state
        copy_feature_group = fg.FeatureGroup(
            name=None,
            version=None,
            featurestore_id=None,
            description=None,
            id=feature_group.id,
            stream=feature_group.stream,
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
        # first get empty dataframe of current version and append new feature
        # necessary to write empty df to the table in order for the parquet schema
        # which is used by hudi to be updated
        df = engine.get_instance().get_empty_appended_dataframe(
            feature_group.read(), new_features
        )

        self._update_features_metadata(
            feature_group, feature_group.features + new_features
        )

        # write empty dataframe to update parquet schema
        engine.get_instance().save_empty_dataframe(feature_group, df)

    def update_description(self, feature_group, description):
        """Updates the description of a feature group."""
        copy_feature_group = fg.FeatureGroup(
            name=None,
            version=None,
            featurestore_id=None,
            description=description,
            id=feature_group.id,
            stream=feature_group.stream,
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
        checkpoint_dir,
        write_options,
    ):

        if not feature_group.online_enabled and not feature_group.stream:
            raise exceptions.FeatureStoreException(
                "Online storage is not enabled for this feature group. "
                "It is currently only possible to stream to the online storage."
            )

        dataframe_features = engine.get_instance().parse_schema_feature_group(
            dataframe, feature_group.time_travel_format
        )

        if not feature_group._id:
            self._save_feature_group_metadata(
                feature_group, dataframe_features, write_options
            )

            if not feature_group.stream:
                # insert_stream method was called on non stream feature group object that has not been saved.
                # we will use save_dataframe method on empty dataframe to create directory structure
                offline_write_options = write_options
                online_write_options = self.get_kafka_config(write_options)
                engine.get_instance().save_dataframe(
                    feature_group,
                    engine.get_instance().create_empty_df(dataframe),
                    hudi_engine.HudiEngine.HUDI_BULK_INSERT
                    if feature_group.time_travel_format == "HUDI"
                    else None,
                    feature_group.online_enabled,
                    None,
                    offline_write_options,
                    online_write_options,
                )
        else:
            # else, just verify that feature group schema matches user-provided dataframe
            self._verify_schema_compatibility(
                feature_group.features, dataframe_features
            )

        if not feature_group.stream:
            warnings.warn(
                "`insert_stream` method in the next release be available only for feature groups created with "
                "`stream=True`."
            )

        streaming_query = engine.get_instance().save_stream_dataframe(
            feature_group,
            dataframe,
            query_name,
            output_mode,
            await_termination,
            timeout,
            checkpoint_dir,
            self.get_kafka_config(write_options),
        )

        return streaming_query

    def _verify_schema_compatibility(self, feature_group_features, dataframe_features):
        err = []
        feature_df_dict = {feat.name: feat.type for feat in dataframe_features}
        for feature_fg in feature_group_features:
            fg_type = feature_fg.type.lower().replace(" ", "")
            # check if feature exists dataframe
            if feature_fg.name in feature_df_dict:
                df_type = feature_df_dict[feature_fg.name].lower().replace(" ", "")
                # remove match from lookup table
                del feature_df_dict[feature_fg.name]

                # check if types match
                if fg_type != df_type:
                    # don't check structs for exact match
                    if fg_type.startswith("struct") and df_type.startswith("struct"):
                        continue

                    err += [
                        f"{feature_fg.name} ("
                        f"expected type: '{fg_type}', "
                        f"derived from input: '{df_type}') has the wrong type."
                    ]

            else:
                err += [
                    f"{feature_fg.name} (type: '{feature_fg.type}') is missing from "
                    f"input dataframe."
                ]

        # any features that are left in lookup table are superfluous
        for feature_df_name, feature_df_type in feature_df_dict.items():
            err += [
                f"{feature_df_name} (type: '{feature_df_type}') does not exist "
                f"in feature group."
            ]

        # raise exception if any errors were found.
        if len(err) > 0:
            raise FeatureStoreException(
                "Features are not compatible with Feature Group schema: "
                + "".join(["\n - " + e for e in err])
            )

    def _save_feature_group_metadata(
        self, feature_group, dataframe_features, write_options
    ):

        # this means FG doesn't exist and should create the new one
        if len(feature_group.features) == 0:
            # User didn't provide a schema; extract it from the dataframe
            feature_group._features = dataframe_features
        else:
            # User provided a schema; check if it is compatible with dataframe.
            self._verify_schema_compatibility(
                feature_group.features, dataframe_features
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

        if feature_group.stream:
            feature_group._options = write_options

        self._feature_group_api.save(feature_group)
        print(
            "Feature Group created successfully, explore it at \n"
            + self._get_feature_group_url(feature_group)
        )

    def _get_feature_group_url(self, feature_group):
        sub_path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/fs/"
            + str(feature_group.feature_store_id)
            + "/fg/"
            + str(feature_group.id)
        )
        return util.get_hostname_replaced_url(sub_path)
