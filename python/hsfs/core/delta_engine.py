#
#   Copyright 2024 Hopsworks AB
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

from hsfs import feature_group_commit, util
from hsfs.core import feature_group_api

try:
    from delta.tables import DeltaTable
except ImportError:
    pass


class DeltaEngine:
    DELTA_SPARK_FORMAT = "delta"
    DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT = "timestampAsOf"

    def __init__(
        self,
        feature_store_id,
        feature_store_name,
        feature_group,
        spark_session,
        spark_context,
    ):
        self._feature_group = feature_group
        self._spark_context = spark_context
        self._spark_session = spark_session
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name

        self._feature_group_api = feature_group_api.FeatureGroupApi()

    def save_delta_fg(self, dataset, write_options, validation_id=None):
        fg_commit = self._write_delta_dataset(dataset, write_options)
        fg_commit.validation_id = validation_id
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def register_temporary_table(self, delta_fg_alias, read_options):
        delta_options = self._setup_delta_read_opts(delta_fg_alias, read_options)
        self._spark_session.read.format(self.DELTA_SPARK_FORMAT).options(
            **delta_options
        ).load(self._feature_group.location).createOrReplaceTempView(
            delta_fg_alias.alias
        )

    def _setup_delta_read_opts(self, delta_fg_alias, read_options):
        delta_options = {}
        if delta_fg_alias.left_feature_group_end_timestamp is None and (
            delta_fg_alias.left_feature_group_start_timestamp is None
            or delta_fg_alias.left_feature_group_start_timestamp == 0
        ):
            # snapshot query latest state
            delta_options = {}
        elif (
            delta_fg_alias.left_feature_group_end_timestamp is not None
            and delta_fg_alias.left_feature_group_start_timestamp is None
        ):
            # snapshot query with end time
            _delta_commit_end_time = util.get_delta_datestr_from_timestamp(
                delta_fg_alias.left_feature_group_end_timestamp
            )
            delta_options = {
                self.DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT: _delta_commit_end_time,
            }

        if read_options:
            delta_options.update(read_options)

        return delta_options

    def delete_record(self, delete_df, write_options):
        pass

    def _write_delta_dataset(self, dataset, write_options):
        if write_options is None:
            write_options = {}

        if not DeltaTable.isDeltaTable(
            self._spark_session, self._feature_group.location
        ):
            (
                dataset.write.format(DeltaEngine.DELTA_SPARK_FORMAT)
                .options(**write_options)
                .partitionBy(
                    self._feature_group.partition_key
                    if self._feature_group.partition_key
                    else []
                )
                .mode("append")
                .save(self._feature_group.location)
            )
        else:
            fg_source_table = DeltaTable.forPath(
                self._spark_session, self._feature_group.location
            )

            source_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_source"
            )
            updates_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_updates"
            )
            merge_query_str = self._generate_merge_query(source_alias, updates_alias)

            fg_source_table.alias(source_alias).merge(
                dataset.alias(updates_alias), merge_query_str
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        return self._get_last_commit_metadata(
            self._spark_session, self._feature_group.location
        )

    def _generate_merge_query(self, source_alias, updates_alias):
        merge_query_list = []
        primary_key = self._feature_group.primary_key

        # add event time to primary key for upserts
        if self._feature_group.event_time is not None:
            primary_key.append(self._feature_group.event_time)

        # add partition key for upserts
        if self._feature_group.partition_key:
            primary_key = primary_key + self._feature_group.partition_key

        for pk in primary_key:
            merge_query_list.append(f"{source_alias}.{pk} == {updates_alias}.{pk}")
        megrge_query_str = " AND ".join(merge_query_list)
        return megrge_query_str

    @staticmethod
    def _get_last_commit_metadata(spark_context, base_path):
        fg_source_table = DeltaTable.forPath(spark_context, base_path)
        last_commit = fg_source_table.history(1).first().asDict()
        version = last_commit["version"]
        commit_timestamp = util.convert_event_time_to_timestamp(
            last_commit["timestamp"]
        )
        commit_date_string = util.get_hudi_datestr_from_timestamp(commit_timestamp)
        operation_metrics = last_commit["operationMetrics"]

        if version == 0:
            fg_commit = feature_group_commit.FeatureGroupCommit(
                commitid=None,
                commit_date_string=commit_date_string,
                commit_time=commit_timestamp,
                rows_inserted=operation_metrics["numOutputRows"],
                rows_updated=0,
                rows_deleted=0,
                last_active_commit_time=commit_timestamp,
            )
        else:
            fg_commit = feature_group_commit.FeatureGroupCommit(
                commitid=None,
                commit_date_string=commit_date_string,
                commit_time=commit_timestamp,
                rows_inserted=operation_metrics["numTargetRowsInserted"],
                rows_updated=operation_metrics["numTargetRowsUpdated"],
                rows_deleted=operation_metrics["numTargetRowsDeleted"],
                last_active_commit_time=commit_timestamp,
            )

        return fg_commit
