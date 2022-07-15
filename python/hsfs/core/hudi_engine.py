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

from hsfs import feature_group_commit, util, client
from hsfs.core import feature_group_api, storage_connector_api


class HudiEngine:
    HUDI_SPARK_FORMAT = "org.apache.hudi"
    HUDI_TABLE_NAME = "hoodie.table.name"
    HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type"
    HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation"
    HUDI_RECORD_KEY = "hoodie.datasource.write.recordkey.field"
    HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field"
    HUDI_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field"

    HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable"
    HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table"
    HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database"
    HUDI_HIVE_SYNC_JDBC_URL = "hoodie.datasource.hive_sync.jdbcurl"
    HUDI_HIVE_SYNC_PARTITION_FIELDS = "hoodie.datasource.hive_sync.partition_fields"
    HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP = "hoodie.datasource.hive_sync.support_timestamp"

    HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class"
    HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator"
    HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = (
        "hoodie.datasource.hive_sync.partition_extractor_class"
    )
    DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL = (
        "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    )
    HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL = (
        "org.apache.hudi.hive.NonPartitionedExtractor"
    )
    HUDI_COPY_ON_WRITE = "COPY_ON_WRITE"
    HUDI_BULK_INSERT = "bulk_insert"
    HUDI_INSERT = "insert"
    HUDI_UPSERT = "upsert"
    HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type"
    HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
    HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
    HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
    HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"
    HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"
    PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
    PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"
    HUDI_DEFAULT_PARALLELISM = {
        "hoodie.bulkinsert.shuffle.parallelism": "5",
        "hoodie.insert.shuffle.parallelism": "5",
        "hoodie.upsert.shuffle.parallelism": "5",
    }

    def __init__(
        self,
        feature_store_id,
        feature_store_name,
        feature_group,
        spark_context,
        spark_session,
    ):
        self._feature_group = feature_group
        self._spark_context = spark_context
        self._spark_session = spark_session
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name

        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            self._feature_store_id
        )

        if self._feature_store_name:
            # For read operations we don't actually need the connector
            # Only to sync write operations
            self._connstr = self._storage_connector_api.get(
                self._feature_store_name
            ).connection_string
        else:
            self._connstr = None

    def save_hudi_fg(
        self, dataset, save_mode, operation, write_options, validation_id=None
    ):

        fg_commit = self._write_hudi_dataset(
            dataset, save_mode, operation, write_options
        )
        fg_commit.validation_id = validation_id
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def delete_record(self, delete_df, write_options):
        write_options[self.PAYLOAD_CLASS_OPT_KEY] = self.PAYLOAD_CLASS_OPT_VAL

        fg_commit = self._write_hudi_dataset(
            delete_df, "append", self.HUDI_UPSERT, write_options
        )
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def register_temporary_table(self, hudi_fg_alias, read_options):
        hudi_options = self._setup_hudi_read_opts(hudi_fg_alias, read_options)
        self._spark_session.read.format(self.HUDI_SPARK_FORMAT).options(
            **hudi_options
        ).load(self._feature_group.location).createOrReplaceTempView(
            hudi_fg_alias.alias
        )

    def _write_hudi_dataset(self, dataset, save_mode, operation, write_options):
        hudi_options = self._setup_hudi_write_opts(operation, write_options)
        dataset.write.format(HudiEngine.HUDI_SPARK_FORMAT).options(**hudi_options).mode(
            save_mode
        ).save(self._feature_group.location)

        feature_group_commit = self._get_last_commit_metadata(
            self._spark_context, self._feature_group.location
        )

        return feature_group_commit

    def _setup_hudi_write_opts(self, operation, write_options):
        table_name = self._feature_group._get_online_table_name()

        primary_key = ",".join(self._feature_group.primary_key)

        # add event time to primary key for upserts
        if self._feature_group.event_time is not None:
            primary_key = primary_key + "," + self._feature_group.event_time

        partition_key = (
            ",".join(self._feature_group.partition_key)
            if len(self._feature_group.partition_key) >= 1
            else ""
        )
        partition_path = (
            ":SIMPLE,".join(self._feature_group.partition_key) + ":SIMPLE"
            if len(self._feature_group.partition_key) >= 1
            else ""
        )
        pre_combine_key = (
            self._feature_group.hudi_precombine_key
            if self._feature_group.hudi_precombine_key
            else self._feature_group.primary_key[0]
        )

        jdbc_url = self._get_conn_str()
        hudi_options = {
            self.HUDI_KEY_GENERATOR_OPT_KEY: self.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL,
            self.HUDI_PRECOMBINE_FIELD: pre_combine_key,
            self.HUDI_RECORD_KEY: primary_key,
            self.HUDI_PARTITION_FIELD: partition_path,
            self.HUDI_TABLE_NAME: table_name,
            self.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY: self.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL
            if len(partition_key) >= 1
            else self.HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL,
            self.HUDI_HIVE_SYNC_ENABLE: "true",
            self.HUDI_HIVE_SYNC_TABLE: table_name,
            self.HUDI_HIVE_SYNC_JDBC_URL: jdbc_url,
            self.HUDI_HIVE_SYNC_DB: self._feature_store_name,
            self.HUDI_HIVE_SYNC_PARTITION_FIELDS: partition_key,
            self.HUDI_TABLE_OPERATION: operation,
            self.HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP: "true",
        }
        hudi_options.update(HudiEngine.HUDI_DEFAULT_PARALLELISM)

        if write_options:
            hudi_options.update(write_options)

        return hudi_options

    def _setup_hudi_read_opts(self, hudi_fg_alias, read_options):
        if hudi_fg_alias.left_feature_group_end_timestamp:
            _hudi_commit_start_time = util.get_hudi_datestr_from_timestamp(
                hudi_fg_alias.left_feature_group_start_timestamp
            )
            _hudi_commit_end_time = util.get_hudi_datestr_from_timestamp(
                hudi_fg_alias.left_feature_group_end_timestamp
            )

            hudi_options = {
                self.HUDI_QUERY_TYPE_OPT_KEY: self.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL,
                self.HUDI_BEGIN_INSTANTTIME_OPT_KEY: _hudi_commit_start_time,
                self.HUDI_END_INSTANTTIME_OPT_KEY: _hudi_commit_end_time,
            }
        else:
            hudi_options = {
                self.HUDI_QUERY_TYPE_OPT_KEY: self.HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL,
            }

        if read_options:
            hudi_options.update(read_options)

        return hudi_options

    @staticmethod
    def _get_last_commit_metadata(spark_context, base_path):
        hopsfs_conf = spark_context._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark_context._jsc.hadoopConfiguration()
        )
        commit_timeline = spark_context._jvm.org.apache.hudi.HoodieDataSourceHelpers.allCompletedCommitsCompactions(
            hopsfs_conf, base_path
        )

        commits_to_return = commit_timeline.getInstantDetails(
            commit_timeline.lastInstant().get()
        ).get()
        commit_metadata = spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata.fromBytes(
            commits_to_return,
            spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata().getClass(),
        )
        return feature_group_commit.FeatureGroupCommit(
            commitid=None,
            commit_date_string=commit_timeline.lastInstant().get().getTimestamp(),
            commit_time=util.get_timestamp_from_date_string(
                commit_timeline.lastInstant().get().getTimestamp()
            ),
            rows_inserted=commit_metadata.fetchTotalInsertRecordsWritten(),
            rows_updated=commit_metadata.fetchTotalUpdateRecordsWritten(),
            rows_deleted=commit_metadata.getTotalRecordsDeleted(),
        )

    def _get_conn_str(self):
        if not self._connstr:
            return ""

        credentials = {
            "sslTrustStore": client.get_instance()._get_jks_trust_store_path(),
            "trustStorePassword": client.get_instance()._cert_key,
            "sslKeyStore": client.get_instance()._get_jks_key_store_path(),
            "keyStorePassword": client.get_instance()._cert_key,
        }

        return self._connstr + ";".join(
            ["{}={}".format(option[0], option[1]) for option in credentials.items()]
        )
