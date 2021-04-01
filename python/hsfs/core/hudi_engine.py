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
    HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"
    HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"
    PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
    PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"

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
        self._base_path = self._feature_group.location
        self._table_name = feature_group._get_online_table_name()

        self._primary_key = ",".join(feature_group.primary_key)
        self._partition_key = (
            ",".join(feature_group.partition_key)
            if len(feature_group.partition_key) >= 1
            else ""
        )
        self._partition_path = (
            ":SIMPLE,".join(feature_group.partition_key) + ":SIMPLE"
            if len(feature_group.partition_key) >= 1
            else ""
        )
        self._pre_combine_key = (
            feature_group.hudi_precombine_key
            if feature_group.hudi_precombine_key
            else feature_group.primary_key[0]
        )

        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            self._feature_store_id
        )
        self._connstr = self._storage_connector_api.get(
            self._feature_store_name
        ).connection_string

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

    def register_temporary_table(
        self, alias, start_timestamp, end_timestamp, read_options
    ):
        hudi_options = self._setup_hudi_read_opts(
            start_timestamp, end_timestamp, read_options
        )
        self._spark_session.read.format(self.HUDI_SPARK_FORMAT).options(
            **hudi_options
        ).load(self._base_path).createOrReplaceTempView(alias)

    def _write_hudi_dataset(self, dataset, save_mode, operation, write_options):
        hudi_options = self._setup_hudi_write_opts(operation, write_options)
        dataset.write.format(HudiEngine.HUDI_SPARK_FORMAT).options(**hudi_options).mode(
            save_mode
        ).save(self._base_path)

        feature_group_commit = self._get_last_commit_metadata(
            self._spark_context, self._base_path
        )

        return feature_group_commit

    def _setup_hudi_write_opts(self, operation, write_options):
        _jdbc_url = self._get_conn_str()
        hudi_options = {
            self.HUDI_KEY_GENERATOR_OPT_KEY: self.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL,
            self.HUDI_PRECOMBINE_FIELD: self._pre_combine_key[0]
            if isinstance(self._pre_combine_key, list)
            else self._pre_combine_key,
            self.HUDI_RECORD_KEY: self._primary_key,
            self.HUDI_PARTITION_FIELD: self._partition_path,
            self.HUDI_TABLE_NAME: self._table_name,
            self.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY: self.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL
            if len(self._partition_key) >= 1
            else self.HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL,
            self.HUDI_HIVE_SYNC_ENABLE: "true",
            self.HUDI_HIVE_SYNC_TABLE: self._table_name,
            self.HUDI_HIVE_SYNC_JDBC_URL: _jdbc_url,
            self.HUDI_HIVE_SYNC_DB: self._feature_store_name,
            self.HUDI_HIVE_SYNC_PARTITION_FIELDS: self._partition_key,
            self.HUDI_TABLE_OPERATION: operation,
        }

        if write_options:
            hudi_options.update(write_options)

        return hudi_options

    def _setup_hudi_read_opts(self, start_timestamp, end_timestamp, read_options):
        _hudi_commit_start_time = util.get_hudi_datestr_from_timestamp(start_timestamp)
        _hudi_commit_end_time = util.get_hudi_datestr_from_timestamp(end_timestamp)

        hudi_options = {
            self.HUDI_QUERY_TYPE_OPT_KEY: self.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL,
            self.HUDI_BEGIN_INSTANTTIME_OPT_KEY: _hudi_commit_start_time,
            self.HUDI_END_INSTANTTIME_OPT_KEY: _hudi_commit_end_time,
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
        pw = client.get_instance()._cert_key
        jdbc_url = (
            self._connstr
            + "sslTrustStore=t_certificate;trustStorePassword="
            + pw
            + ";sslKeyStore=k_certificate;keyStorePassword="
            + pw
        )
        return jdbc_url
