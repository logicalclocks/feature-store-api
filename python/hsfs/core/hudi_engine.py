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

import os
from pathlib import Path

from datetime import datetime

from hsfs import feature_group_commit
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
    HUDI_COPY_ON_WRITE = "COPY_ON_WRITE"
    HUDI_BULK_INSERT = "bulk_insert"
    HUDI_INSERT = "insert"
    HUDI_UPSERT = "upsert"
    HUDI_DELETE = "delete"
    HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type"
    HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
    HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
    HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"
    HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"
    PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
    PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"

    def __init__(self, feature_group, spark_context):
        """
        HudiEngine object that has utility methods for handling interacting with apache hudi on hopsworks.

        # Arguments
        feature_group: FeatureGroup, required
            metadata object of feature group

        spark_context: SparkContext, required
            SparkContext
        """

        self._feature_group = feature_group
        self._spark_context = spark_context
        self._feature_store_name = self._feature_group.feature_store_name
        self._base_path = self._feature_group.location
        self._table_name = feature_group.name + "_" + feature_group.version
        self._partition_key = feature_group.partition_key
        self._partition_key = feature_group.partition_key[0]
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_group.feature_store_id
        )
        self._connstr = self._storage_connector_api.get(
            self._feature_group.feature_store_name, "JDBC"
        )

    def save_hudi_fg(self, dataset, save_mode, operation):
        """
        Save feature group as hudi format and save commit metadata in the backend.

        # Arguments
        feature_group_instance: FeatureGroup, required
            metadata object of feature group
        feature_group_commit_instance: FeatureGroupCommit, required
            metadata object of feature group commit

        # Returns
            FeatureGroupCommit
        """

        fg_commit = _write_hudi_dataset(
            self._spark_context,
            self._connstr,
            dataset,
            save_mode,
            self._base_path,
            operation,
            self._feature_store_name,
            self._table_name,
            self._recordkey,
            self._partitionkey,
            self._precombinekey,
        )

        # TODO (davit): make sure writing completed without exception
        api_responce_fgcommit = feature_group_api.commit(self._feature_group, fg_commit)
        return api_responce_fgcommit

    def register_temporary_table(self, alias, start_timestamp, end_timestamp):
        """
        Register temporary table as hudi format and save commit metadata in the backend.

        # Arguments
        dataset: Dataset, required
            Spark Data Frame to be saved as feature group in hopsworks feature store
        alias: string, required
            alias name for temporary table
        start_timestamp: long, required
             timestamp to start retrieving commits
        end_timestamp: long, required
             timestamp till retrieving commits

        # Returns

        """
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
        self._spark_session.conf.set(
            "mapreduce.input.pathFilter.class",
            self._spark_context._jvm.org.apache.hudi.hadoop.HoodieROTablePathFilter.getClass(),
            self._spark_context._jvm.org.apache.hadoop.fs.PathFilter.getClass(),
        )

        hudi_options = _setup_hudi_read_opts(
            self._partition_key, self._base_path, start_timestamp, end_timestamp
        )
        self._spark_session.read().format(self.HUDI_SPARK_FORMAT).options(
            **hudi_options
        ).load(self._base_path).registerTempTable(alias)


def _write_hudi_dataset(
    spark_context,
    connstr,
    dataset,
    save_mode,
    base_path,
    operation,
    feature_store_name,
    table_name,
    recordkey,
    partitionkey,
    precombinekey,
):

    hudi_options = _setup_hudi_write_opts(
        connstr,
        feature_store_name,
        table_name,
        operation,
        recordkey,
        partitionkey,
        precombinekey,
    )

    supported_ops = ["upsert", "insert", "bulk_insert"]

    if operation not in supported_ops:
        raise ValueError(
            "Unknown operation "
            + operation
            + " is provided. Please use either upsert, insert or bulk_insert"
        )

    hudi_options[HudiEngine.HUDI_TABLE_OPERATION] = operation

    dataset.write().format(HudiEngine.HUDI_SPARK_FORMAT).options(**hudi_options).mode(
        save_mode
    ).save(base_path)

    feature_group_commit = _get_last_commit_metadata(spark_context, base_path)
    return feature_group_commit


def _setup_hudi_write_opts(
    connstr, feature_store_name, tableName, operation, recordkey, partition, precombine
):
    pw = _get_cert_pw()
    jdbc_url = (
        connstr
        + "sslTrustStore=t_certificate;trustStorePassword="
        + pw
        + ";sslKeyStore=k_certificate;keyStorePassword="
        + pw
    )

    hudi_options = {
        HudiEngine.HUDI_PRECOMBINE_FIELD: precombine,
        HudiEngine.HUDI_RECORD_KEY: recordkey,
        HudiEngine.HUDI_PARTITION_FIELD: partition,
        HudiEngine.HUDI_TABLE_NAME: tableName,
        HudiEngine.HUDI_HIVE_SYNC_ENABLE: "true",
        HudiEngine.HUDI_HIVE_SYNC_TABLE: tableName,
        HudiEngine.HUDI_HIVE_SYNC_JDBC_URL: jdbc_url,
        HudiEngine.HUDI_HIVE_SYNC_DB: feature_store_name,
        # TODO (davit):
        "hoodie.upsert.shuffle.parallelism": 2,
        "hoodie.insert.shuffle.parallelism": 2,
    }

    return hudi_options


def _setup_hudi_read_opts(partition_key, base_path, start_timestamp, end_timestamp):

    hudi_options = {}
    number_partitions = len(partition_key)

    # Snapshot query
    if start_timestamp is None and end_timestamp is None:
        hudi_options["hoodie.datasource.query.type"] = "snapshot"
        _base_path = base_path + "/*" * (number_partitions + 1)
    elif end_timestamp is not None:
        hudi_options["hoodie.datasource.query.type"] = "incremental"
        hudi_commit_endtime = _timestamp2hudiformat(end_timestamp)
        # point in time  query
        if start_timestamp is None:
            hudi_options["hoodie.datasource.query.type"] = "000"
            hudi_options["hoodie.datasource.read.end.instanttime"] = hudi_commit_endtime
        # incremental  query
        elif start_timestamp is not None:
            hudi_commit_starttime = _timestamp2hudiformat(start_timestamp)
            hudi_options[
                "hoodie.datasource.read.begin.instanttime"
            ] = hudi_commit_starttime
            hudi_options["hoodie.datasource.read.end.instanttime"] = hudi_commit_endtime

    return hudi_options


def _get_last_commit_metadata(spark_context, base_path):

    hopsfs_conf = spark_context._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark_context._jsc.hadoopConfiguration()
    )
    commit_timeline = spark_context._jvm.org.apache.hudi.HoodieDataSourceHelpers.allCompletedCommitsCompactions(
        hopsfs_conf, base_path
    )

    commits2return = commit_timeline.getInstantDetails(
        commit_timeline.lastInstant().get()
    ).get()
    commit_metadata = spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata.fromBytes(
        commits2return,
        spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata().getClass(),
    )

    rows_inserted = commit_metadata.fetchTotalInsertRecordsWritten()
    rows_updated = commit_metadata.fetchTotalUpdateRecordsWritten()
    rows_deleted = commit_metadata.getTotalRecordsDeleted()

    fg_commit_metadata = feature_group_commit.FeatureGroupCommit(
        commitid=None,
        commitdatestring=None,
        rowsinserted=rows_inserted,
        rowsupdated=rows_updated,
        rowsdeleted=rows_deleted,
    )

    return fg_commit_metadata


def _timestamp2hudiformat(timestamp):
    date_obj = datetime.fromtimestamp(timestamp)
    date_str = date_obj.strftime("%Y%m%d%H%M%S")
    return date_str


def _get_cert_pw():
    """
    Get keystore password from local container
    Returns:
        Certificate password
    """
    pwd_path = Path("material_passwd")
    if not pwd_path.exists():
        username = os.environ["HADOOP_USER_NAME"]
        material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
        pwd_path = material_directory.joinpath(username + "__cert.key")

    with pwd_path.open() as f:
        return f.read()
