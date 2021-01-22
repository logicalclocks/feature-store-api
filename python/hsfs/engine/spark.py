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

import importlib.util
import os

import pandas as pd
import numpy as np

# in case importing in %%local
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.rdd import RDD
except ImportError:
    pass

from hsfs import feature, training_dataset_feature
from hsfs.storage_connector import StorageConnector
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import hudi_engine
from hsfs.constructor import query


class Engine:
    HIVE_FORMAT = "hive"
    JDBC_FORMAT = "jdbc"

    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

        if importlib.util.find_spec("pydoop"):
            # If we are on Databricks don't setup Pydoop as it's not available and cannot be easily installed.
            self._setup_pydoop()

    def sql(self, sql_query, feature_store, connector, dataframe_type):
        if not connector:
            result_df = self._sql_offline(sql_query, feature_store)
        else:
            result_df = self._jdbc(sql_query, connector)

        self.set_job_group("", "")
        return self._return_dataframe_type(result_df, dataframe_type)

    def _sql_offline(self, sql_query, feature_store):
        # set feature store
        self._spark_session.sql("USE {}".format(feature_store))
        return self._spark_session.sql(sql_query)

    def _jdbc(self, sql_query, connector):
        options = connector.spark_options()
        if sql_query:
            options["query"] = sql_query

        return (
            self._spark_session.read.format(self.JDBC_FORMAT).options(**options).load()
        )

    def show(self, sql_query, feature_store, n, online_conn):
        return self.sql(sql_query, feature_store, online_conn, "default").show(n)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)

    def register_on_demand_temporary_table(self, on_demand_fg, alias, options={}):
        if (
            on_demand_fg.storage_connector.connector_type == "JDBC"
            or on_demand_fg.storage_connector.connector_type == "REDSHIFT"
        ):
            # This is a JDBC on demand featuregroup
            on_demand_dataset = self._jdbc(
                on_demand_fg.query, on_demand_fg.storage_connector
            )
        else:
            on_demand_dataset = self.read(
                on_demand_fg.storage_connector,
                on_demand_fg.data_format,
                on_demand_fg.options,
                os.path.join(on_demand_fg.storage_connector.path, on_demand_fg.path),
            )

        on_demand_dataset.createOrReplaceTempView(alias)
        return on_demand_dataset

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        hudi_engine_instance = hudi_engine.HudiEngine(
            feature_store_id,
            feature_store_name,
            hudi_fg_alias.feature_group,
            self._spark_context,
            self._spark_session,
        )
        hudi_engine_instance.register_temporary_table(
            hudi_fg_alias.alias,
            hudi_fg_alias.left_feature_group_start_timestamp,
            hudi_fg_alias.left_feature_group_end_timestamp,
            read_options,
        )

    def _return_dataframe_type(self, dataframe, dataframe_type):
        if dataframe_type.lower() in ["default", "spark"]:
            return dataframe
        if dataframe_type.lower() == "pandas":
            return dataframe.toPandas()
        if dataframe_type.lower() == "numpy":
            return dataframe.toPandas().values
        if dataframe_type == "python":
            return dataframe.toPandas().values.tolist()

        raise TypeError(
            "Dataframe type `{}` not supported on this platform.".format(dataframe_type)
        )

    def convert_to_default_dataframe(self, dataframe):
        if isinstance(dataframe, pd.DataFrame):
            return self._spark_session.createDataFrame(dataframe)
        if isinstance(dataframe, list):
            dataframe = np.array(dataframe)
        if isinstance(dataframe, np.ndarray):
            if dataframe.ndim != 2:
                raise TypeError(
                    "Cannot convert numpy array that do not have two dimensions to a dataframe. "
                    "The number of dimensions are: {}".format(dataframe.ndim)
                )
            num_cols = dataframe.shape[1]
            dataframe_dict = {}
            for n_col in list(range(num_cols)):
                col_name = "col_" + str(n_col)
                dataframe_dict[col_name] = dataframe[:, n_col]
            pandas_df = pd.DataFrame(dataframe_dict)
            return self._spark_session.createDataFrame(pandas_df)
        if isinstance(dataframe, RDD):
            return dataframe.toDF()
        if isinstance(dataframe, DataFrame):
            return dataframe
        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, "
            "pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: {}".format(
                type(dataframe)
            )
        )

    def save_dataframe(
        self,
        table_name,
        feature_group,
        dataframe,
        save_mode,
        operation,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
    ):
        if storage == "offline" or not online_enabled:
            self._save_offline_dataframe(
                table_name,
                feature_group,
                dataframe,
                save_mode,
                operation,
                offline_write_options,
            )
        elif storage == "online":
            self._save_online_dataframe(
                table_name, dataframe, save_mode, online_write_options
            )
        elif online_enabled and storage is None:
            self._save_offline_dataframe(
                table_name,
                feature_group,
                dataframe,
                save_mode,
                operation,
                offline_write_options,
            )
            self._save_online_dataframe(
                table_name, dataframe, save_mode, online_write_options
            )
        else:
            raise FeatureStoreException(
                "Error writing to offline and online feature store."
            )

    def _save_offline_dataframe(
        self,
        table_name,
        feature_group,
        dataframe,
        save_mode,
        operation,
        write_options,
    ):
        if feature_group.time_travel_format == "HUDI":
            hudi_engine_instance = hudi_engine.HudiEngine(
                feature_group.feature_store_id,
                feature_group.feature_store_name,
                feature_group,
                self._spark_session,
                self._spark_context,
            )
            hudi_engine_instance.save_hudi_fg(
                dataframe, save_mode, operation, write_options
            )
        else:
            dataframe.write.format(self.HIVE_FORMAT).mode(save_mode).options(
                **write_options
            ).partitionBy(
                feature_group.partition_key if feature_group.partition_key else []
            ).saveAsTable(
                table_name
            )

    def _save_online_dataframe(self, table_name, dataframe, save_mode, write_options):
        dataframe.write.format(self.JDBC_FORMAT).mode(save_mode).options(
            **write_options
        ).save()

    def write_training_dataset(
        self, training_dataset, features, user_write_options, save_mode
    ):
        if isinstance(features, query.Query):
            dataset = features.read()
        else:
            dataset = features

        write_options = self.write_options(
            training_dataset.data_format, user_write_options
        )

        if len(training_dataset.splits) == 0:
            path = training_dataset.location + "/" + training_dataset.name
            self._write_training_dataset_single(
                dataset,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                path,
            )
        else:
            split_names = sorted([*training_dataset.splits])
            split_weights = [training_dataset.splits[i] for i in split_names]
            self._write_training_dataset_splits(
                dataset.randomSplit(split_weights, training_dataset.seed),
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                training_dataset.location,
                split_names,
            )

    def _write_training_dataset_splits(
        self,
        feature_dataframe_list,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
        split_names,
    ):
        for i in range(len(feature_dataframe_list)):
            split_path = path + "/" + str(split_names[i])
            self._write_training_dataset_single(
                feature_dataframe_list[i],
                storage_connector,
                data_format,
                write_options,
                save_mode,
                split_path,
            )

    def _write_training_dataset_single(
        self,
        feature_dataframe,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
    ):
        # TODO: currently not supported petastorm, hdf5 and npy file formats
        if data_format.lower() == "tsv":
            data_format = "csv"

        path = self._setup_storage_connector(storage_connector, path)

        feature_dataframe.write.format(data_format).options(**write_options).mode(
            save_mode
        ).save(path)

    def read(self, storage_connector, data_format, read_options, path):

        if data_format.lower() == "tsv":
            data_format = "csv"

        path = self._setup_storage_connector(storage_connector, path)

        return (
            self._spark_session.read.format(data_format)
            .options(**read_options)
            .load(path)
        )

    def profile(self, dataframe, relevant_columns, correlations, histograms):
        """Profile a dataframe with Deequ."""
        return (
            self._jvm.com.logicalclocks.hsfs.engine.SparkEngine.getInstance().profile(
                dataframe._jdf, relevant_columns, correlations, histograms
            )
        )

    def write_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def read_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example", **provided_options)
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true", inferSchema="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true", inferSchema="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def parse_schema_feature_group(self, dataframe):
        return [
            feature.Feature(
                feat.name,
                feat.dataType.simpleString(),
                feat.metadata.get("description", ""),
            )
            for feat in dataframe.schema
        ]

    def parse_schema_training_dataset(self, dataframe):
        return [
            training_dataset_feature.TrainingDatasetFeature(
                feat.name, feat.dataType.simpleString()
            )
            for feat in dataframe.schema
        ]

    def parse_schema_dict(self, dataframe):
        return {
            feat.name: feature.Feature(
                feat.name,
                feat.dataType.simpleString(),
                feat.metadata.get("description", ""),
            )
            for feat in dataframe.schema
        }

    def training_dataset_schema_match(self, dataframe, schema):
        schema_sorted = sorted(schema, key=lambda f: f.index)
        insert_schema = dataframe.schema
        if len(schema_sorted) != len(insert_schema):
            raise SchemaError(
                "Schemas do not match. Expected {} features, the dataframe contains {} features".format(
                    len(schema_sorted), len(insert_schema)
                )
            )

        i = 0
        for feat in schema_sorted:
            if feat.name != insert_schema[i].name:
                raise SchemaError(
                    "Schemas do not match, expected feature {} in position {}, found {}".format(
                        feat.name, str(i), insert_schema[i].name
                    )
                )

            i += 1

    def _setup_storage_connector(self, storage_connector, path=None):
        if storage_connector.connector_type == StorageConnector.S3:
            return self._setup_s3_hadoop_conf(storage_connector, path)
        elif storage_connector.connector_type == StorageConnector.ADLS:
            return self._setup_adls_hadoop_conf(storage_connector, path)
        else:
            return path

    def _setup_s3_hadoop_conf(self, storage_connector, path):
        if storage_connector.access_key:
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.access.key", storage_connector.access_key
            )
        if storage_connector.secret_key:
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.secret.key", storage_connector.secret_key
            )
        if storage_connector.server_encryption_algorithm:
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.server-side-encryption-algorithm",
                storage_connector.server_encryption_algorithm,
            )
        if storage_connector.server_encryption_key:
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.server-side-encryption-key",
                storage_connector.server_encryption_key,
            )
        if storage_connector.session_token:
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.session.token",
                storage_connector.session_token,
            )
        return path.replace("s3", "s3a", 1)

    def _setup_adls_hadoop_conf(self, storage_connector, path):
        for k, v in storage_connector.spark_options().items():
            self._spark_context._jsc.hadoopConfiguration().set(k, v)

        return path

    def _setup_pydoop(self):
        # Import Pydoop only here, so it doesn't trigger if the execution environment
        # does not support Pydoop. E.g. Sagemaker
        from pydoop import hdfs

        # Create a subclass that replaces the check on the hdfs scheme to allow hopsfs as well.
        class _HopsFSPathSplitter(hdfs.path._HdfsPathSplitter):
            @classmethod
            def split(cls, hdfs_path, user):
                if not hdfs_path:
                    cls.raise_bad_path(hdfs_path, "empty")
                scheme, netloc, path = cls.parse(hdfs_path)
                if not scheme:
                    scheme = "file" if hdfs.fs.default_is_local() else "hdfs"
                if scheme == "hdfs" or scheme == "hopsfs":
                    if not path:
                        cls.raise_bad_path(hdfs_path, "path part is empty")
                    if ":" in path:
                        cls.raise_bad_path(
                            hdfs_path, "':' not allowed outside netloc part"
                        )
                    hostname, port = cls.split_netloc(netloc)
                    if not path.startswith("/"):
                        path = "/user/%s/%s" % (user, path)
                elif scheme == "file":
                    hostname, port, path = "", 0, netloc + path
                else:
                    cls.raise_bad_path(hdfs_path, "unsupported scheme %r" % scheme)
                return hostname, port, path

        # Monkey patch the class to use the one defined above.
        hdfs.path._HdfsPathSplitter = _HopsFSPathSplitter


class SchemaError(Exception):
    """Thrown when schemas don't match"""
