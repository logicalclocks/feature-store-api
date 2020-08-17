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

import pandas as pd
import numpy as np

# in case importing in %%local
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.rdd import RDD
except ModuleNotFoundError:
    pass

from hsfs import feature
from hsfs.storage_connector import StorageConnector
from hsfs.client.exceptions import FeatureStoreException


class Engine:
    HIVE_FORMAT = "hive"
    JDBC_FORMAT = "jdbc"

    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()
        self._spark_context = self._spark_session.sparkContext

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    def sql(self, sql_query, feature_store, online_conn, dataframe_type):
        if not online_conn:
            result_df = self._sql_offline(sql_query, feature_store)
        else:
            result_df = self._sql_online(sql_query, online_conn)

        self.set_job_group("", "")
        return self._return_dataframe_type(result_df, dataframe_type)

    def _sql_offline(self, sql_query, feature_store):
        # set feature store
        self._spark_session.sql("USE {}".format(feature_store))
        return self._spark_session.sql(sql_query)

    def _sql_online(self, sql_query, online_conn):
        options = online_conn.spark_options()
        options["query"] = sql_query

        return (
            self._spark_session.read.format(self.JDBC_FORMAT).options(**options).load()
        )

    def show(self, sql_query, feature_store, n, online_conn):
        return self.sql(sql_query, feature_store, online_conn, "default").show(n)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)

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
        partition_columns,
        dataframe,
        save_mode,
        storage,
        offline_write_options,
        online_write_options,
    ):
        if storage == "offline":
            self._save_offline_dataframe(
                table_name,
                partition_columns,
                dataframe,
                save_mode,
                offline_write_options,
            )
        elif storage == "online":
            self._save_online_dataframe(
                table_name, dataframe, save_mode, online_write_options
            )
        elif storage == "all":
            self._save_offline_dataframe(
                table_name,
                partition_columns,
                dataframe,
                save_mode,
                offline_write_options,
            )
            self._save_online_dataframe(
                table_name, dataframe, save_mode, online_write_options
            )
        else:
            raise FeatureStoreException("Storage not supported")

    def _save_offline_dataframe(
        self, table_name, partition_columns, dataframe, save_mode, write_options
    ):
        dataframe.write.format(self.HIVE_FORMAT).mode(save_mode).options(
            **write_options
        ).partitionBy(partition_columns if partition_columns else []).saveAsTable(
            table_name
        )

    def _save_online_dataframe(self, table_name, dataframe, save_mode, write_options):
        dataframe.write.format(self.JDBC_FORMAT).mode(save_mode).options(
            **write_options
        ).save()

    def write(
        self, dataframe, storage_connector, data_format, write_mode, write_options, path
    ):
        if data_format.lower() == "tsv":
            data_format = "csv"

        if storage_connector.connector_type == StorageConnector.S3:
            path = self._setup_s3(storage_connector, path)

        dataframe.write.format(data_format).options(**write_options).mode(
            write_mode
        ).save(path)

    def read(self, storage_connector, data_format, read_options, path):

        if data_format.lower() == "tsv":
            data_format = "csv"

        if storage_connector.connector_type == StorageConnector.S3:
            path = self._setup_s3(storage_connector, path)
        return (
            self._spark_session.read.format(data_format)
            .options(**read_options)
            .load(path)
        )

    def write_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
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

    def parse_schema(self, dataframe):
        return [
            feature.Feature(
                feat.name,
                feat.dataType.simpleString(),
                feat.metadata.get("description", ""),
            )
            for feat in dataframe.schema
        ]

    def parse_schema_dict(self, dataframe):
        return {
            feat["name"]: feature.Feature(
                feat.name,
                feat.dataType.simpleString(),
                feat.metadata.get("description", ""),
            )
            for feat in dataframe.schema
        }

    def schema_matches(self, dataframe, schema):
        # This does not respect order, for that we would need to make sure the features in the
        # list coming from the backend are ordered correctly
        insert_schema = self.parse_schema_dict(dataframe)
        for feat in schema:
            insert_feat = insert_schema.pop(feat.name, False)
            if insert_feat:
                if insert_feat.type == feat.type:
                    pass
            else:
                raise SchemaError(
                    "Schemas do not match, could not find feature {} among the data to be inserted.".format()
                )

    def _setup_s3(self, storage_connector, path):
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
        return path.replace("s3", "s3a", 1)


class SchemaError(Exception):
    """Thrown when schemas don't match"""
