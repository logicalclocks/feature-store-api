#
#   Copyright 2022 Hopsworks AB
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

import avro
from typing import Any

# in case importing in %%local
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, from_json
    from pyspark.sql.avro.functions import from_avro
except ImportError:
    pass

from hsfs import util
from hsfs.storage_connector import StorageConnector
from hsfs.engine import engine_base


class EngineRead(engine_base.EngineReadBase):
    def __init__(self):
        self._spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

        if importlib.util.find_spec("pydoop"):
            # If we are on Databricks don't setup Pydoop as it's not available and cannot be easily installed.
            util.setup_pydoop()

    def read(self, storage_connector, data_format, read_options, location) -> Any:
        if isinstance(location, str):
            if data_format.lower() in ["delta", "parquet", "hudi", "orc", "bigquery"]:
                # All the above data format readers can handle partitioning
                # by their own, they don't need /**
                # for bigquery, argument location can be a SQL query
                path = location
            else:
                path = location + "/**"

            if data_format.lower() == "tsv":
                data_format = "csv"

        else:
            path = None

        path = self.setup_storage_connector(storage_connector, path)

        return (
            self._spark_session.read.format(data_format)
            .options(**(read_options if read_options else {}))
            .load(path)
        )

    def read_options(self, data_format, provided_options) -> Any:
        if provided_options is None:
            provided_options = {}
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

    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ) -> Any:
        # ideally all this logic should be in the storage connector in case we add more
        # streaming storage connectors...
        stream = self._spark_session.readStream.format(storage_connector.SPARK_FORMAT)

        # set user options last so that they overwrite any default options
        stream = stream.options(**storage_connector.spark_options(), **options)

        if storage_connector.type == StorageConnector.KAFKA:
            return self._read_stream_kafka(
                stream, message_format, schema, include_metadata
            )

    def show(self, sql_query, feature_store, n, online_conn) -> Any:
        return self.sql(sql_query, feature_store, online_conn, "default", {}).show(n)

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name) -> Any:
        unique_values = feature_dataframe.select(feature_name).distinct().collect()
        return [field[feature_name] for field in unique_values]

    def get_training_data(
        self, training_dataset_obj, feature_view_obj, query_obj, read_options
    ) -> Any:
        df = query_obj.read(read_options=read_options)
        return self.write_training_dataset(
            training_dataset_obj,
            df,
            read_options,
            None,
            to_df=True,
            feature_view_obj=feature_view_obj,
        )

    def get_empty_appended_dataframe(self, dataframe, new_features) -> Any:
        dataframe = dataframe.limit(0)
        for f in new_features:
            dataframe = dataframe.withColumn(f.name, lit(None).cast(f.type))
        return dataframe

    def _read_stream_kafka(self, stream, message_format, schema, include_metadata):
        kafka_cols = [
            col("key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("timestampType"),
        ]

        if message_format == "avro" and schema is not None:
            # check if vallid avro schema
            avro.schema.parse(schema)
            df = stream.load()
            if include_metadata is True:
                return df.select(
                    *kafka_cols, from_avro(df.value, schema).alias("value")
                ).select(*kafka_cols, col("value.*"))
            return df.select(from_avro(df.value, schema).alias("value")).select(
                col("value.*")
            )
        elif message_format == "json" and schema is not None:
            df = stream.load()
            if include_metadata is True:
                return df.select(
                    *kafka_cols,
                    from_json(df.value.cast("string"), schema).alias("value"),
                ).select(*kafka_cols, col("value.*"))
            return df.select(
                from_json(df.value.cast("string"), schema).alias("value")
            ).select(col("value.*"))

        if include_metadata is True:
            return stream.load()
        return stream.load().select("key", "value")
