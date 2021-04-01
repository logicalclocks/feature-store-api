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
import json
import datetime

import numpy as np
import pandas as pd

# in case importing in %%local
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.rdd import RDD
    from pyspark.sql.column import Column, _to_java_column
    from pyspark.sql.functions import struct, concat, col
except ImportError:
    pass

from hsfs import feature, training_dataset_feature, client
from hsfs.storage_connector import StorageConnector
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import hudi_engine
from hsfs.constructor import query


class Engine:
    HIVE_FORMAT = "hive"
    JDBC_FORMAT = "jdbc"
    KAFKA_FORMAT = "kafka"
    SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

    APPEND = "append"
    OVERWRITE = "overwrite"

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

    def _snowflake(self, sql_query, connector):
        options = connector.spark_options()
        if sql_query:
            options["query"] = sql_query

        return (
            self._spark_session.read.format(self.SNOWFLAKE_FORMAT)
            .options(**options)
            .load()
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
        elif on_demand_fg.storage_connector.connector_type == "SNOWFLAKE":
            on_demand_dataset = self._snowflake(
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
            dataframe = self._spark_session.createDataFrame(dataframe)
        elif isinstance(dataframe, list):
            dataframe = np.array(dataframe)
        elif isinstance(dataframe, np.ndarray):
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
            dataframe = self._spark_session.createDataFrame(pandas_df)
        elif isinstance(dataframe, RDD):
            dataframe = dataframe.toDF()

        if isinstance(dataframe, DataFrame):
            return dataframe.select(
                [col(x).alias(x.lower()) for x in dataframe.columns]
            )

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, "
            "pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: {}".format(
                type(dataframe)
            )
        )

    def save_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
        validation_id=None,
    ):

        if storage == "offline" or not online_enabled:
            self._save_offline_dataframe(
                feature_group,
                dataframe,
                operation,
                offline_write_options,
                validation_id,
            )
        elif storage == "online":
            self._save_online_dataframe(feature_group, dataframe, online_write_options)
        elif online_enabled and storage is None:
            self._save_offline_dataframe(
                feature_group,
                dataframe,
                operation,
                offline_write_options,
            )
            self._save_online_dataframe(feature_group, dataframe, online_write_options)
        else:
            raise FeatureStoreException(
                "Error writing to offline and online feature store."
            )

    def save_stream_dataframe(
        self,
        feature_group,
        dataframe,
        query_name,
        output_mode,
        await_termination,
        timeout,
        write_options,
    ):
        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )
        if query_name is None:
            query_name = (
                "insert_stream_"
                + feature_group._online_topic_name
                + "_"
                + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            )

        query = (
            serialized_df.writeStream.outputMode(output_mode)
            .format(self.KAFKA_FORMAT)
            .options(**write_options)
            .option(
                "checkpointLocation",
                "/Projects/"
                + client.get_instance()._project_name
                + "/Resources/"
                + query_name
                + "-checkpoint",
            )
            .option("topic", feature_group._online_topic_name)
            .queryName(query_name)
            .start()
        )

        if await_termination:
            query.awaitTermination(timeout)

        return query

    def _save_offline_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        write_options,
        validation_id=None,
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
                dataframe, self.APPEND, operation, write_options, validation_id
            )
        else:
            dataframe.write.format(self.HIVE_FORMAT).mode(self.APPEND).options(
                **write_options
            ).partitionBy(
                feature_group.partition_key if feature_group.partition_key else []
            ).saveAsTable(
                feature_group._get_table_name()
            )

    def _save_online_dataframe(self, feature_group, dataframe, write_options):

        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )
        serialized_df.write.format(self.KAFKA_FORMAT).options(**write_options).option(
            "topic", feature_group._online_topic_name
        ).save()

    def _encode_complex_features(self, feature_group, dataframe):
        """Encodes all complex type features to binary using their avro type as schema."""
        return dataframe.select(
            [
                field["name"]
                if field["name"] not in feature_group.get_complex_features()
                else self.to_avro(
                    field["name"], feature_group._get_feature_avro_schema(field["name"])
                ).alias(field["name"])
                for field in json.loads(feature_group.avro_schema)["fields"]
            ]
        )

    def _online_fg_to_avro(self, feature_group, dataframe):
        """Packs all features into named struct to be serialized to single avro/binary
        column. And packs primary key into arry to be serialized for partitioning.
        """
        return dataframe.select(
            [
                # be aware: primary_key array should always be sorted
                self.to_avro(
                    concat(
                        *[
                            col(f).cast("string")
                            for f in sorted(feature_group.primary_key)
                        ]
                    )
                ).alias("key"),
                self.to_avro(
                    struct(
                        [
                            field["name"]
                            for field in json.loads(feature_group.avro_schema)["fields"]
                        ]
                    ),
                    feature_group._get_encoded_avro_schema(),
                ).alias("value"),
            ]
        )

    def write_training_dataset(
        self, training_dataset, dataset, user_write_options, save_mode
    ):
        if isinstance(dataset, query.Query):
            dataset = dataset.read()

        dataset = self.convert_to_default_dataframe(dataset)
        if training_dataset.coalesce:
            dataset = dataset.coalesce(1)

        self.training_dataset_schema_match(dataset, training_dataset.schema)
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

    def validate(self, dataframe, expectations):
        """Run data validation on the dataframe with Deequ."""

        expectations_java = []
        for expectation in expectations:
            rules = []
            for rule in expectation.rules:
                rules.append(
                    self._jvm.com.logicalclocks.hsfs.metadata.validation.Rule.builder()
                    .name(
                        self._jvm.com.logicalclocks.hsfs.metadata.validation.RuleName.valueOf(
                            rule.get("name")
                        )
                    )
                    .level(
                        self._jvm.com.logicalclocks.hsfs.metadata.validation.Level.valueOf(
                            rule.get("level")
                        )
                    )
                    .min(rule.get("min", None))
                    .max(rule.get("max", None))
                    .pattern(rule.get("pattern", None))
                    .acceptedType(
                        self._jvm.com.logicalclocks.hsfs.metadata.validation.AcceptedType.valueOf(
                            rule.get("accepted_type")
                        )
                        if rule.get("accepted_type") is not None
                        else None
                    )
                    .legalValues(rule.get("legal_values", None))
                    .build()
                )
            expectation = (
                self._jvm.com.logicalclocks.hsfs.metadata.Expectation.builder()
                .name(expectation.name)
                .description(expectation.description)
                .features(expectation.features)
                .rules(rules)
                .build()
            )
            expectations_java.append(expectation)

        return self._jvm.com.logicalclocks.hsfs.engine.DataValidationEngine.getInstance().validate(
            dataframe._jdf, expectations_java
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
                feat.name.lower(),
                feat.dataType.simpleString(),
                feat.metadata.get("description", ""),
            )
            for feat in dataframe.schema
        ]

    def parse_schema_training_dataset(self, dataframe):
        return [
            training_dataset_feature.TrainingDatasetFeature(
                feat.name.lower(), feat.dataType.simpleString()
            )
            for feat in dataframe.schema
        ]

    def parse_schema_dict(self, dataframe):
        return {
            feat.name: feature.Feature(
                feat.name.lower(),
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
            if feat.name != insert_schema[i].name.lower():
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

    def is_spark_dataframe(self, dataframe):
        if isinstance(dataframe, DataFrame):
            return True
        return False

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

    def _print_missing_jar(self, lib_name, pkg_name, jar_name, spark_version):
        #
        # Licensed to the Apache Software Foundation (ASF) under one or more
        # contributor license agreements.  See the NOTICE file distributed with
        # this work for additional information regarding copyright ownership.
        # The ASF licenses this file to You under the Apache License, Version 2.0
        # (the "License"); you may not use this file except in compliance with
        # the License.  You may obtain a copy of the License at
        #
        #    http://www.apache.org/licenses/LICENSE-2.0
        #
        # Unless required by applicable law or agreed to in writing, software
        # distributed under the License is distributed on an "AS IS" BASIS,
        # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        # See the License for the specific language governing permissions and
        # limitations under the License.
        #

        # This function is a copy from https://github.com/apache/spark/blob/master/python/pyspark/util.py
        # In accordance with the Apache License Version 2.0, the following changes have been made:
        # - the function was made a method of a class

        print(
            """
    ________________________________________________________________________________________________
    Spark %(lib_name)s libraries not found in class path. Try one of the following.
    1. Include the %(lib_name)s library and its dependencies with in the
        spark-submit command as
        $ bin/spark-submit --packages org.apache.spark:spark-%(pkg_name)s:%(spark_version)s ...
    2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
        Group Id = org.apache.spark, Artifact Id = spark-%(jar_name)s, Version = %(spark_version)s.
        Then, include the jar in the spark-submit command as
        $ bin/spark-submit --jars <spark-%(jar_name)s.jar> ...
    ________________________________________________________________________________________________
    """
            % {
                "lib_name": lib_name,
                "pkg_name": pkg_name,
                "jar_name": jar_name,
                "spark_version": spark_version,
            }
        )

    def from_avro(self, data, jsonFormatSchema, options={}):
        """
        Converts a binary column of avro format into its corresponding catalyst value. The specified
        schema must match the read data, otherwise the behavior is undefined: it may fail or return
        arbitrary result.
        Note: Avro is built-in but external data source module since Spark 2.4. Please deploy the
        application as per the deployment section of "Apache Avro Data Source Guide".
        :param data: the binary column.
        :param jsonFormatSchema: the avro schema in JSON string format.
        :param options: options to control how the Avro record is parsed.
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.avro.functions import from_avro, to_avro
        >>> data = [(1, Row(name='Alice', age=2))]
        >>> df = spark.createDataFrame(data, ("key", "value"))
        >>> avroDf = df.select(to_avro(df.value).alias("avro"))
        >>> avroDf.collect()
        [Row(avro=bytearray(b'\\x00\\x00\\x04\\x00\\nAlice'))]
        >>> jsonFormatSchema = '''{"type":"record","name":"topLevelRecord","fields":
        ...     [{"name":"avro","type":[{"type":"record","name":"value","namespace":"topLevelRecord",
        ...     "fields":[{"name":"age","type":["long","null"]},
        ...     {"name":"name","type":["string","null"]}]},"null"]}]}'''
        >>> avroDf.select(from_avro(avroDf.avro, jsonFormatSchema).alias("value")).collect()
        [Row(value=Row(avro=Row(age=2, name=u'Alice')))]
        """

        #
        # Licensed to the Apache Software Foundation (ASF) under one or more
        # contributor license agreements.  See the NOTICE file distributed with
        # this work for additional information regarding copyright ownership.
        # The ASF licenses this file to You under the Apache License, Version 2.0
        # (the "License"); you may not use this file except in compliance with
        # the License.  You may obtain a copy of the License at
        #
        #    http://www.apache.org/licenses/LICENSE-2.0
        #
        # Unless required by applicable law or agreed to in writing, software
        # distributed under the License is distributed on an "AS IS" BASIS,
        # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        # See the License for the specific language governing permissions and
        # limitations under the License.
        #

        # This function is a copy from https://github.com/apache/spark/blob/master/python/pyspark/sql/avro/functions.py
        # In accordance with the Apache License Version 2.0, the following changes have been made:
        # - the function was made a method of a class

        try:
            jc = self._jvm.org.apache.spark.sql.avro.functions.from_avro(
                _to_java_column(data), jsonFormatSchema, options
            )
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                self._print_missing_jar(
                    "Avro", "avro", "avro", self._spark_context.version
                )
            raise
        return Column(jc)

    def to_avro(self, data, jsonFormatSchema=""):
        """
        Converts a column into binary of avro format.
        Note: Avro is built-in but external data source module since Spark 2.4. Please deploy the
        application as per the deployment section of "Apache Avro Data Source Guide".
        :param data: the data column.
        :param jsonFormatSchema: user-specified output avro schema in JSON string format.
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.avro.functions import to_avro
        >>> data = ['SPADES']
        >>> df = spark.createDataFrame(data, "string")
        >>> df.select(to_avro(df.value).alias("suite")).collect()
        [Row(suite=bytearray(b'\\x00\\x0cSPADES'))]
        >>> jsonFormatSchema = '''["null", {"type": "enum", "name": "value",
        ...     "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}]'''
        >>> df.select(to_avro(df.value, jsonFormatSchema).alias("suite")).collect()
        [Row(suite=bytearray(b'\\x02\\x00'))]
        """

        #
        # Licensed to the Apache Software Foundation (ASF) under one or more
        # contributor license agreements.  See the NOTICE file distributed with
        # this work for additional information regarding copyright ownership.
        # The ASF licenses this file to You under the Apache License, Version 2.0
        # (the "License"); you may not use this file except in compliance with
        # the License.  You may obtain a copy of the License at
        #
        #    http://www.apache.org/licenses/LICENSE-2.0
        #
        # Unless required by applicable law or agreed to in writing, software
        # distributed under the License is distributed on an "AS IS" BASIS,
        # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        # See the License for the specific language governing permissions and
        # limitations under the License.
        #

        # This function is a copy from https://github.com/apache/spark/blob/master/python/pyspark/sql/avro/functions.py
        # In accordance with the Apache License Version 2.0, the following changes have been made:
        # - the function was made a method of a class

        try:
            if jsonFormatSchema == "":
                jc = self._jvm.org.apache.spark.sql.avro.functions.to_avro(
                    _to_java_column(data)
                )
            else:
                jc = self._jvm.org.apache.spark.sql.avro.functions.to_avro(
                    _to_java_column(data), jsonFormatSchema
                )
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                self._print_missing_jar(
                    "Avro", "avro", "avro", self._spark_context.version
                )
            raise
        return Column(jc)


class SchemaError(Exception):
    """Thrown when schemas don't match"""
