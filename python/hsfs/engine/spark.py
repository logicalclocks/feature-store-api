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
import shutil
import json
import copy
import importlib.util
import re
import warnings
from typing import Optional, TypeVar

import numpy as np
import pandas as pd
import avro
from datetime import datetime, timezone

import tzlocal

# in case importing in %%local

try:
    import pyspark
    from pyspark import SparkFiles
    from pyspark.sql import SparkSession, DataFrame, SQLContext
    from pyspark.rdd import RDD
    from pyspark.sql.functions import (
        struct,
        concat,
        col,
        lit,
        array,
        from_json,
        udf,
    )
    from pyspark.sql.avro.functions import from_avro, to_avro
    from pyspark.sql.types import (
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType,
        DateType,
        StringType,
        TimestampType,
        ArrayType,
        StructType,
        BinaryType,
        BooleanType,
        StructField,
    )

    if pd.__version__ >= "2.0.0" and pyspark.__version__ < "3.2.3":

        def iteritems(self):
            return self.items()

        setattr(pd.DataFrame, "iteritems", iteritems)
except ImportError:
    pass

from hsfs import feature, training_dataset_feature, client, util
from hsfs.feature_group import ExternalFeatureGroup, SpineGroup
from hsfs.storage_connector import StorageConnector
from hsfs.client.exceptions import FeatureStoreException
from hsfs.client import hopsworks
from hsfs.core import (
    hudi_engine,
    transformation_function_engine,
    storage_connector_api,
    dataset_api,
)
from hsfs.constructor import query
from hsfs.training_dataset_split import TrainingDatasetSplit

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


class Engine:
    HIVE_FORMAT = "hive"
    KAFKA_FORMAT = "kafka"

    APPEND = "append"
    OVERWRITE = "overwrite"

    def __init__(self):
        self._spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
        self._spark_session.conf.set("spark.sql.session.timeZone", "UTC")

        if importlib.util.find_spec("pydoop"):
            # If we are on Databricks don't setup Pydoop as it's not available and cannot be easily installed.
            util.setup_pydoop()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()
        self._dataset_api = dataset_api.DatasetApi()

    def sql(
        self,
        sql_query,
        feature_store,
        connector,
        dataframe_type,
        read_options,
        schema=None,
    ):
        if not connector:
            result_df = self._sql_offline(sql_query, feature_store)
        else:
            result_df = connector.read(sql_query, None, {}, None)

        self.set_job_group("", "")
        return self._return_dataframe_type(result_df, dataframe_type)

    def is_flyingduck_query_supported(self, query, read_options={}):
        return False  # we do not support flyingduck on pyspark clients

    def _sql_offline(self, sql_query, feature_store):
        # set feature store
        self._spark_session.sql("USE {}".format(feature_store))
        return self._spark_session.sql(sql_query)

    def show(self, sql_query, feature_store, n, online_conn, read_options={}):
        return self.sql(
            sql_query, feature_store, online_conn, "default", read_options
        ).show(n)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)

    def register_external_temporary_table(self, external_fg, alias):
        if not isinstance(external_fg, SpineGroup):
            external_dataset = external_fg.storage_connector.read(
                external_fg.query,
                external_fg.data_format,
                external_fg.options,
                external_fg.storage_connector._get_path(external_fg.path),
            )
        else:
            external_dataset = external_fg.dataframe
        if external_fg.location:
            self._spark_session.sparkContext.textFile(external_fg.location).collect()

        external_dataset.createOrReplaceTempView(alias)
        return external_dataset

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
            hudi_fg_alias,
            read_options,
        )
        hudi_engine_instance.reconcile_hudi_schema(
            self.save_empty_dataframe, hudi_fg_alias, read_options
        )

    def _return_dataframe_type(self, dataframe, dataframe_type):
        if dataframe_type.lower() in ["default", "spark"]:
            return dataframe
        if dataframe_type.lower() == "pandas":
            return dataframe.toPandas()
        if dataframe_type.lower() == "numpy":
            return dataframe.toPandas().values
        if dataframe_type.lower() == "python":
            return dataframe.toPandas().values.tolist()

        raise TypeError(
            "Dataframe type `{}` not supported on this platform.".format(dataframe_type)
        )

    def convert_to_default_dataframe(self, dataframe):
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
            dataframe = pd.DataFrame(dataframe_dict)

        if isinstance(dataframe, pd.DataFrame):
            # convert timestamps to current timezone
            local_tz = tzlocal.get_localzone()
            # make shallow copy so the original df does not get changed
            dataframe_copy = dataframe.copy(deep=False)
            for c in dataframe_copy.columns:
                if isinstance(
                    dataframe_copy[c].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype
                ):
                    # convert to utc timestamp
                    dataframe_copy[c] = dataframe_copy[c].dt.tz_convert(None)
                if dataframe_copy[c].dtype == np.dtype("datetime64[ns]"):
                    # set the timezone to the client's timezone because that is
                    # what spark expects.
                    dataframe_copy[c] = dataframe_copy[c].dt.tz_localize(
                        str(local_tz), ambiguous="infer", nonexistent="shift_forward"
                    )
            dataframe = self._spark_session.createDataFrame(dataframe_copy)
        elif isinstance(dataframe, RDD):
            dataframe = dataframe.toDF()

        if isinstance(dataframe, DataFrame):
            upper_case_features = [
                c for c in dataframe.columns if any(re.finditer("[A-Z]", c))
            ]
            if len(upper_case_features) > 0:
                warnings.warn(
                    "The ingested dataframe contains upper case letters in feature names: `{}`. "
                    "Feature names are sanitized to lower case in the feature store.".format(
                        upper_case_features
                    ),
                    util.FeatureGroupWarning,
                )

            lowercase_dataframe = dataframe.select(
                [col(x).alias(x.lower()) for x in dataframe.columns]
            )
            # for streaming dataframes this will be handled in DeltaStreamerTransformer.java class
            if not lowercase_dataframe.isStreaming:
                nullable_schema = copy.deepcopy(lowercase_dataframe.schema)
                for struct_field in nullable_schema:
                    struct_field.nullable = True
                lowercase_dataframe = self._spark_session.createDataFrame(
                    lowercase_dataframe.rdd, nullable_schema
                )

            return lowercase_dataframe
        if dataframe == "spine":
            return None

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
        try:
            if (
                isinstance(feature_group, ExternalFeatureGroup)
                and feature_group.online_enabled
            ) or feature_group.stream:
                self._save_online_dataframe(
                    feature_group, dataframe, online_write_options
                )
            else:
                if storage == "offline" or not online_enabled:
                    self._save_offline_dataframe(
                        feature_group,
                        dataframe,
                        operation,
                        offline_write_options,
                        validation_id,
                    )
                elif storage == "online":
                    self._save_online_dataframe(
                        feature_group, dataframe, online_write_options
                    )
                elif online_enabled and storage is None:
                    self._save_offline_dataframe(
                        feature_group,
                        dataframe,
                        operation,
                        offline_write_options,
                    )
                    self._save_online_dataframe(
                        feature_group, dataframe, online_write_options
                    )
        except Exception as e:
            raise FeatureStoreException(e).with_traceback(e.__traceback__)

    def save_stream_dataframe(
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
        write_options = self._get_kafka_config(
            feature_group.feature_store_id, write_options
        )
        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )

        if query_name is None:
            query_name = "insert_stream_" + feature_group._online_topic_name

        project_id = str(feature_group.feature_store.project_id).encode("utf8")
        feature_group_id = str(feature_group._id).encode("utf8")
        subject_id = str(feature_group.subject["id"]).encode("utf8")

        query = (
            serialized_df.withColumn(
                "headers",
                array(
                    struct(
                        lit("projectId").alias("key"), lit(project_id).alias("value")
                    ),
                    struct(
                        lit("featureGroupId").alias("key"),
                        lit(feature_group_id).alias("value"),
                    ),
                    struct(
                        lit("subjectId").alias("key"), lit(subject_id).alias("value")
                    ),
                ),
            )
            .writeStream.outputMode(output_mode)
            .format(self.KAFKA_FORMAT)
            .option(
                "checkpointLocation",
                "/Projects/"
                + client.get_instance()._project_name
                + "/Resources/"
                + query_name
                + "-checkpoint"
                if checkpoint_dir is None
                else checkpoint_dir,
            )
            .options(**write_options)
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
        write_options = self._get_kafka_config(
            feature_group.feature_store_id, write_options
        )

        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )

        project_id = str(feature_group.feature_store.project_id).encode("utf8")
        feature_group_id = str(feature_group._id).encode("utf8")
        subject_id = str(feature_group.subject["id"]).encode("utf8")

        serialized_df.withColumn(
            "headers",
            array(
                struct(lit("projectId").alias("key"), lit(project_id).alias("value")),
                struct(
                    lit("featureGroupId").alias("key"),
                    lit(feature_group_id).alias("value"),
                ),
                struct(lit("subjectId").alias("key"), lit(subject_id).alias("value")),
            ),
        ).write.format(self.KAFKA_FORMAT).options(**write_options).option(
            "topic", feature_group._online_topic_name
        ).save()

    def _encode_complex_features(self, feature_group, dataframe):
        """Encodes all complex type features to binary using their avro type as schema."""
        return dataframe.select(
            [
                field["name"]
                if field["name"] not in feature_group.get_complex_features()
                else to_avro(
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
                to_avro(
                    concat(
                        *[
                            col(f).cast("string")
                            for f in sorted(feature_group.primary_key)
                        ]
                    )
                ).alias("key"),
                to_avro(
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

    def get_training_data(
        self, training_dataset, feature_view_obj, query_obj, read_options
    ):
        return self.write_training_dataset(
            training_dataset,
            query_obj,
            read_options,
            None,
            read_options=read_options,
            to_df=True,
            feature_view_obj=feature_view_obj,
        )

    def split_labels(self, df, labels):
        if labels:
            labels_df = df.select(*labels)
            df_new = df.drop(*labels)
            return df_new, labels_df
        else:
            return df, None

    def write_training_dataset(
        self,
        training_dataset,
        query_obj,
        user_write_options,
        save_mode,
        read_options={},
        feature_view_obj=None,
        to_df=False,
    ):
        write_options = self.write_options(
            training_dataset.data_format, user_write_options
        )

        if len(training_dataset.splits) == 0:
            if isinstance(query_obj, query.Query):
                dataset = self.convert_to_default_dataframe(
                    query_obj.read(read_options=read_options)
                )
            else:
                raise ValueError("Dataset should be a query.")

            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset, feature_view_obj, dataset
            )
            if training_dataset.coalesce:
                dataset = dataset.coalesce(1)
            path = training_dataset.location + "/" + training_dataset.name
            return self._write_training_dataset_single(
                training_dataset.transformation_functions,
                dataset,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                path,
                to_df=to_df,
            )
        else:
            split_dataset = self._split_df(
                query_obj, training_dataset, read_options=read_options
            )
            for key in split_dataset:
                if training_dataset.coalesce:
                    split_dataset[key] = split_dataset[key].coalesce(1)

                split_dataset[key] = split_dataset[key].cache()

            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset, feature_view_obj, split_dataset
            )
            return self._write_training_dataset_splits(
                training_dataset, split_dataset, write_options, save_mode, to_df=to_df
            )

    def _split_df(self, query_obj, training_dataset, read_options={}):
        if (
            training_dataset.splits[0].split_type
            == TrainingDatasetSplit.TIME_SERIES_SPLIT
        ):
            event_time = query_obj._left_feature_group.event_time
            if event_time not in [_feature.name for _feature in query_obj.features]:
                query_obj.append_feature(
                    query_obj._left_feature_group.__getattr__(event_time)
                )
                return self._time_series_split(
                    training_dataset,
                    query_obj.read(read_options=read_options),
                    event_time,
                    drop_event_time=True,
                )
            else:
                return self._time_series_split(
                    training_dataset,
                    query_obj.read(read_options=read_options),
                    event_time,
                )
        else:
            return self._random_split(
                query_obj.read(read_options=read_options), training_dataset
            )

    def _random_split(self, dataset, training_dataset):
        splits = [(split.name, split.percentage) for split in training_dataset.splits]
        split_weights = [split[1] for split in splits]
        split_dataset = dataset.randomSplit(split_weights, training_dataset.seed)
        return dict([(split[0], split_dataset[i]) for i, split in enumerate(splits)])

    def _time_series_split(
        self, training_dataset, dataset, event_time, drop_event_time=False
    ):
        # registering the UDF
        _convert_event_time_to_timestamp = udf(
            util.convert_event_time_to_timestamp, LongType()
        )

        result_dfs = {}
        ts_col = _convert_event_time_to_timestamp(col(event_time))
        for split in training_dataset.splits:
            result_df = dataset.filter(ts_col >= split.start_time).filter(
                ts_col < split.end_time
            )
            if drop_event_time:
                result_df = result_df.drop(event_time)
            result_dfs[split.name] = result_df
        return result_dfs

    def _write_training_dataset_splits(
        self,
        training_dataset,
        feature_dataframes,
        write_options,
        save_mode,
        to_df=False,
    ):
        for split_name, feature_dataframe in feature_dataframes.items():
            split_path = training_dataset.location + "/" + str(split_name)
            feature_dataframes[split_name] = self._write_training_dataset_single(
                training_dataset.transformation_functions,
                feature_dataframes[split_name],
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                split_path,
                to_df=to_df,
            )

        if to_df:
            return feature_dataframes

    def _write_training_dataset_single(
        self,
        transformation_functions,
        feature_dataframe,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
        to_df=False,
    ):
        # apply transformation functions (they are applied separately to each split)
        feature_dataframe = self._apply_transformation_function(
            transformation_functions, dataset=feature_dataframe
        )
        if to_df:
            return feature_dataframe
        # TODO: currently not supported petastorm, hdf5 and npy file formats
        if data_format.lower() == "tsv":
            data_format = "csv"

        path = self.setup_storage_connector(storage_connector, path)

        feature_dataframe.write.format(data_format).options(**write_options).mode(
            save_mode
        ).save(path)

        feature_dataframe.unpersist()

    def read(self, storage_connector, data_format, read_options, location):
        if not data_format:
            raise FeatureStoreException("data_format is not specified")

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

    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ):
        # ideally all this logic should be in the storage connector in case we add more
        # streaming storage connectors...
        stream = self._spark_session.readStream.format(
            storage_connector.SPARK_FORMAT
        )  # todo SPARK_FORMAT available only for KAFKA connectors

        # set user options last so that they overwrite any default options
        stream = stream.options(**storage_connector.spark_options(), **options)

        if storage_connector.type == StorageConnector.KAFKA:
            return self._read_stream_kafka(
                stream, message_format, schema, include_metadata
            )

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

    def add_file(self, file):
        if not file:
            return file

        # This is used for unit testing
        if not file.startswith("file://"):
            file = "hdfs://" + file

        file_name = os.path.basename(file)

        # for external clients, download the file
        if isinstance(client.get_instance(), client.external.Client):
            tmp_file = os.path.join(SparkFiles.getRootDirectory(), file_name)
            print("Reading key file from storage connector.")
            response = self._dataset_api.read_content(tmp_file, "HIVEDB")

            with open(tmp_file, "wb") as f:
                f.write(response.content)
        else:
            self._spark_context.addFile(file)

            # The file is not added to the driver current working directory
            # We should add it manually by copying from the download location
            # The file will be added to the executors current working directory
            # before the next task is executed
            shutil.copy(SparkFiles.get(file_name), file_name)

        return file_name

    def profile(
        self,
        dataframe,
        relevant_columns,
        correlations,
        histograms,
        exact_uniqueness=True,
    ):
        """Profile a dataframe with Deequ."""
        return self._jvm.com.logicalclocks.hsfs.spark.engine.SparkEngine.getInstance().profile(
            dataframe._jdf,
            relevant_columns,
            correlations,
            histograms,
            exact_uniqueness,
        )

    def validate_with_great_expectations(
        self,
        dataframe: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        expectation_suite: TypeVar("ge.core.ExpectationSuite"),  # noqa: F821
        ge_validate_kwargs: Optional[dict],
    ):
        # NOTE: InMemoryStoreBackendDefaults SHOULD NOT BE USED in normal settings. You
        # may experience data loss as it persists nothing. It is used here for testing.
        # Please refer to docs to learn how to instantiate your DataContext.
        store_backend_defaults = InMemoryStoreBackendDefaults()
        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
        )
        context = BaseDataContext(project_config=data_context_config)

        datasource = {
            "name": "my_spark_dataframe",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine",
                "force_reuse_spark_context": True,
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            },
        }
        context.add_datasource(**datasource)

        # Here is a RuntimeBatchRequest using a dataframe
        batch_request = RuntimeBatchRequest(
            datasource_name="my_spark_dataframe",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
            batch_identifiers={"batch_id": "default_identifier"},
            runtime_parameters={"batch_data": dataframe},  # Your dataframe goes here
        )
        context.save_expectation_suite(expectation_suite)
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite.expectation_suite_name,
        )
        report = validator.validate(**ge_validate_kwargs)

        return report

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

    def parse_schema_feature_group(self, dataframe, time_travel_format=None):
        features = []
        using_hudi = time_travel_format == "HUDI"
        for feat in dataframe.schema:
            name = feat.name.lower()
            try:
                converted_type = Engine.convert_spark_type_to_offline_type(
                    feat.dataType, using_hudi
                )
            except ValueError as e:
                raise FeatureStoreException(f"Feature '{name}': {str(e)}")
            features.append(
                feature.Feature(
                    name, converted_type, feat.metadata.get("description", None)
                )
            )
        return features

    def parse_schema_training_dataset(self, dataframe):
        return [
            training_dataset_feature.TrainingDatasetFeature(
                feat.name.lower(), feat.dataType.simpleString()
            )
            for feat in dataframe.schema
        ]

    def setup_storage_connector(self, storage_connector, path=None):
        if storage_connector.type == StorageConnector.S3:
            return self._setup_s3_hadoop_conf(storage_connector, path)
        elif storage_connector.type == StorageConnector.ADLS:
            return self._setup_adls_hadoop_conf(storage_connector, path)
        elif storage_connector.type == StorageConnector.GCS:
            return self._setup_gcp_hadoop_conf(storage_connector, path)
        else:
            return path

    def _setup_s3_hadoop_conf(self, storage_connector, path):
        FS_S3_ENDPOINT = "fs.s3a.endpoint"
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
        if FS_S3_ENDPOINT in storage_connector.arguments:
            self._spark_context._jsc.hadoopConfiguration().set(
                FS_S3_ENDPOINT, storage_connector.spark_options().get(FS_S3_ENDPOINT)
            )

        return path.replace("s3", "s3a", 1) if path is not None else None

    def _setup_adls_hadoop_conf(self, storage_connector, path):
        for k, v in storage_connector.spark_options().items():
            self._spark_context._jsc.hadoopConfiguration().set(k, v)

        return path

    def is_spark_dataframe(self, dataframe):
        if isinstance(dataframe, DataFrame):
            return True
        return False

    def save_empty_dataframe(self, feature_group):
        fg_table_name = feature_group._get_table_name()
        dataframe = self._spark_session.table(fg_table_name).limit(0)

        self.save_dataframe(
            feature_group,
            dataframe,
            "upsert",
            feature_group.online_enabled,
            "offline",
            {},
            {},
        )

    def _apply_transformation_function(self, transformation_functions, dataset):
        # generate transformation function expressions
        transformed_feature_names = []
        transformation_fn_expressions = []
        for (
            feature_name,
            transformation_fn,
        ) in transformation_functions.items():
            fn_registration_name = (
                transformation_fn.name
                + "_"
                + str(transformation_fn.version)
                + "_"
                + feature_name
            )

            def timezone_decorator(func):
                if transformation_fn.output_type != "TIMESTAMP":
                    return func

                current_timezone = tzlocal.get_localzone()

                def decorated_func(x):
                    result = func(x)
                    if isinstance(result, datetime):
                        if result.tzinfo is None:
                            # if timestamp is timezone unaware, make sure it's localized to the system's timezone.
                            # otherwise, spark will implicitly convert it to the system's timezone.
                            return result.replace(tzinfo=current_timezone)
                        else:
                            # convert to utc, then localize to system's timezone
                            return result.astimezone(timezone.utc).replace(
                                tzinfo=current_timezone
                            )
                    return result

                return decorated_func

            self._spark_session.udf.register(
                fn_registration_name,
                timezone_decorator(transformation_fn.transformation_fn),
                transformation_fn.output_type,
            )
            transformation_fn_expressions.append(
                "{fn_name:}({name:}) AS {name:}".format(
                    fn_name=fn_registration_name, name=feature_name
                )
            )
            transformed_feature_names.append(feature_name)

        # generate non transformation expressions
        no_transformation_expr = [
            "{name:} AS {name:}".format(name=col_name)
            for col_name in dataset.columns
            if col_name not in transformed_feature_names
        ]

        # generate entire expression and execute it
        transformation_fn_expressions.extend(no_transformation_expr)
        transformed_dataset = dataset.selectExpr(*transformation_fn_expressions)
        return transformed_dataset.select(*dataset.columns)

    def _setup_gcp_hadoop_conf(self, storage_connector, path):
        PROPERTY_ENCRYPTION_KEY = "fs.gs.encryption.key"
        PROPERTY_ENCRYPTION_HASH = "fs.gs.encryption.key.hash"
        PROPERTY_ALGORITHM = "fs.gs.encryption.algorithm"
        PROPERTY_GCS_FS_KEY = "fs.AbstractFileSystem.gs.impl"
        PROPERTY_GCS_FS_VALUE = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        PROPERTY_GCS_ACCOUNT_ENABLE = "google.cloud.auth.service.account.enable"
        PROPERTY_ACCT_EMAIL = "fs.gs.auth.service.account.email"
        PROPERTY_ACCT_KEY_ID = "fs.gs.auth.service.account.private.key.id"
        PROPERTY_ACCT_KEY = "fs.gs.auth.service.account.private.key"
        # The AbstractFileSystem for 'gs:' URIs
        self._spark_context._jsc.hadoopConfiguration().setIfUnset(
            PROPERTY_GCS_FS_KEY, PROPERTY_GCS_FS_VALUE
        )
        # Whether to use a service account for GCS authorization. Setting this
        # property to `false` will disable use of service accounts for authentication.
        self._spark_context._jsc.hadoopConfiguration().setIfUnset(
            PROPERTY_GCS_ACCOUNT_ENABLE, "true"
        )

        # The JSON key file of the service account used for GCS
        # access when google.cloud.auth.service.account.enable is true.
        local_path = self.add_file(storage_connector.key_path)
        with open(local_path, "r") as f_in:
            jsondata = json.load(f_in)
        self._spark_context._jsc.hadoopConfiguration().set(
            PROPERTY_ACCT_EMAIL, jsondata["client_email"]
        )
        self._spark_context._jsc.hadoopConfiguration().set(
            PROPERTY_ACCT_KEY_ID, jsondata["private_key_id"]
        )
        self._spark_context._jsc.hadoopConfiguration().set(
            PROPERTY_ACCT_KEY, jsondata["private_key"]
        )

        if storage_connector.algorithm:
            # if encryption fields present
            self._spark_context._jsc.hadoopConfiguration().set(
                PROPERTY_ALGORITHM, storage_connector.algorithm
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                PROPERTY_ENCRYPTION_KEY, storage_connector.encryption_key
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                PROPERTY_ENCRYPTION_HASH, storage_connector.encryption_key_hash
            )
        else:
            # unset if already set
            self._spark_context._jsc.hadoopConfiguration().unset(PROPERTY_ALGORITHM)
            self._spark_context._jsc.hadoopConfiguration().unset(
                PROPERTY_ENCRYPTION_HASH
            )
            self._spark_context._jsc.hadoopConfiguration().unset(
                PROPERTY_ENCRYPTION_KEY
            )

        return path

    def create_empty_df(self, streaming_df):
        return SQLContext(self._spark_context).createDataFrame(
            self._spark_context.emptyRDD(), streaming_df.schema
        )

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name):
        unique_values = feature_dataframe.select(feature_name).distinct().collect()
        return [field[feature_name] for field in unique_values]

    @staticmethod
    def convert_spark_type_to_offline_type(spark_type, using_hudi):
        # The HiveSyncTool is strict and does not support schema evolution from tinyint/short to
        # int. Avro, on the other hand, does not support tinyint/short and delivers them as int
        # to Hive. Therefore, we need to force Hive to create int-typed columns in the first place.

        if not using_hudi:
            return spark_type.simpleString()
        elif type(spark_type) == ByteType:
            return "int"
        elif type(spark_type) == ShortType:
            return "int"
        elif type(spark_type) in [
            BooleanType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
            TimestampType,
            DateType,
            StringType,
            ArrayType,
            StructType,
            BinaryType,
        ]:
            return spark_type.simpleString()

        raise ValueError(f"spark type {str(type(spark_type))} not supported")

    @staticmethod
    def _convert_offline_type_to_spark_type(offline_type):
        if "array<" == offline_type[:6]:
            return ArrayType(
                Engine._convert_offline_type_to_spark_type(offline_type[6:-1])
            )
        elif "struct<label:string,index:int>" in offline_type:
            return StructType(
                [
                    StructField("label", StringType(), True),
                    StructField("index", IntegerType(), True),
                ]
            )
        elif offline_type.startswith("decimal"):
            return DecimalType()
        else:
            offline_type_spark_type_mapping = {
                "string": StringType(),
                "bigint": LongType(),
                "int": IntegerType(),
                "smallint": ShortType(),
                "tinyint": ByteType(),
                "float": FloatType(),
                "double": DoubleType(),
                "timestamp": TimestampType(),
                "boolean": BooleanType(),
                "date": DateType(),
                "binary": BinaryType(),
            }
            if offline_type in offline_type_spark_type_mapping:
                return offline_type_spark_type_mapping[offline_type]
            else:
                raise FeatureStoreException(
                    f"Offline type {offline_type} cannot be converted to a spark type."
                )

    @staticmethod
    def cast_columns(df, schema, online=False):
        pyspark_schema = dict(
            [
                (_feat.name, Engine._convert_offline_type_to_spark_type(_feat.type))
                for _feat in schema
            ]
        )
        for _feat in pyspark_schema:
            df = df.withColumn(_feat, col(_feat).cast(pyspark_schema[_feat]))
        return df

    def _get_kafka_config(
        self, feature_store_id: int, write_options: dict = {}
    ) -> dict:
        external = not (
            isinstance(client.get_instance(), hopsworks.Client)
            or write_options.get("internal_kafka", False)
        )

        storage_connector = self._storage_connector_api.get_kafka_connector(
            feature_store_id, external
        )

        config = storage_connector.spark_options()
        config.update(write_options)
        return config

    @staticmethod
    def is_connector_type_supported(type):
        return True


class SchemaError(Exception):
    """Thrown when schemas don't match"""
