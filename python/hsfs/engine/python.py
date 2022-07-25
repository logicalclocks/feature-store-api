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
import boto3
import time
import re
import warnings
import avro
import socket
import pyarrow as pa
import json
import random
import uuid

import great_expectations as ge

from io import BytesIO
from pyhive import hive
from urllib.parse import urlparse
from typing import TypeVar, Optional, Dict, Any
from confluent_kafka import Producer
from tqdm.auto import tqdm

from hsfs import client, feature, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import (
    feature_group_api,
    dataset_api,
    job_api,
    ingestion_job_conf,
    kafka_api,
    statistics_api,
    training_dataset_api,
    training_dataset_job_conf,
    feature_view_api,
    transformation_function_engine,
)
from hsfs.constructor import query
from hsfs.client import exceptions, external, hopsworks
from hsfs.feature_group import FeatureGroup
from thrift.transport.TTransport import TTransportException
from pyhive.exc import OperationalError

HAS_FAST = False
try:
    from fastavro import schemaless_writer
    from fastavro.schema import parse_schema

    HAS_FAST = True
except ImportError:
    pass


class Engine:
    APP_OP_INSERT_FG = "insert_fg"

    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._job_api = job_api.JobApi()
        self._kafka_api = kafka_api.KafkaApi()

        # cache the sql engine which contains the connection pool
        self._mysql_online_fs_engine = None

    def sql(self, sql_query, feature_store, online_conn, dataframe_type, read_options):
        if not online_conn:
            return self._sql_offline(sql_query, feature_store, dataframe_type)
        else:
            return self._jdbc(sql_query, online_conn, dataframe_type, read_options)

    def _sql_offline(self, sql_query, feature_store, dataframe_type):
        with self._create_hive_connection(feature_store) as hive_conn:
            result_df = pd.read_sql(sql_query, hive_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def _jdbc(self, sql_query, connector, dataframe_type, read_options):
        if self._mysql_online_fs_engine is None:
            self._mysql_online_fs_engine = util.create_mysql_engine(
                connector,
                (
                    isinstance(client.get_instance(), client.external.Client)
                    if "external" not in read_options
                    else read_options["external"]
                ),
            )
        with self._mysql_online_fs_engine.connect() as mysql_conn:
            result_df = pd.read_sql(sql_query, mysql_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def read(self, storage_connector, data_format, read_options, location):
        if storage_connector.type == storage_connector.HOPSFS:
            df_list = self._read_hopsfs(location, data_format)
        elif storage_connector.type == storage_connector.S3:
            df_list = self._read_s3(storage_connector, location, data_format)
        else:
            raise NotImplementedError(
                "{} Storage Connectors for training datasets are not supported yet for external environments.".format(
                    storage_connector.type
                )
            )
        return pd.concat(df_list, ignore_index=True)

    def _read_pandas(self, data_format, obj):
        if data_format.lower() == "csv":
            return pd.read_csv(obj)
        elif data_format.lower() == "tsv":
            return pd.read_csv(obj, sep="\t")
        elif data_format.lower() == "parquet":
            return pd.read_parquet(BytesIO(obj.read()))
        else:
            raise TypeError(
                "{} training dataset format is not supported to read as pandas dataframe.".format(
                    data_format
                )
            )

    def _read_hopsfs(self, location, data_format):
        # providing more informative error
        try:
            from pydoop import hdfs
        except ModuleNotFoundError:
            return self._read_hopsfs_rest(location, data_format)

        util.setup_pydoop()
        path_list = hdfs.ls(location, recursive=True)

        df_list = []
        for path in path_list:
            if (
                hdfs.path.isfile(path)
                and not path.endswith("_SUCCESS")
                and hdfs.path.getsize(path) > 0
            ):
                df_list.append(self._read_pandas(data_format, path))
        return df_list

    # This is a version of the read method that uses the Hopsworks REST APIs
    # To read the training dataset content, this to avoid the pydoop dependency
    # requirement and allow users to read Hopsworks training dataset from outside
    def _read_hopsfs_rest(self, location, data_format):
        total_count = 10000
        offset = 0
        df_list = []

        while offset < total_count:
            total_count, inode_list = self._dataset_api.list_files(
                location, offset, 100
            )

            for inode in inode_list:
                if not inode.path.endswith("_SUCCESS"):
                    content_stream = self._dataset_api.read_content(inode.path)
                    df_list.append(
                        self._read_pandas(data_format, BytesIO(content_stream.content))
                    )
                offset += 1

        return df_list

    def _read_s3(self, storage_connector, location, data_format):
        # get key prefix
        path_parts = location.replace("s3://", "").split("/")
        _ = path_parts.pop(0)  # pop first element -> bucket

        prefix = "/".join(path_parts)

        if storage_connector.session_token is not None:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=storage_connector.access_key,
                aws_secret_access_key=storage_connector.secret_key,
                aws_session_token=storage_connector.session_token,
            )
        else:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=storage_connector.access_key,
                aws_secret_access_key=storage_connector.secret_key,
            )

        df_list = []
        object_list = {"is_truncated": True}
        while object_list.get("is_truncated", False):
            if "NextContinuationToken" in object_list:
                object_list = s3.list_objects_v2(
                    Bucket=storage_connector.bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                    ContinuationToken=object_list["NextContinuationToken"],
                )
            else:
                object_list = s3.list_objects_v2(
                    Bucket=storage_connector.bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                )

            for obj in object_list["Contents"]:
                if not obj["Key"].endswith("_SUCCESS") and obj["Size"] > 0:
                    obj = s3.get_object(
                        Bucket=storage_connector.bucket,
                        Key=obj["Key"],
                    )
                    df_list.append(self._read_pandas(data_format, obj["Body"]))
        return df_list

    def read_options(self, data_format, provided_options):
        return {}

    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ):
        raise NotImplementedError(
            "Streaming Sources are not supported for pure Python Environments."
        )

    def show(self, sql_query, feature_store, n, online_conn):
        return self.sql(sql_query, feature_store, online_conn, "default", {}).head(n)

    def register_external_temporary_table(self, external_fg, alias):
        # No op to avoid query failure
        pass

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        # No op to avoid query failure
        pass

    def profile_by_spark(self, metadata_instance):
        stat_api = statistics_api.StatisticsApi(
            metadata_instance.feature_store_id, metadata_instance.ENTITY_TYPE
        )
        job = stat_api.compute(metadata_instance)
        print(
            "Statistics Job started successfully, you can follow the progress at \n{}".format(
                self._get_job_url(job.href)
            )
        )

        self._wait_for_job(job)

    def profile(
        self,
        df,
        relevant_columns,
        correlations,
        histograms,
        exact_uniqueness=True,
    ):
        # TODO: add statistics for correlations, histograms and exact_uniqueness
        if not relevant_columns:
            stats = df.describe()
        else:
            target_cols = [col for col in df.columns if col in relevant_columns]
            stats = df[target_cols].describe()
        final_stats = []
        for col in stats.columns:
            stat = self._convert_pandas_statistics(stats[col].to_dict())
            stat["dataType"] = (
                "Fractional"
                if isinstance(stats[col].dtype, type(np.dtype(np.float64)))
                else "Integral"
            )
            stat["isDataTypeInferred"] = "false"
            stat["column"] = col.split(".")[-1]
            stat["completeness"] = 1
            final_stats.append(stat)
        return json.dumps({"columns": final_stats})

    def _convert_pandas_statistics(self, stat):
        # For now transformation only need 25th, 50th, 75th percentiles
        # TODO: calculate properly all percentiles
        content_dict = {}
        percentiles = []
        if "25%" in stat:
            percentiles = [0] * 100
            percentiles[24] = stat["25%"]
            percentiles[49] = stat["50%"]
            percentiles[74] = stat["75%"]
        if "mean" in stat:
            content_dict["mean"] = stat["mean"]
        if "mean" in stat and "count" in stat:
            content_dict["sum"] = stat["mean"] * stat["count"]
        if "max" in stat:
            content_dict["maximum"] = stat["max"]
        if "std" in stat:
            content_dict["stdDev"] = stat["std"]
        if "min" in stat:
            content_dict["minimum"] = stat["min"]
        if percentiles:
            content_dict["approxPercentiles"] = percentiles
        return content_dict

    def validate(self, dataframe: pd.DataFrame, expectations, log_activity=True):
        raise NotImplementedError(
            "Deequ data validation is only available with Spark Engine. Use validate_with_great_expectations"
        )

    def validate_with_great_expectations(
        self,
        dataframe: pd.DataFrame,
        expectation_suite: TypeVar("ge.core.ExpectationSuite"),
        ge_validate_kwargs: Optional[Dict[Any, Any]] = {},
    ):
        report = ge.from_pandas(
            dataframe, expectation_suite=expectation_suite
        ).validate(**ge_validate_kwargs)
        return report

    def set_job_group(self, group_id, description):
        pass

    def convert_to_default_dataframe(self, dataframe):
        if isinstance(dataframe, pd.DataFrame):
            upper_case_features = [
                col for col in dataframe.columns if any(re.finditer("[A-Z]", col))
            ]
            if len(upper_case_features) > 0:
                warnings.warn(
                    "The ingested dataframe contains upper case letters in feature names: `{}`. "
                    "Feature names are sanitized to lower case in the feature store.".format(
                        upper_case_features
                    ),
                    util.FeatureGroupWarning,
                )

            # making a shallow copy of the dataframe so that column names are unchanged
            dataframe_copy = dataframe.copy(deep=False)
            dataframe_copy.columns = [x.lower() for x in dataframe_copy.columns]
            return dataframe_copy

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: pandas dataframe. "
            + "The provided dataframe has type: {}".format(type(dataframe))
        )

    def parse_schema_feature_group(self, dataframe, time_travel_format=None):
        arrow_schema = pa.Schema.from_pandas(dataframe)
        features = []
        for feat_name, feat_type in dataframe.dtypes.items():
            name = feat_name.lower()
            try:
                converted_type = self._convert_pandas_type(
                    feat_type, arrow_schema.field(feat_name).type
                )
            except ValueError as e:
                raise FeatureStoreException(f"Feature '{name}': {str(e)}")
            features.append(feature.Feature(name, converted_type))
        return features

    def parse_schema_training_dataset(self, dataframe):
        raise NotImplementedError(
            "Training dataset creation from Dataframes is not "
            + "supported in Python environment. Use HSFS Query object instead."
        )

    def _convert_pandas_type(self, dtype, arrow_type):
        # This is a simple type conversion between pandas dtypes and pyspark (hive) types,
        # using pyarrow types to convert "O (object)"-typed fields.
        # In the backend, the types specified here will also be used for mapping to Avro types.
        if dtype == np.dtype("O"):
            return self._infer_type_pyarrow(arrow_type)

        return self._convert_simple_pandas_type(dtype)

    def _convert_simple_pandas_type(self, dtype):
        if dtype == np.dtype("uint8"):
            return "int"
        elif dtype == np.dtype("uint16"):
            return "int"
        elif dtype == np.dtype("int8"):
            return "int"
        elif dtype == np.dtype("int16"):
            return "int"
        elif dtype == np.dtype("int32"):
            return "int"
        elif dtype == np.dtype("uint32"):
            return "bigint"
        elif dtype == np.dtype("int64"):
            return "bigint"
        elif dtype == np.dtype("float16"):
            return "float"
        elif dtype == np.dtype("float32"):
            return "float"
        elif dtype == np.dtype("float64"):
            return "double"
        elif dtype == np.dtype("datetime64[ns]"):
            return "timestamp"
        elif dtype == np.dtype("bool"):
            return "boolean"
        elif dtype == "category":
            return "string"

        raise ValueError(f"dtype '{dtype}' not supported")

    def _infer_type_pyarrow(self, arrow_type):
        if pa.types.is_list(arrow_type):
            # figure out sub type
            sub_arrow_type = arrow_type.value_type
            sub_dtype = np.dtype(sub_arrow_type.to_pandas_dtype())
            subtype = self._convert_pandas_type(sub_dtype, sub_arrow_type)
            return "array<{}>".format(subtype)
        if pa.types.is_struct(arrow_type):
            # best effort, based on pyarrow's string representation
            return str(arrow_type)
        # Currently not supported
        # elif pa.types.is_decimal(arrow_type):
        #    return str(arrow_type).replace("decimal128", "decimal")
        elif pa.types.is_date(arrow_type):
            return "date"
        elif pa.types.is_binary(arrow_type):
            return "binary"
        elif pa.types.is_string(arrow_type) or pa.types.is_unicode(arrow_type):
            return "string"

        raise ValueError(f"dtype 'O' (arrow_type '{str(arrow_type)}') not supported")

    def save_dataframe(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame,
        operation: str,
        online_enabled: bool,
        storage: bool,
        offline_write_options: dict,
        online_write_options: dict,
        validation_id: int = None,
    ):
        if feature_group.stream:
            return self._write_dataframe_kafka(
                feature_group, dataframe, offline_write_options
            )
        else:
            # for backwards compatibility
            return self.legacy_save_dataframe(
                feature_group,
                dataframe,
                operation,
                online_enabled,
                storage,
                offline_write_options,
                online_write_options,
                validation_id,
            )

    def legacy_save_dataframe(
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
        # App configuration
        app_options = self._get_app_options(offline_write_options)

        # Setup job for ingestion
        # Configure Hopsworks ingestion job
        print("Configuring ingestion job...")
        fg_api = feature_group_api.FeatureGroupApi(feature_group.feature_store_id)
        ingestion_job = fg_api.ingestion(feature_group, app_options)

        # Upload dataframe into Hopsworks
        print("Uploading Pandas dataframe...")
        self._dataset_api.upload(feature_group, ingestion_job.data_path, dataframe)

        # Launch job
        print("Launching ingestion job...")
        self._job_api.launch(ingestion_job.job.name)
        print(
            "Ingestion Job started successfully, you can follow the progress at \n{}".format(
                self._get_job_url(ingestion_job.job.href)
            )
        )

        self._wait_for_job(ingestion_job.job, offline_write_options)

        return ingestion_job.job

    def get_training_data(
        self, training_dataset_obj, feature_view_obj, query_obj, read_options
    ):
        df = query_obj.read(read_options=read_options)
        if training_dataset_obj.splits:
            split_df = self._prepare_transform_split_df(
                df, training_dataset_obj, feature_view_obj
            )
        else:
            split_df = df
            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset_obj, feature_view_obj, split_df
            )
            split_df = self._apply_transformation_function(
                training_dataset_obj, split_df
            )
        return split_df

    def split_labels(self, df, labels):
        if labels:
            labels_df = df[labels]
            df_new = df.drop(columns=labels)
            return df_new, labels_df
        else:
            return df, None

    def _prepare_transform_split_df(self, df, training_dataset_obj, feature_view_obj):
        """
        Split a df into slices defined by `splits`. `splits` is a `dict(str, int)` which keys are name of split
        and values are split ratios.
        """
        split_column = f"_SPLIT_INDEX_{uuid.uuid1()}"
        result_dfs = {}
        splits = training_dataset_obj.splits
        if (
            sum([split.percentage for split in splits]) != 1
            or sum([split.percentage > 1 or split.percentage < 0 for split in splits])
            > 1
        ):
            raise ValueError(
                "Sum of split ratios should be 1 and each values should be in range (0, 1)"
            )

        df_size = len(df)
        groups = []
        for i, split in enumerate(splits):
            groups += [i] * int(df_size * split.percentage)
        groups += [len(splits) - 1] * (df_size - len(groups))
        random.shuffle(groups)
        df[split_column] = groups
        for i, split in enumerate(splits):
            split_df = df[df[split_column] == i].drop(split_column, axis=1)
            result_dfs[split.name] = split_df

        # apply transformations
        # 1st parametrise transformation functions with dt split stats
        transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
            training_dataset_obj, feature_view_obj, result_dfs
        )
        # and the apply them
        for split_name in result_dfs:
            result_dfs[split_name] = self._apply_transformation_function(
                training_dataset_obj,
                result_dfs.get(split_name),
            )

        return result_dfs

    def write_training_dataset(
        self,
        training_dataset,
        dataset,
        user_write_options,
        save_mode,
        feature_view_obj=None,
        to_df=False,
    ):
        if not feature_view_obj and not isinstance(dataset, query.Query):
            raise Exception(
                "Currently only query based training datasets are supported by the Python engine"
            )

        # As for creating a feature group, users have the possibility of passing
        # a spark_job_configuration object as part of the user_write_options with the key "spark"
        spark_job_configuration = user_write_options.pop("spark", None)
        td_app_conf = training_dataset_job_conf.TrainingDatsetJobConf(
            query=dataset,
            overwrite=(save_mode == "overwrite"),
            write_options=user_write_options,
            spark_job_configuration=spark_job_configuration,
        )

        if feature_view_obj:
            fv_api = feature_view_api.FeatureViewApi(feature_view_obj.featurestore_id)
            td_job = fv_api.compute_training_dataset(
                feature_view_obj.name,
                feature_view_obj.version,
                training_dataset.version,
                td_app_conf,
            )
        else:
            td_api = training_dataset_api.TrainingDatasetApi(
                training_dataset.feature_store_id
            )
            td_job = td_api.compute(training_dataset, td_app_conf)
        print(
            "Training dataset job started successfully, you can follow the progress at \n{}".format(
                self._get_job_url(td_job.href)
            )
        )

        # If the user passed the wait_for_job option consider it,
        # otherwise use the default True
        self._wait_for_job(td_job, user_write_options)

        return td_job

    def _create_hive_connection(self, feature_store):
        try:
            return hive.Connection(
                host=client.get_instance()._host,
                port=9085,
                # database needs to be set every time, 'default' doesn't work in pyhive
                database=feature_store,
                auth="CERTIFICATES",
                truststore=client.get_instance()._get_jks_trust_store_path(),
                keystore=client.get_instance()._get_jks_key_store_path(),
                keystore_password=client.get_instance()._cert_key,
            )
        except (TTransportException, AttributeError):
            raise ValueError(
                f"Cannot connect to hive server. Please check the host name '{client.get_instance()._host}' "
                "is correct and make sure port '9085' is open on host server."
            )
        except OperationalError as e:
            if e.args[0].status.statusCode == 3:
                raise RuntimeError(
                    f"Cannot access feature store '{feature_store}'. Please check if your project has the access right."
                    f" It is possible to request access from data owners of '{feature_store}'."
                )

    def _return_dataframe_type(self, dataframe, dataframe_type):
        if dataframe_type.lower() in ["default", "pandas"]:
            return dataframe
        if dataframe_type.lower() == "numpy":
            return dataframe.values
        if dataframe_type == "python":
            return dataframe.values.tolist()

        raise TypeError(
            "Dataframe type `{}` not supported on this platform.".format(dataframe_type)
        )

    def is_spark_dataframe(self, dataframe):
        return False

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
        raise NotImplementedError(
            "Stream ingestion is not available on Python environments, because it requires Spark as engine."
        )

    def get_empty_appended_dataframe(self, dataframe, new_features):
        """No-op in python engine, user has to write to feature group manually for schema
        change to take effect."""
        return None

    def save_empty_dataframe(self, feature_group, dataframe):
        """Wrapper around save_dataframe in order to provide no-op."""
        pass

    def _get_job_url(self, href: str):
        """Use the endpoint returned by the API to construct the UI url for jobs

        Args:
            href (str): the endpoint returned by the API
        """
        url = urlparse(href)
        url_splits = url.path.split("/")
        project_id = url_splits[4]
        job_name = url_splits[6]
        ui_url = url._replace(
            path="p/{}/jobs/named/{}/executions".format(project_id, job_name)
        )
        ui_url = client.get_instance().replace_public_host(ui_url)
        return ui_url.geturl()

    def _get_app_options(self, user_write_options={}):
        """
        Generate the options that should be passed to the application doing the ingestion.
        Options should be data format, data options to read the input dataframe and
        insert options to be passed to the insert method

        Users can pass Spark configurations to the save/insert method
        Property name should match the value in the JobConfiguration.__init__
        """
        spark_job_configuration = user_write_options.pop("spark", None)

        return ingestion_job_conf.IngestionJobConf(
            data_format="PARQUET",
            data_options=[],
            write_options=user_write_options,
            spark_job_configuration=spark_job_configuration,
        )

    def _wait_for_job(self, job, user_write_options=None):
        # If the user passed the wait_for_job option consider it,
        # otherwise use the default True
        while user_write_options is None or user_write_options.get(
            "wait_for_job", True
        ):
            executions = self._job_api.last_execution(job)
            if len(executions) > 0:
                execution = executions[0]
            else:
                return

            if execution.final_status.lower() == "succeeded":
                return
            elif execution.final_status.lower() == "failed":
                raise exceptions.FeatureStoreException(
                    "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
                )
            elif execution.final_status.lower() == "killed":
                raise exceptions.FeatureStoreException("The Hopsworks Job was stopped")

            time.sleep(3)

    def add_file(self, file):
        # if streaming connectors are implemented in the future, this method
        # can be used to materialize certificates locally
        return file

    @staticmethod
    def _apply_transformation_function(training_dataset_instance, dataset):
        for (
            feature_name,
            transformation_fn,
        ) in training_dataset_instance.transformation_functions.items():
            dataset[feature_name] = dataset[feature_name].map(
                transformation_fn.transformation_fn
            )

        return dataset

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name):
        return feature_dataframe[feature_name].unique()

    def _write_dataframe_kafka(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame,
        offline_write_options: dict,
    ):
        # setup kafka producer
        producer = Producer(self._get_kafka_config(offline_write_options))

        # setup complex feature writers
        feature_writers = {
            feature: self._get_encoder_func(
                feature_group._get_feature_avro_schema(feature)
            )
            for feature in feature_group.get_complex_features()
        }

        # setup row writer function
        writer = self._get_encoder_func(feature_group._get_encoded_avro_schema())

        def acked(err, msg):
            if err is not None and offline_write_options.get("debug_kafka", False):
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                # update progress bar for each msg
                progress_bar.update()

        # initialize progress bar
        progress_bar = tqdm(
            total=dataframe.shape[0],
            bar_format="{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt} | "
            "Elapsed Time: {elapsed} | Remaining Time: {remaining}",
            desc="Uploading Dataframe",
            mininterval=1,
        )
        # loop over rows
        for r in dataframe.itertuples(index=False):
            # itertuples returns Python NamedTyple, to be able to serialize it using
            # avro, create copy of row only by converting to dict, which preserves datatypes
            row = r._asdict()

            # transform special data types
            # here we might need to handle also timestamps and other complex types
            # possible optimizaiton: make it based on type so we don't need to loop over
            # all keys in the row
            for k in row.keys():
                # for avro to be able to serialize them, they need to be python data types
                if isinstance(row[k], np.ndarray):
                    row[k] = row[k].tolist()
                if isinstance(row[k], pd.Timestamp):
                    row[k] = row[k].to_pydatetime()

            # encode complex features
            row = self._encode_complex_features(feature_writers, row)

            # encode feature row
            with BytesIO() as outf:
                writer(row, outf)
                encoded_row = outf.getvalue()

            # assemble key
            key = "".join([str(row[pk]) for pk in sorted(feature_group.primary_key)])

            while True:
                # if BufferError is thrown, we can be sure, message hasn't been send so we retry
                try:
                    # produce
                    producer.produce(
                        topic=feature_group._online_topic_name,
                        key=key,
                        value=encoded_row,
                        callback=acked,
                    )

                    # Trigger internal callbacks to empty op queue
                    producer.poll(0)
                    break
                except BufferError as e:
                    if offline_write_options.get("debug_kafka", False):
                        print("Caught: {}".format(e))
                    # backoff for 1 second
                    producer.poll(1)

        # make sure producer blocks and everything is delivered
        producer.flush()
        progress_bar.close()

        # start backfilling job
        job_name = "{fg_name}_{version}_offline_fg_backfill".format(
            fg_name=feature_group.name, version=feature_group.version
        )
        job = self._job_api.get(job_name)

        if offline_write_options is not None and offline_write_options.get(
            "start_offline_backfill", True
        ):
            print("Launching offline feature group backfill job...")
            self._job_api.launch(job_name)
            print(
                "Backfill Job started successfully, you can follow the progress at \n{}".format(
                    self._get_job_url(job.href)
                )
            )
            self._wait_for_job(job, offline_write_options)

        return job

    def _encode_complex_features(
        self, feature_writers: Dict[str, callable], row: dict
    ) -> dict:
        for feature_name, writer in feature_writers.items():
            with BytesIO() as outf:
                writer(row[feature_name], outf)
                row[feature_name] = outf.getvalue()
        return row

    def _get_encoder_func(self, writer_schema: str) -> callable:
        if HAS_FAST:
            schema = json.loads(writer_schema)
            parsed_schema = parse_schema(schema)
            return lambda record, outf: schemaless_writer(outf, parsed_schema, record)

        parsed_schema = avro.schema.parse(writer_schema)
        writer = avro.io.DatumWriter(parsed_schema)
        return lambda record, outf: writer.write(record, avro.io.BinaryEncoder(outf))

    def _get_kafka_config(self, write_options: dict = {}) -> dict:
        # producer configuration properties
        # https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
        config = {
            "security.protocol": "SSL",
            "ssl.ca.location": client.get_instance()._get_ca_chain_path(),
            "ssl.certificate.location": client.get_instance()._get_client_cert_path(),
            "ssl.key.location": client.get_instance()._get_client_key_path(),
            "client.id": socket.gethostname(),
            **write_options.get("kafka_producer_config", {}),
        }

        if isinstance(client.get_instance(), hopsworks.Client) or write_options.get(
            "internal_kafka", False
        ):
            config["bootstrap.servers"] = ",".join(
                [
                    endpoint.replace("INTERNAL://", "")
                    for endpoint in self._kafka_api.get_broker_endpoints(
                        externalListeners=False
                    )
                ]
            )
        elif isinstance(client.get_instance(), external.Client):
            config["bootstrap.servers"] = ",".join(
                [
                    endpoint.replace("EXTERNAL://", "")
                    for endpoint in self._kafka_api.get_broker_endpoints(
                        externalListeners=True
                    )
                ]
            )
        return config
