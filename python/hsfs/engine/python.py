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
import ast
import warnings
import logging
import avro
import pyarrow as pa
import json
import random
import uuid
import decimal
import numbers
import math
import os
from datetime import datetime, timezone

import great_expectations as ge

from io import BytesIO
from pyhive import hive
from urllib.parse import urlparse
from typing import TypeVar, Optional, Dict, Any
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError
from tqdm.auto import tqdm
from botocore.response import StreamingBody
from sqlalchemy import sql

from hsfs import client, feature, util
from hsfs.feature_group import ExternalFeatureGroup
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import (
    feature_group_api,
    dataset_api,
    job_api,
    ingestion_job_conf,
    statistics_api,
    training_dataset_api,
    training_dataset_job_conf,
    feature_view_api,
    transformation_function_engine,
    arrow_flight_client,
    storage_connector_api,
    variable_api,
)
from hsfs.constructor import query
from hsfs.training_dataset_split import TrainingDatasetSplit
from hsfs.client import exceptions, hopsworks
from hsfs.feature_group import FeatureGroup
from thrift.transport.TTransport import TTransportException
from pyhive.exc import OperationalError

from hsfs.storage_connector import StorageConnector

# Disable pyhive INFO logging
logging.getLogger("pyhive").setLevel(logging.WARNING)

HAS_FAST = False
try:
    from fastavro import schemaless_writer
    from fastavro.schema import parse_schema

    HAS_FAST = True
except ImportError:
    pass


class Engine:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._job_api = job_api.JobApi()
        self._feature_group_api = feature_group_api.FeatureGroupApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()

        # cache the sql engine which contains the connection pool
        self._mysql_online_fs_engine = None

    def sql(
        self,
        sql_query,
        feature_store,
        online_conn,
        dataframe_type,
        read_options,
        schema=None,
    ):
        if not online_conn:
            return self._sql_offline(
                sql_query,
                feature_store,
                dataframe_type,
                schema,
                hive_config=read_options.get("hive_config") if read_options else None,
            )
        else:
            return self._jdbc(
                sql_query, online_conn, dataframe_type, read_options, schema
            )

    def is_flyingduck_query_supported(self, query, read_options={}):
        return arrow_flight_client.get_instance().is_query_supported(
            query, read_options
        )

    def _sql_offline(
        self,
        sql_query,
        feature_store,
        dataframe_type,
        schema=None,
        hive_config=None,
    ):
        if arrow_flight_client.get_instance().is_flyingduck_query_object(sql_query):
            result_df = util.run_with_loading_animation(
                "Reading data from Hopsworks, using ArrowFlight",
                arrow_flight_client.get_instance().read_query,
                sql_query,
            )
        else:
            with self._create_hive_connection(
                feature_store, hive_config=hive_config
            ) as hive_conn:
                # Suppress SQLAlchemy pandas warning
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    result_df = util.run_with_loading_animation(
                        "Reading data from Hopsworks, using Hive",
                        pd.read_sql,
                        sql_query,
                        hive_conn,
                    )

        if schema:
            result_df = Engine.cast_columns(result_df, schema)
        return self._return_dataframe_type(result_df, dataframe_type)

    def _jdbc(self, sql_query, connector, dataframe_type, read_options, schema=None):
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
            if "sqlalchemy" in str(type(mysql_conn)):
                sql_query = sql.text(sql_query)
            result_df = pd.read_sql(sql_query, mysql_conn)
            if schema:
                result_df = Engine.cast_columns(result_df, schema, online=True)
        return self._return_dataframe_type(result_df, dataframe_type)

    def read(self, storage_connector, data_format, read_options, location):
        if not data_format:
            raise FeatureStoreException("data_format is not specified")

        if storage_connector.type == storage_connector.HOPSFS:
            df_list = self._read_hopsfs(location, data_format, read_options)
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
        elif data_format.lower() == "parquet" and isinstance(obj, StreamingBody):
            return pd.read_parquet(BytesIO(obj.read()))
        elif data_format.lower() == "parquet":
            return pd.read_parquet(obj)
        else:
            raise TypeError(
                "{} training dataset format is not supported to read as pandas dataframe.".format(
                    data_format
                )
            )

    def _read_hopsfs(self, location, data_format, read_options={}):
        # providing more informative error
        try:
            from pydoop import hdfs
        except ModuleNotFoundError:
            return self._read_hopsfs_remote(location, data_format, read_options)
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

    # This is a version of the read method that uses the Hopsworks REST APIs or Flyginduck Server
    # To read the training dataset content, this to avoid the pydoop dependency
    # requirement and allow users to read Hopsworks training dataset from outside
    def _read_hopsfs_remote(self, location, data_format, read_options={}):
        total_count = 10000
        offset = 0
        df_list = []

        while offset < total_count:
            total_count, inode_list = self._dataset_api.list_files(
                location, offset, 100
            )

            for inode in inode_list:
                if not inode.path.endswith("_SUCCESS"):
                    if arrow_flight_client.get_instance().is_data_format_supported(
                        data_format, read_options
                    ):
                        df = arrow_flight_client.get_instance().read_path(inode.path)
                    else:
                        content_stream = self._dataset_api.read_content(inode.path)
                        df = self._read_pandas(
                            data_format, BytesIO(content_stream.content)
                        )

                    df_list.append(df)
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
        return provided_options or {}

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

    def show(self, sql_query, feature_store, n, online_conn, read_options={}):
        return self.sql(
            sql_query, feature_store, online_conn, "default", read_options
        ).head(n)

    def register_external_temporary_table(self, external_fg, alias):
        # No op to avoid query failure
        pass

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        if hudi_fg_alias and (
            hudi_fg_alias.left_feature_group_end_timestamp is not None
            or hudi_fg_alias.left_feature_group_start_timestamp is not None
        ):
            raise FeatureStoreException(
                "Hive engine on Python environments does not support incremental queries. "
                + "Read feature group without timestamp to retrieve latest snapshot or switch to "
                + "environment with Spark Engine."
            )

    def profile_by_spark(self, metadata_instance):
        stat_api = statistics_api.StatisticsApi(
            metadata_instance.feature_store_id, metadata_instance.ENTITY_TYPE
        )
        job = stat_api.compute(metadata_instance)
        print(
            "Statistics Job started successfully, you can follow the progress at \n{}".format(
                self.get_job_url(job.href)
            )
        )

        self.wait_for_job(job)

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
            stats_dict = json.loads(stats[col].to_json())
            stat = self._convert_pandas_statistics(stats_dict)
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
            if isinstance(stat["mean"], numbers.Number):
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

            # make shallow copy so the original df does not get changed
            # this is always needed to keep the user df unchanged
            dataframe_copy = dataframe.copy(deep=False)

            # making a shallow copy of the dataframe so that column names are unchanged
            if len(upper_case_features) > 0:
                warnings.warn(
                    "The ingested dataframe contains upper case letters in feature names: `{}`. "
                    "Feature names are sanitized to lower case in the feature store.".format(
                        upper_case_features
                    ),
                    util.FeatureGroupWarning,
                )
                dataframe_copy.columns = [x.lower() for x in dataframe_copy.columns]

            # convert timestamps with timezone to UTC
            for col in dataframe_copy.columns:
                if isinstance(
                    dataframe_copy[col].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype
                ):
                    dataframe_copy[col] = dataframe_copy[col].dt.tz_convert(None)
            return dataframe_copy
        elif dataframe == "spine":
            return None

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
                converted_type = self._convert_pandas_dtype_to_offline_type(
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

    def save_dataframe(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame,
        operation: str,
        online_enabled: bool,
        storage: str,
        offline_write_options: dict,
        online_write_options: dict,
        validation_id: int = None,
    ):
        if (
            isinstance(feature_group, ExternalFeatureGroup)
            and feature_group.online_enabled
        ) or feature_group.stream:
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
        ingestion_job = self._feature_group_api.ingestion(feature_group, app_options)

        # Upload dataframe into Hopsworks
        print("Uploading Pandas dataframe...")
        self._dataset_api.upload(feature_group, ingestion_job.data_path, dataframe)

        # run job
        ingestion_job.job.run(
            await_termination=offline_write_options is None
            or offline_write_options.get("wait_for_job", True)
        )

        return ingestion_job.job

    def get_training_data(
        self, training_dataset_obj, feature_view_obj, query_obj, read_options
    ):
        if training_dataset_obj.splits:
            return self._prepare_transform_split_df(
                query_obj, training_dataset_obj, feature_view_obj, read_options
            )
        else:
            df = query_obj.read(read_options=read_options)
            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset_obj, feature_view_obj, df
            )
            return self._apply_transformation_function(
                training_dataset_obj.transformation_functions, df
            )

    def split_labels(self, df, labels):
        if labels:
            labels_df = df[labels]
            df_new = df.drop(columns=labels)
            return df_new, labels_df
        else:
            return df, None

    def _prepare_transform_split_df(
        self, query_obj, training_dataset_obj, feature_view_obj, read_option
    ):
        """
        Split a df into slices defined by `splits`. `splits` is a `dict(str, int)` which keys are name of split
        and values are split ratios.
        """
        if (
            training_dataset_obj.splits[0].split_type
            == TrainingDatasetSplit.TIME_SERIES_SPLIT
        ):
            event_time = query_obj._left_feature_group.event_time
            if event_time not in [_feature.name for _feature in query_obj.features]:
                query_obj.append_feature(
                    query_obj._left_feature_group.__getattr__(event_time)
                )
                result_dfs = self._time_series_split(
                    query_obj.read(read_options=read_option),
                    training_dataset_obj,
                    event_time,
                    drop_event_time=True,
                )
            else:
                result_dfs = self._time_series_split(
                    query_obj.read(read_options=read_option),
                    training_dataset_obj,
                    event_time,
                )
        else:
            result_dfs = self._random_split(
                query_obj.read(read_options=read_option), training_dataset_obj
            )

        # apply transformations
        # 1st parametrise transformation functions with dt split stats
        transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
            training_dataset_obj, feature_view_obj, result_dfs
        )
        # and the apply them
        for split_name in result_dfs:
            result_dfs[split_name] = self._apply_transformation_function(
                training_dataset_obj.transformation_functions,
                result_dfs.get(split_name),
            )

        return result_dfs

    def _random_split(self, df, training_dataset_obj):
        split_column = f"_SPLIT_INDEX_{uuid.uuid1()}"
        result_dfs = {}
        splits = training_dataset_obj.splits
        if (
            not math.isclose(
                sum([split.percentage for split in splits]), 1
            )  # relative tolerance = 1e-09
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
        return result_dfs

    def _time_series_split(
        self, df, training_dataset_obj, event_time, drop_event_time=False
    ):
        result_dfs = {}
        for split in training_dataset_obj.splits:
            if len(df[event_time]) > 0:
                result_df = df[
                    [
                        split.start_time
                        <= util.convert_event_time_to_timestamp(t)
                        < split.end_time
                        for t in df[event_time]
                    ]
                ]
            else:
                # if df[event_time] is empty, it returns an empty dataframe
                result_df = df
            if drop_event_time:
                result_df = result_df.drop([event_time], axis=1)
            result_dfs[split.name] = result_df
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

        if (
            arrow_flight_client.get_instance().is_query_supported(
                dataset, user_write_options
            )
            and len(training_dataset.splits) == 0
            and len(training_dataset.transformation_functions) == 0
            and training_dataset.data_format == "parquet"
        ):
            query_obj, _ = dataset._prep_read(False, user_write_options)
            response = util.run_with_loading_animation(
                "Materializing data to Hopsworks, using ArrowFlight",
                arrow_flight_client.get_instance().create_training_dataset,
                feature_view_obj,
                training_dataset,
                query_obj,
            )

            return response

        # As for creating a feature group, users have the possibility of passing
        # a spark_job_configuration object as part of the user_write_options with the key "spark"
        spark_job_configuration = user_write_options.pop("spark", None)
        td_app_conf = training_dataset_job_conf.TrainingDatasetJobConf(
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
                self.get_job_url(td_job.href)
            )
        )

        self.wait_for_job(
            td_job,
            await_termination=user_write_options.get("wait_for_job", True),
        )

        return td_job

    def _create_hive_connection(self, feature_store, hive_config=None):
        host = variable_api.VariableApi().get_loadbalancer_external_domain()
        if host == "":
            # If the load balancer is not configured, then fall back to use
            # the hive server on the head node
            host = client.get_instance().host

        try:
            return hive.Connection(
                host=host,
                port=9085,
                # database needs to be set every time, 'default' doesn't work in pyhive
                database=feature_store,
                configuration=hive_config,
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
        if dataframe_type.lower() == "python":
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

    def save_empty_dataframe(self, feature_group):
        """Wrapper around save_dataframe in order to provide no-op."""
        pass

    def get_job_url(self, href: str):
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

    def wait_for_job(self, job, await_termination=True):
        # If the user passed the wait_for_job option consider it,
        # otherwise use the default True
        while await_termination:
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
        if not file:
            return file

        # This is used for unit testing
        if not file.startswith("file://"):
            file = "hdfs://" + file

        local_file = os.path.join("/tmp", os.path.basename(file))
        if not os.path.exists(local_file):
            content_stream = self._dataset_api.read_content(file, "HIVEDB")
            bytesio_object = BytesIO(content_stream.content)
            # Write the stuff
            with open(local_file, "wb") as f:
                f.write(bytesio_object.getbuffer())
        return local_file

    def _apply_transformation_function(self, transformation_functions, dataset):
        for (
            feature_name,
            transformation_fn,
        ) in transformation_functions.items():
            dataset[feature_name] = dataset[feature_name].map(
                transformation_fn.transformation_fn
            )
            offline_type = Engine.convert_spark_type_to_offline_type(
                transformation_fn.output_type
            )
            dataset[feature_name] = Engine._cast_column_to_offline_type(
                dataset[feature_name], offline_type
            )

        return dataset

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name):
        return feature_dataframe[feature_name].unique()

    def _init_kafka_producer(self, feature_group, offline_write_options):
        # setup kafka producer
        return Producer(
            self._get_kafka_config(
                feature_group.feature_store_id, offline_write_options
            )
        )

    def _init_kafka_consumer(self, feature_group, offline_write_options):
        # setup kafka consumer
        consumer_config = self._get_kafka_config(
            feature_group.feature_store_id, offline_write_options
        )
        if "group.id" not in consumer_config:
            consumer_config["group.id"] = "hsfs_consumer_group"

        return Consumer(consumer_config)

    def _init_kafka_resources(self, feature_group, offline_write_options):
        # setup kafka producer
        producer = self._init_kafka_producer(feature_group, offline_write_options)

        # setup complex feature writers
        feature_writers = {
            feature: self._get_encoder_func(
                feature_group._get_feature_avro_schema(feature)
            )
            for feature in feature_group.get_complex_features()
        }

        # setup row writer function
        writer = self._get_encoder_func(feature_group._get_encoded_avro_schema())
        return producer, feature_writers, writer

    def _write_dataframe_kafka(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame,
        offline_write_options: dict,
    ):
        initial_check_point = ""
        if feature_group._multi_part_insert:
            if feature_group._kafka_producer is None:
                producer, feature_writers, writer = self._init_kafka_resources(
                    feature_group, offline_write_options
                )
                feature_group._kafka_producer = producer
                feature_group._feature_writers = feature_writers
                feature_group._writer = writer
            else:
                producer = feature_group._kafka_producer
                feature_writers = feature_group._feature_writers
                writer = feature_group._writer
        else:
            producer, feature_writers, writer = self._init_kafka_resources(
                feature_group, offline_write_options
            )

            # initialize progress bar
            progress_bar = tqdm(
                total=dataframe.shape[0],
                bar_format="{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt} | "
                "Elapsed Time: {elapsed} | Remaining Time: {remaining}",
                desc="Uploading Dataframe",
                mininterval=1,
            )

            # set initial_check_point to the current offset
            initial_check_point = self._kafka_get_offsets(
                feature_group, offline_write_options, True
            )

        def acked(err, msg):
            if err is not None:
                if offline_write_options.get("debug_kafka", False):
                    print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
                if err.code() in [
                    KafkaError.TOPIC_AUTHORIZATION_FAILED,
                    KafkaError._MSG_TIMED_OUT,
                ]:
                    progress_bar.colour = "RED"
                    raise err  # Stop producing and show error
            # update progress bar for each msg
            if not feature_group._multi_part_insert:
                progress_bar.update()

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
                if isinstance(row[k], datetime) and row[k].tzinfo is None:
                    row[k] = row[k].replace(tzinfo=timezone.utc)
                if isinstance(row[k], pd._libs.missing.NAType):
                    row[k] = None

            # encode complex features
            row = self._encode_complex_features(feature_writers, row)

            # encode feature row
            with BytesIO() as outf:
                writer(row, outf)
                encoded_row = outf.getvalue()

            # assemble key
            key = "".join([str(row[pk]) for pk in sorted(feature_group.primary_key)])

            self._kafka_produce(
                producer, feature_group, key, encoded_row, acked, offline_write_options
            )

        # make sure producer blocks and everything is delivered
        if not feature_group._multi_part_insert:
            producer.flush()
            progress_bar.close()

        # start materialization job
        # if topic didn't exist, always run the materialization job to reset the offsets except if it's a multi insert
        if (
            not isinstance(feature_group, ExternalFeatureGroup)
            and not initial_check_point
            and not feature_group._multi_part_insert
        ):
            if self._start_offline_materialization(offline_write_options):
                warnings.warn(
                    "This is the first ingestion after an upgrade or backup/restore, running materialization job even though `start_offline_materialization` was set to `False`.",
                    util.FeatureGroupWarning,
                )
            # set the initial_check_point to the lowest offset (it was not set previously due to topic not existing)
            initial_check_point = self._kafka_get_offsets(
                feature_group, offline_write_options, False
            )
            feature_group.materialization_job.run(
                args=feature_group.materialization_job.config.get("defaultArgs", "")
                + initial_check_point,
                await_termination=offline_write_options.get("wait_for_job", False),
            )
        elif not isinstance(
            feature_group, ExternalFeatureGroup
        ) and self._start_offline_materialization(offline_write_options):
            if offline_write_options.get("skip_offsets", False):
                # don't provide the current offsets (read from where the job last left off)
                initial_check_point = ""
            # provide the initial_check_point as it will reduce the read amplification of materialization job
            feature_group.materialization_job.run(
                args=feature_group.materialization_job.config.get("defaultArgs", "")
                + initial_check_point,
                await_termination=offline_write_options.get("wait_for_job", False),
            )
        if isinstance(feature_group, ExternalFeatureGroup):
            return None
        return feature_group.materialization_job

    def _kafka_get_offsets(
        self,
        feature_group: FeatureGroup,
        offline_write_options: dict,
        high: bool,
    ):
        topic_name = feature_group._online_topic_name
        consumer = self._init_kafka_consumer(feature_group, offline_write_options)
        topics = consumer.list_topics(
            timeout=offline_write_options.get("kafka_timeout", 6)
        ).topics
        if topic_name in topics.keys():
            # topic exists
            offsets = ""
            tuple_value = int(high)
            for partition_metadata in topics.get(topic_name).partitions.values():
                partition = TopicPartition(
                    topic=topic_name, partition=partition_metadata.id
                )
                offsets += f",{partition_metadata.id}:{consumer.get_watermark_offsets(partition)[tuple_value]}"
            consumer.close()

            return f" -initialCheckPointString {topic_name + offsets}"
        return ""

    def _kafka_produce(
        self, producer, feature_group, key, encoded_row, acked, offline_write_options
    ):
        while True:
            # if BufferError is thrown, we can be sure, message hasn't been send so we retry
            try:
                # produce
                producer.produce(
                    topic=feature_group._online_topic_name,
                    key=key,
                    value=encoded_row,
                    callback=acked,
                    headers={
                        "projectId": str(feature_group.feature_store.project_id).encode(
                            "utf8"
                        ),
                        "featureGroupId": str(feature_group._id).encode("utf8"),
                        "subjectId": str(feature_group.subject["id"]).encode("utf8"),
                    },
                )

                # Trigger internal callbacks to empty op queue
                producer.poll(0)
                break
            except BufferError as e:
                if offline_write_options.get("debug_kafka", False):
                    print("Caught: {}".format(e))
                # backoff for 1 second
                producer.poll(1)

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

        config = storage_connector.confluent_options()
        config.update(write_options.get("kafka_producer_config", {}))
        return config

    @staticmethod
    def _convert_pandas_dtype_to_offline_type(dtype, arrow_type):
        # This is a simple type conversion between pandas dtypes and pyspark (hive) types,
        # using pyarrow types to convert "O (object)"-typed fields.
        # In the backend, the types specified here will also be used for mapping to Avro types.
        if dtype == np.dtype("O"):
            return Engine._convert_pandas_object_type_to_offline_type(arrow_type)

        return Engine._convert_simple_pandas_dtype_to_offline_type(dtype)

    @staticmethod
    def convert_spark_type_to_offline_type(spark_type_string):
        if spark_type_string.endswith("Type()"):
            spark_type_string = util.translate_legacy_spark_type(spark_type_string)
        if spark_type_string == "STRING":
            return "STRING"
        elif spark_type_string == "BINARY":
            return "BINARY"
        elif spark_type_string == "BYTE":
            return "INT"
        elif spark_type_string == "SHORT":
            return "INT"
        elif spark_type_string == "INT":
            return "INT"
        elif spark_type_string == "LONG":
            return "BIGINT"
        elif spark_type_string == "FLOAT":
            return "FLOAT"
        elif spark_type_string == "DOUBLE":
            return "DOUBLE"
        elif spark_type_string == "TIMESTAMP":
            return "TIMESTAMP"
        elif spark_type_string == "DATE":
            return "DATE"
        elif spark_type_string == "BOOLEAN":
            return "BOOLEAN"
        else:
            raise ValueError(
                f"Return type {spark_type_string} not supported for transformation functions."
            )

    @staticmethod
    def _convert_simple_pandas_dtype_to_offline_type(dtype):
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
        elif str(dtype) == "string":
            return "string"
        elif not isinstance(dtype, np.dtype):
            if dtype == pd.Int8Dtype():
                return "int"
            elif dtype == pd.Int16Dtype():
                return "int"
            elif dtype == pd.Int32Dtype():
                return "int"
            elif dtype == pd.Int64Dtype():
                return "bigint"

        raise ValueError(f"dtype '{dtype}' not supported")

    @staticmethod
    def _convert_pandas_object_type_to_offline_type(arrow_type):
        if pa.types.is_list(arrow_type):
            # figure out sub type
            sub_arrow_type = arrow_type.value_type
            try:
                sub_dtype = np.dtype(sub_arrow_type.to_pandas_dtype())
            except NotImplementedError:
                sub_dtype = np.dtype("object")
            subtype = Engine._convert_pandas_dtype_to_offline_type(
                sub_dtype, sub_arrow_type
            )
            return "array<{}>".format(subtype)
        if pa.types.is_struct(arrow_type):
            struct_schema = {}
            for index in range(arrow_type.num_fields):
                sub_arrow_type = arrow_type.field(index).type
                try:
                    sub_dtype = np.dtype(sub_arrow_type.to_pandas_dtype())
                except NotImplementedError:
                    sub_dtype = np.dtype("object")
                struct_schema[
                    arrow_type.field(index).name
                ] = Engine._convert_pandas_dtype_to_offline_type(
                    sub_dtype, arrow_type.field(index).type
                )
            return (
                "struct<"
                + ",".join([f"{key}:{value}" for key, value in struct_schema.items()])
                + ">"
            )
        # Currently not supported
        # elif pa.types.is_decimal(arrow_type):
        #    return str(arrow_type).replace("decimal128", "decimal")
        elif pa.types.is_date(arrow_type):
            return "date"
        elif pa.types.is_binary(arrow_type):
            return "binary"
        elif pa.types.is_string(arrow_type) or pa.types.is_unicode(arrow_type):
            return "string"
        elif pa.types.is_boolean(arrow_type):
            return "boolean"

        raise ValueError(f"dtype 'O' (arrow_type '{str(arrow_type)}') not supported")

    @staticmethod
    def _cast_column_to_offline_type(feature_column, offline_type):
        offline_type = offline_type.lower()
        if offline_type == "timestamp":
            # convert (if tz!=UTC) to utc, then make timezone unaware
            return pd.to_datetime(feature_column, utc=True).dt.tz_localize(None)
        elif offline_type == "date":
            return pd.to_datetime(feature_column, utc=True).dt.date
        elif offline_type.startswith("array<") or offline_type.startswith("struct<"):
            return feature_column.apply(
                lambda x: (ast.literal_eval(x) if type(x) is str else x)
                if (x is not None and x != "")
                else None
            )
        elif offline_type == "boolean":
            return feature_column.apply(
                lambda x: (ast.literal_eval(x) if type(x) is str else x)
                if (x is not None and x != "")
                else None
            )
        elif offline_type == "string":
            return feature_column.apply(lambda x: str(x) if x is not None else None)
        elif offline_type.startswith("decimal"):
            return feature_column.apply(
                lambda x: decimal.Decimal(x) if (x is not None) else None
            )
        else:
            offline_dtype_mapping = {
                "bigint": pd.Int64Dtype(),
                "int": pd.Int32Dtype(),
                "smallint": pd.Int16Dtype(),
                "tinyint": pd.Int8Dtype(),
                "float": np.dtype("float32"),
                "double": np.dtype("float64"),
            }
            if offline_type in offline_dtype_mapping:
                casted_feature = feature_column.astype(
                    offline_dtype_mapping[offline_type]
                )
                return casted_feature
            else:
                return feature_column  # handle gracefully, just return the column as-is

    @staticmethod
    def _cast_column_to_online_type(feature_column, online_type):
        online_type = online_type.lower()
        if online_type == "timestamp":
            # convert (if tz!=UTC) to utc, then make timezone unaware
            return pd.to_datetime(feature_column, utc=True).dt.tz_localize(None)
        elif online_type == "date":
            return pd.to_datetime(feature_column, utc=True).dt.date
        elif online_type.startswith("varchar") or online_type == "text":
            return feature_column.apply(lambda x: str(x) if x is not None else None)
        elif online_type == "boolean":
            return feature_column.apply(
                lambda x: (ast.literal_eval(x) if type(x) is str else x)
                if (x is not None and x != "")
                else None
            )
        elif online_type.startswith("decimal"):
            return feature_column.apply(
                lambda x: decimal.Decimal(x) if (x is not None) else None
            )
        else:
            online_dtype_mapping = {
                "bigint": pd.Int64Dtype(),
                "int": pd.Int32Dtype(),
                "smallint": pd.Int16Dtype(),
                "tinyint": pd.Int8Dtype(),
                "float": np.dtype("float32"),
                "double": np.dtype("float64"),
            }
            if online_type in online_dtype_mapping:
                casted_feature = feature_column.astype(
                    online_dtype_mapping[online_type]
                )
                return casted_feature
            else:
                return feature_column  # handle gracefully, just return the column as-is

    @staticmethod
    def cast_columns(df, schema, online=False):
        for _feat in schema:
            if not online:
                df[_feat.name] = Engine._cast_column_to_offline_type(
                    df[_feat.name], _feat.type
                )
            else:
                df[_feat.name] = Engine._cast_column_to_online_type(
                    df[_feat.name], _feat.online_type
                )
        return df

    @staticmethod
    def is_connector_type_supported(connector_type):
        return connector_type in [
            StorageConnector.HOPSFS,
            StorageConnector.S3,
            StorageConnector.KAFKA,
        ]

    @staticmethod
    def _start_offline_materialization(offline_write_options):
        if offline_write_options is not None:
            if "start_offline_materialization" in offline_write_options:
                return offline_write_options.get("start_offline_materialization")
            elif "start_offline_backfill" in offline_write_options:
                return offline_write_options.get("start_offline_backfill")
            else:
                return True
        else:
            return True
