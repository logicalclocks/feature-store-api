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

from io import BytesIO
from pyhive import hive
from urllib.parse import urlparse

from hsfs import client, feature, util
from hsfs.core import (
    feature_group_api,
    dataset_api,
    job_api,
    ingestion_job_conf,
    statistics_api,
    training_dataset_api,
    training_dataset_job_conf,
)
from hsfs.constructor import query
from hsfs.client import exceptions


class Engine:

    APP_OP_INSERT_FG = "insert_fg"

    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._job_api = job_api.JobApi()

    def sql(self, sql_query, feature_store, online_conn, dataframe_type, read_options):
        if not online_conn:
            return self._sql_offline(sql_query, feature_store, dataframe_type)
        else:
            return self._jdbc(sql_query, online_conn, dataframe_type, read_options)

    def _sql_offline(self, sql_query, feature_store, dataframe_type):
        print("Lazily executing query: {}".format(sql_query))
        with self._create_hive_connection(feature_store) as hive_conn:
            result_df = pd.read_sql(sql_query, hive_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def _jdbc(self, sql_query, connector, dataframe_type, read_options):
        with util.create_mysql_connection(
            connector, "external" in read_options and read_options["external"]
        ) as mysql_conn:
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
                "{} training dataset format is not supported to read as pandas dataframe. If you are using `tfrecord` use the `.tf_data` helper functions.".format(
                    data_format
                )
            )

    def _read_hopsfs(self, location, data_format):
        # providing more informative error
        try:
            from pydoop import hdfs
        except ImportError as err:
            raise ModuleNotFoundError(
                "Reading training dataset from HopsFS requires `pydoop`"
            ) from err

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

    def show(self, sql_query, feature_store, n, online_conn):
        return self.sql(sql_query, feature_store, online_conn, "default", {}).head(n)

    def register_on_demand_temporary_table(self, on_demand_fg, alias):
        raise NotImplementedError

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        # No op to avoid query failure
        pass

    def profile(self, metadata_instance):
        stat_api = statistics_api.StatisticsApi(
            metadata_instance.feature_store_id, metadata_instance.ENTITY_TYPE
        )
        job = stat_api.compute(metadata_instance)
        print(
            "Statistics Job started successfully, you can follow the progress at {}".format(
                self._get_job_url(job.href)
            )
        )

        self._wait_for_job(job)

    def set_job_group(self, group_id, description):
        pass

    def convert_to_default_dataframe(self, dataframe):
        if isinstance(dataframe, pd.DataFrame):
            dataframe.columns = [x.lower() for x in dataframe.columns]
            return dataframe

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: pandas dataframe. "
            + "The provided dataframe has type: {}".format(type(dataframe))
        )

    def parse_schema_feature_group(self, dataframe):
        return [
            feature.Feature(feat_name.lower(), self._convert_pandas_type(feat_type))
            for feat_name, feat_type in dataframe.dtypes.items()
        ]

    def _convert_pandas_type(self, dtype):
        # This is a simple type conversion between pandas type and pyspark types.
        # In PySpark they use PyArrow to do the schema conversion, but this python layer
        # should be as thin as possible. Adding PyArrow will make the library less flexible.
        # If the conversion fails, users can always fall back and provide their own types

        # TODO(Fabio): consider arrays
        if dtype == np.dtype("O"):
            # This is an object, fall back to string
            return "string"
        elif dtype == np.dtype("int32"):
            return "int"
        elif dtype == np.dtype("int64"):
            return "bigint"
        elif dtype == np.dtype("float32"):
            return "float"
        elif dtype == np.dtype("float64"):
            return "double"

        return "string"

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
            "Ingestion Job started successfully, you can follow the progress at {}".format(
                self._get_job_url(ingestion_job.job.href)
            )
        )

        self._wait_for_job(ingestion_job.job, offline_write_options)

        return ingestion_job.job

    def write_training_dataset(
        self, training_dataset, dataset, user_write_options, save_mode
    ):
        if not isinstance(dataset, query.Query):
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

        td_api = training_dataset_api.TrainingDatasetApi(
            training_dataset.feature_store_id
        )
        td_job = td_api.compute(training_dataset, td_app_conf)
        print(
            "Training dataset job started successfully, you can follow the progress at {}".format(
                self._get_job_url(td_job.href)
            )
        )

        # If the user passed the wait_for_job option consider it,
        # otherwise use the default True
        self._wait_for_job(td_job, user_write_options)

        return td_job

    def _create_hive_connection(self, feature_store):
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

    def _get_job_url(self, href: str):
        """Use the endpoint returned by the API to construct the UI url for jobs

        Args:
            href (str): the endpoint returned by the API
        """
        url_splits = urlparse(href)
        project_id = url_splits.path.split("/")[4]
        ui_url = url_splits._replace(
            path="hopsworks/#!/project/{}/jobs".format(project_id)
        )
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
            data_format="CSV",
            data_options=[
                {"name": "header", "value": "true"},
                {"name": "inferSchema", "value": "true"},
            ],
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
