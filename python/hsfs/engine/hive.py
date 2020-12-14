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
import time
import pandas as pd
import numpy as np
from pyhive import hive
from sqlalchemy import create_engine

from hsfs import feature, util
from hsfs.core import feature_group_api, dataset_api, job_api, job_configuration


class Engine:
    def __init__(self, host, cert_folder, project, cert_key):
        self._host = host
        self._cert_folder = os.path.join(cert_folder, host, project)
        self._cert_key = cert_key

        self._dataset_api = dataset_api.DatasetApi()
        self._job_api = job_api.JobApi()

    def sql(self, sql_query, feature_store, online_conn, dataframe_type):
        if not online_conn:
            return self._sql_offline(sql_query, feature_store, dataframe_type)
        else:
            return self._jdbc(sql_query, online_conn, dataframe_type)

    def _sql_offline(self, sql_query, feature_store, dataframe_type):
        print("Lazily executing query: {}".format(sql_query))
        with self._create_hive_connection(feature_store) as hive_conn:
            result_df = pd.read_sql(sql_query, hive_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def _jdbc(self, sql_query, connector, dataframe_type):
        with self._create_mysql_connection(connector) as mysql_conn:
            result_df = pd.read_sql(sql_query, mysql_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def show(self, sql_query, feature_store, n, online_conn):
        return self.sql(sql_query, feature_store, online_conn, "default").head(n)

    def register_temporary_table(self, query, storage_connector, alias):
        raise NotImplementedError

    def profile_df(self, dataframe, relevant_columns, correlations, histograms):
        raise NotImplementedError

    def set_job_group(self, group_id, description):
        pass

    def convert_to_default_dataframe(self, dataframe):
        if isinstance(dataframe, pd.DataFrame):
            return dataframe

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: pandas dataframe. "
            + "The provided dataframe has type: {}".format(type(dataframe))
        )

    def parse_schema_feature_group(self, dataframe):
        return [
            feature.Feature(feat_name, self._convert_pandas_type(feat_type))
            for feat_name, feat_type in dataframe.dtypes.items()
        ]

    def _convert_pandas_type(self, dtype):
        # This is a simple type conversion between pandas type and pyspark types.
        # In PySpark they use PyArrow to do the schema conversion, but this python layer
        # should be as thin as possible. Adding PyArrow will make the library less flexible.
        # If the conversion fails, users can always fall back and provide their own types

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
        # Setup job for ingestion
        fg_api = feature_group_api.FeatureGroupApi(feature_group.feature_store_id)
        ingestion_job = fg_api.ingestion(feature_group)

        # Upload dataframe into Hopsworks
        self._dataset_api.upload(feature_group, ingestion_job.data_path, dataframe)

        # Configure ingestion job
        job_conf = job_configuration.JobConfiguration(
            app_path=ingestion_job.job_path, default_args=ingestion_job.job_conf_path
        )

        job = self._job_api.create(
            self._generate_job_name("ingestion", feature_group), job_conf
        )

        self._job_api.launch(job.name)

    def _generate_job_name(self, prefix, feature_group):
        return "_".join(
            [prefix, util.feature_group_name(feature_group), str(time.time())]
        )

    def _create_hive_connection(self, feature_store):
        return hive.Connection(
            host=self._host,
            port=9085,
            # database needs to be set every time, 'default' doesn't work in pyhive
            database=feature_store,
            auth="CERTIFICATES",
            truststore=os.path.join(self._cert_folder, "trustStore.jks"),
            keystore=os.path.join(self._cert_folder, "keyStore.jks"),
            keystore_password=self._cert_key,
        )

    def _create_mysql_connection(self, online_conn):
        online_options = online_conn.spark_options()
        # Here we are replacing the first part of the string returned by Hopsworks,
        # jdbc:mysql:// with the sqlalchemy one + username and password
        sql_alchemy_conn_str = online_options["url"].replace(
            "jdbc:mysql://",
            "mysql+pymysql://"
            + online_options["user"]
            + ":"
            + online_options["password"]
            + "@",
        )
        sql_alchemy_engine = create_engine(sql_alchemy_conn_str, pool_recycle=3600)
        return sql_alchemy_engine.connect()

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
