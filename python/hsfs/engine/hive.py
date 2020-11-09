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
import pandas as pd
from pyhive import hive
from sqlalchemy import create_engine


class Engine:
    def __init__(self, host, cert_folder, project, cert_key):
        self._host = host
        self._cert_folder = os.path.join(cert_folder, host, project)
        self._cert_key = cert_key

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

    def save_dataframe(
        self,
        table_name,
        feature_group,
        dataframe,
        save_mode,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
    ):
        raise NotImplementedError

    def profile_df(self, dataframe, relevant_columns, correlations, histograms):
        raise NotImplementedError

    def set_job_group(self, group_id, description):
        pass

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
