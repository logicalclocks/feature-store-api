import os
import pandas as pd
from pyhive import hive


class Engine:
    def __init__(self, host, cert_folder, cert_key):
        self._host = host
        self._feature_store = None
        self._cert_folder = cert_folder
        self._cert_key = cert_key

    def sql(self, sql_query, dataframe_type):
        print("Lazily executing query: {}".format(sql_query))
        with self._create_hive_connection() as hive_conn:
            result_df = pd.read_sql(sql_query, hive_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def show(self, sql_query, n):
        return self.sql(sql_query, "default").head(n)

    def set_job_group(self, group_id, description):
        pass

    def _create_hive_connection(self):
        return hive.Connection(
            host=self._host,
            port=9085,
            database=self._feature_store,
            auth="CERTIFICATES",
            truststore=os.path.join(self._cert_folder, "trustStore.jks"),
            keystore=os.path.join(self._cert_folder, "keyStore.jks"),
            keystore_password=self._cert_key,
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
