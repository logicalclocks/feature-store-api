import os
import pandas as pd
from pyhive import hive


class Engine:
    def __init__(self, host, cert_folder, cert_key):
        self._host = host
        self._feature_store = None
        self._cert_folder = cert_folder
        self._cert_key = cert_key

    def sql(self, sql_query):
        print("Lazily executing query: {}".format(sql_query))
        with self._create_hive_connection() as hive_conn:
            result_df = pd.read_sql(sql_query, hive_conn)
        return result_df

    def show(self, sql_query, n):
        return self.sql(sql_query).head(n)

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
