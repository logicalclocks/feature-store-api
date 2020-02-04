# in case importing in %%local
from hopsworks import util

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    pass


class SparkEngine(metaclass=util.Singleton):
    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()

    def sql(self, sql_query):
        print("Lazily executing query: {}".format(sql_query))
        result_df = self._spark_session.sql(sql_query)
        self._spark_session.sparkContext.setJobGroup("", "")
        return result_df

    def setJobGroup(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)
