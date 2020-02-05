# in case importing in %%local
try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    pass


class Engine:
    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()
        self._feature_store = None

    def sql(self, sql_query):
        print("Lazily executing query: {}".format(sql_query))
        result_df = self._spark_session.sql(sql_query)
        self.set_job_group("", "")
        return result_df

    def show(self, sql_query, n):
        return self.sql(sql_query).show(n)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)
