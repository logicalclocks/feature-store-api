# in case importing in %%local
try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    pass


class Engine:
    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()
        self._feature_store = None

    def sql(self, sql_query, dataframe_type):
        print("Lazily executing query: {}".format(sql_query))
        result_df = self._spark_session.sql(sql_query)
        self.set_job_group("", "")
        return self._return_dataframe_type(result_df, dataframe_type)

    def show(self, sql_query, dataframe_type, n):
        return self.sql(sql_query, dataframe_type).show(n)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)

    def _return_dataframe_type(dataframe, dataframe_type):
        if dataframe_type.lower() in ["default", "spark"]:
            return dataframe
        if dataframe_type.lower() == "pandas":
            return dataframe.toPandas()
        if dataframe_type.lower() == "numpy":
            return dataframe.toPandas().values
        if dataframe_type == "python":
            return dataframe.toPandas().values.tolist()

        raise TypeError(
            "Dataframe type `{}` not supported on this platform.".format(dataframe_type)
        )
