# in case importing in %%local
try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    pass


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SparkEngine(metaclass=Singleton):
    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()

    def sql(self, sql_query):
        print("Lazily executing query: {}".format(sql_query))
        result_df = self._spark_session.sql(sql_query)
        self._spark_session.sparkContext.setJobGroup("", "")
        return result_df

    def setJobGroup(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)
