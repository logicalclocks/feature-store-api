import json
import pandas as pd
import numpy as np

# in case importing in %%local
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.rdd import RDD
except ModuleNotFoundError:
    pass

from hopsworks import feature
from hopsworks.storage_connector import StorageConnector


class Engine:
    def __init__(self):
        self._spark_session = SparkSession.builder.getOrCreate()
        self._spark_context = self._spark_session.sparkContext

    def sql(self, sql_query, feature_store, dataframe_type):
        print("Lazily executing query: {}".format(sql_query))
        # set feature store
        self._spark_session.sql("USE {}".format(feature_store))
        result_df = self._spark_session.sql(sql_query)
        self.set_job_group("", "")
        return self._return_dataframe_type(result_df, dataframe_type)

    def show(self, sql_query, feature_store, n):
        return self.sql(sql_query, feature_store, "default").show(n)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)

    def _return_dataframe_type(self, dataframe, dataframe_type):
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

    def convert_to_default_dataframe(self, dataframe):
        if isinstance(dataframe, pd.DataFrame):
            return self._spark_session.createDataFrame(dataframe)
        if isinstance(dataframe, list):
            dataframe = np.array(dataframe)
        if isinstance(dataframe, np.ndarray):
            if dataframe.ndim != 2:
                raise TypeError(
                    "Cannot convert numpy array that do not have two dimensions to a dataframe. "
                    "The number of dimensions are: {}".format(dataframe.ndim)
                )
            num_cols = dataframe.shape[1]
            dataframe_dict = {}
            for n_col in list(range(num_cols)):
                col_name = "col_" + str(n_col)
                dataframe_dict[col_name] = dataframe[:, n_col]
            pandas_df = pd.DataFrame(dataframe_dict)
            return self._spark_session.createDataFrame(pandas_df)
        if isinstance(dataframe, RDD):
            return dataframe.toDF()
        if isinstance(dataframe, DataFrame):
            return dataframe
        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, "
            "pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: {}".format(
                type(dataframe)
            )
        )

    def write(
        self, dataframe, storage_connector, data_format, write_mode, write_options, path
    ):
        print("write feature dataframe, write_mode: {}".format(write_mode))

        if data_format.lower() == "tsv":
            data_format = "csv"

        if storage_connector.connector_type == StorageConnector.S3:
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.access.key", storage_connector.access_key
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                "fs.s3a.secret.key", storage_connector.access_key
            )
            path = path.replace("s3", "s3a", 1)
            print(path)

        dataframe.write.format(data_format).options(**write_options).mode(
            write_mode
        ).save(path)

    def write_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example", **provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true", **provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true", **provided_options)
        else:
            options = {}
        return options

    def parse_schema(self, dataframe):
        return [
            feature.Feature(
                feat["name"], feat["type"], feat["metadata"].get("description", "")
            )
            for feat in json.loads(dataframe.schema.json())["fields"]
        ]
