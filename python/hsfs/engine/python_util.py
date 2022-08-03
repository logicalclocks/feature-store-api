#
#   Copyright 2022 Hopsworks AB
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
import re
import warnings
import pyarrow as pa
import json

import great_expectations as ge

from pyhive import hive
from typing import TypeVar, Optional, Dict, Any

from hsfs import client, feature, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.engine import engine_base
from thrift.transport.TTransport import TTransportException
from pyhive.exc import OperationalError


class EngineUtil(engine_base.EngineUtilBase):
    def __init__(self):
        # cache the sql engine which contains the connection pool
        self._mysql_online_fs_engine = None

    def set_job_group(self, group_id: int, description: str) -> None:
        pass

    def sql(self, sql_query, feature_store, online_conn, dataframe_type, read_options) -> Any:
        if not online_conn:
            return self._sql_offline(sql_query, feature_store, dataframe_type)
        else:
            return self._jdbc(sql_query, online_conn, dataframe_type, read_options)

    def profile(
        self,
        dataframe,
        relevant_columns,
        correlations,
        histograms,
        exact_uniqueness=True,
    ) -> Any:
        # TODO: add statistics for correlations, histograms and exact_uniqueness
        if not relevant_columns:
            stats = dataframe.describe()
        else:
            target_cols = [col for col in dataframe.columns if col in relevant_columns]
            stats = dataframe[target_cols].describe()
        final_stats = []
        for col in stats.columns:
            stat = self._convert_pandas_statistics(stats[col].to_dict())
            stat["dataType"] = (
                "Fractional"
                if isinstance(stats[col].dtype, type(np.dtype(np.float64)))
                else "Integral"
            )
            stat["isDataTypeInferred"] = "false"
            stat["column"] = col.split(".")[-1]
            stat["completeness"] = 1
            final_stats.append(stat)
        return json.dumps({"columns": final_stats})

    def validate_with_great_expectations(
        self,
        dataframe: pd.DataFrame,
        expectation_suite: TypeVar("ge.core.ExpectationSuite"),
        ge_validate_kwargs: Optional[Dict[Any, Any]] = {},
    ) -> Any:
        report = ge.from_pandas(
            dataframe, expectation_suite=expectation_suite
        ).validate(**ge_validate_kwargs)
        return report

    def convert_to_default_dataframe(self, dataframe) -> Any:
        if isinstance(dataframe, pd.DataFrame):
            upper_case_features = [
                col for col in dataframe.columns if any(re.finditer("[A-Z]", col))
            ]
            if len(upper_case_features) > 0:
                warnings.warn(
                    "The ingested dataframe contains upper case letters in feature names: `{}`. "
                    "Feature names are sanitized to lower case in the feature store.".format(
                        upper_case_features
                    ),
                    util.FeatureGroupWarning,
                )

            # making a shallow copy of the dataframe so that column names are unchanged
            dataframe_copy = dataframe.copy(deep=False)
            dataframe_copy.columns = [x.lower() for x in dataframe_copy.columns]
            return dataframe_copy

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: pandas dataframe. "
            + "The provided dataframe has type: {}".format(type(dataframe))
        )

    def parse_schema_feature_group(self, dataframe, time_travel_format=None) -> Any:
        arrow_schema = pa.Schema.from_pandas(dataframe)
        features = []
        for feat_name, feat_type in dataframe.dtypes.items():
            name = feat_name.lower()
            try:
                converted_type = self._convert_pandas_type(
                    feat_type, arrow_schema.field(feat_name).type
                )
            except ValueError as e:
                raise FeatureStoreException(f"Feature '{name}': {str(e)}")
            features.append(feature.Feature(name, converted_type))
        return features

    def parse_schema_training_dataset(self, dataframe) -> None:
        raise NotImplementedError(
            "Training dataset creation from Dataframes is not "
            + "supported in Python environment. Use HSFS Query object instead."
        )

    def split_labels(self, df, labels) -> tuple:
        if labels:
            labels_df = df[labels]
            df_new = df.drop(columns=labels)
            return df_new, labels_df
        else:
            return df, None

    def is_spark_dataframe(self, dataframe) -> bool:
        return False

    def create_empty_df(self, streaming_df) -> None:
        raise NotImplementedError(
            "Create empty df is only available with Spark Engine."
        )

    def setup_storage_connector(self, storage_connector, path=None) -> None:
        raise NotImplementedError(
            "Setup storage connector is only available with Spark Engine."
        )

    def _sql_offline(self, sql_query, feature_store, dataframe_type):
        with self._create_hive_connection(feature_store) as hive_conn:
            result_df = pd.read_sql(sql_query, hive_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

    def _create_hive_connection(self, feature_store):
        try:
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
        except (TTransportException, AttributeError):
            raise ValueError(
                f"Cannot connect to hive server. Please check the host name '{client.get_instance()._host}' "
                "is correct and make sure port '9085' is open on host server."
            )
        except OperationalError as e:
            if e.args[0].status.statusCode == 3:
                raise RuntimeError(
                    f"Cannot access feature store '{feature_store}'. Please check if your project has the access right."
                    f" It is possible to request access from data owners of '{feature_store}'."
                )

    def _jdbc(self, sql_query, connector, dataframe_type, read_options):
        if self._mysql_online_fs_engine is None:
            self._mysql_online_fs_engine = util.create_mysql_engine(
                connector,
                (
                    isinstance(client.get_instance(), client.external.Client)
                    if "external" not in read_options
                    else read_options["external"]
                ),
            )
        with self._mysql_online_fs_engine.connect() as mysql_conn:
            result_df = pd.read_sql(sql_query, mysql_conn)
        return self._return_dataframe_type(result_df, dataframe_type)

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

    def _convert_pandas_statistics(self, stat):
        # For now transformation only need 25th, 50th, 75th percentiles
        # TODO: calculate properly all percentiles
        content_dict = {}
        percentiles = []
        if "25%" in stat:
            percentiles = [0] * 100
            percentiles[24] = stat["25%"]
            percentiles[49] = stat["50%"]
            percentiles[74] = stat["75%"]
        if "mean" in stat:
            content_dict["mean"] = stat["mean"]
        if "mean" in stat and "count" in stat:
            content_dict["sum"] = stat["mean"] * stat["count"]
        if "max" in stat:
            content_dict["maximum"] = stat["max"]
        if "std" in stat:
            content_dict["stdDev"] = stat["std"]
        if "min" in stat:
            content_dict["minimum"] = stat["min"]
        if percentiles:
            content_dict["approxPercentiles"] = percentiles
        return content_dict

    def _convert_pandas_type(self, dtype, arrow_type):
        # This is a simple type conversion between pandas dtypes and pyspark (hive) types,
        # using pyarrow types to convert "O (object)"-typed fields.
        # In the backend, the types specified here will also be used for mapping to Avro types.
        if dtype == np.dtype("O"):
            return self._infer_type_pyarrow(arrow_type)

        return self._convert_simple_pandas_type(dtype)

    def _infer_type_pyarrow(self, arrow_type):
        if pa.types.is_list(arrow_type):
            # figure out sub type
            sub_arrow_type = arrow_type.value_type
            sub_dtype = np.dtype(sub_arrow_type.to_pandas_dtype())
            subtype = self._convert_pandas_type(sub_dtype, sub_arrow_type)
            return "array<{}>".format(subtype)
        if pa.types.is_struct(arrow_type):
            # best effort, based on pyarrow's string representation
            return str(arrow_type)
        # Currently not supported
        # elif pa.types.is_decimal(arrow_type):
        #    return str(arrow_type).replace("decimal128", "decimal")
        elif pa.types.is_date(arrow_type):
            return "date"
        elif pa.types.is_binary(arrow_type):
            return "binary"
        elif pa.types.is_string(arrow_type) or pa.types.is_unicode(arrow_type):
            return "string"

        raise ValueError(f"dtype 'O' (arrow_type '{str(arrow_type)}') not supported")

    def _convert_simple_pandas_type(self, dtype):
        if dtype == np.dtype("uint8"):
            return "int"
        elif dtype == np.dtype("uint16"):
            return "int"
        elif dtype == np.dtype("int8"):
            return "int"
        elif dtype == np.dtype("int16"):
            return "int"
        elif dtype == np.dtype("int32"):
            return "int"
        elif dtype == np.dtype("uint32"):
            return "bigint"
        elif dtype == np.dtype("int64"):
            return "bigint"
        elif dtype == np.dtype("float16"):
            return "float"
        elif dtype == np.dtype("float32"):
            return "float"
        elif dtype == np.dtype("float64"):
            return "double"
        elif dtype == np.dtype("datetime64[ns]"):
            return "timestamp"
        elif dtype == np.dtype("bool"):
            return "boolean"
        elif dtype == "category":
            return "string"

        raise ValueError(f"dtype '{dtype}' not supported")
