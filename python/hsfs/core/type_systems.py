#
#   Copyright 2024 Hopsworks AB
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
from __future__ import annotations

import ast
import datetime
import decimal
from typing import TYPE_CHECKING, Literal, Union

import pytz
from hsfs.core.constants import HAS_ARROW, HAS_PANDAS, HAS_POLARS


if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import polars as pl

if HAS_ARROW:
    import pyarrow as pa

    # Decimal types are currently not supported
    _INT_TYPES = [pa.uint8(), pa.uint16(), pa.int8(), pa.int16(), pa.int32()]
    _BIG_INT_TYPES = [pa.uint32(), pa.int64()]
    _FLOAT_TYPES = [pa.float16(), pa.float32()]
    _DOUBLE_TYPES = [pa.float64()]
    _TIMESTAMP_UNIT = ["ns", "us", "ms", "s"]
    _BOOLEAN_TYPES = [pa.bool_()]
    _STRING_TYPES = [pa.string(), pa.large_string()]
    _DATE_TYPES = [pa.date32(), pa.date64()]
    _BINARY_TYPES = [pa.binary(), pa.large_binary()]

    PYARROW_HOPSWORKS_DTYPE_MAPPING = {
        **dict.fromkeys(_INT_TYPES, "int"),
        **dict.fromkeys(_BIG_INT_TYPES, "bigint"),
        **dict.fromkeys(_FLOAT_TYPES, "float"),
        **dict.fromkeys(_DOUBLE_TYPES, "double"),
        **dict.fromkeys(
            [
                *[pa.timestamp(unit) for unit in _TIMESTAMP_UNIT],
                *[
                    pa.timestamp(unit, tz=tz)
                    for unit in _TIMESTAMP_UNIT
                    for tz in pytz.all_timezones
                ],
            ],
            "timestamp",
        ),
        **dict.fromkeys(_BOOLEAN_TYPES, "boolean"),
        **dict.fromkeys(
            [
                *_STRING_TYPES,
                # Category type in pandas stored as dictinoary in pyarrow
                *[
                    pa.dictionary(
                        value_type=value_type, index_type=index_type, ordered=ordered
                    )
                    for value_type in _STRING_TYPES
                    for index_type in _INT_TYPES + _BIG_INT_TYPES
                    for ordered in [True, False]
                ],
            ],
            "string",
        ),
        **dict.fromkeys(_DATE_TYPES, "date"),
        **dict.fromkeys(_BINARY_TYPES, "binary"),
    }
else:
    PYARROW_HOPSWORKS_DTYPE_MAPPING = {}

# python cast column to offline type
if HAS_POLARS:
    import polars as pl

    polars_offline_dtype_mapping = {
        "bigint": pl.Int64,
        "int": pl.Int32,
        "smallint": pl.Int16,
        "tinyint": pl.Int8,
        "float": pl.Float32,
        "double": pl.Float64,
    }

    _polars_online_dtype_mapping = {
        "bigint": pl.Int64,
        "int": pl.Int32,
        "smallint": pl.Int16,
        "tinyint": pl.Int8,
        "float": pl.Float32,
        "double": pl.Float64,
    }

if HAS_PANDAS:
    import numpy as np
    import pandas as pd

    pandas_offline_dtype_mapping = {
        "bigint": pd.Int64Dtype(),
        "int": pd.Int32Dtype(),
        "smallint": pd.Int16Dtype(),
        "tinyint": pd.Int8Dtype(),
        "float": np.dtype("float32"),
        "double": np.dtype("float64"),
    }

    pandas_online_dtype_mapping = {
        "bigint": pd.Int64Dtype(),
        "int": pd.Int32Dtype(),
        "smallint": pd.Int16Dtype(),
        "tinyint": pd.Int8Dtype(),
        "float": np.dtype("float32"),
        "double": np.dtype("float64"),
    }


def convert_pandas_dtype_to_offline_type(arrow_type: str) -> str:
    # This is a simple type conversion between pandas dtypes and pyspark (hive) types,
    # using pyarrow types obatined from pandas dataframe to convert pandas typed fields,
    # A recurisive function  "convert_pandas_object_type_to_offline_type" is used to convert complex types like lists and structures
    # "_onvert_simple_pandas_dtype_to_offline_type" is used to convert simple types
    # In the backend, the types specified here will also be used for mapping to Avro types.
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_struct(arrow_type)
    ):
        return convert_pandas_object_type_to_offline_type(arrow_type)

    return convert_simple_pandas_dtype_to_offline_type(arrow_type)


def convert_pandas_object_type_to_offline_type(arrow_type: str) -> str:
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # figure out sub type
        sub_arrow_type = arrow_type.value_type
        subtype = convert_pandas_dtype_to_offline_type(sub_arrow_type)
        return "array<{}>".format(subtype)
    if pa.types.is_struct(arrow_type):
        struct_schema = {}
        for index in range(arrow_type.num_fields):
            struct_schema[arrow_type.field(index).name] = (
                convert_pandas_dtype_to_offline_type(arrow_type.field(index).type)
            )
        return (
            "struct<"
            + ",".join([f"{key}:{value}" for key, value in struct_schema.items()])
            + ">"
        )

    raise ValueError(f"dtype 'O' (arrow_type '{str(arrow_type)}') not supported")


def cast_pandas_column_to_offline_type(
    feature_column: pd.Series, offline_type: str
) -> pd.Series:
    offline_type = offline_type.lower()
    if offline_type == "timestamp":
        return pd.to_datetime(feature_column, utc=True).dt.tz_localize(None)
    elif offline_type == "date":
        return pd.to_datetime(feature_column, utc=True).dt.date
    elif (
        offline_type.startswith("array<")
        or offline_type.startswith("struct<")
        or offline_type == "boolean"
    ):
        return feature_column.apply(
            lambda x: (ast.literal_eval(x) if isinstance(x, str) else x)
            if (x is not None and x != "")
            else None
        )
    elif offline_type == "string":
        return feature_column.apply(lambda x: str(x) if x is not None else None)
    elif offline_type.startswith("decimal"):
        return feature_column.apply(
            lambda x: decimal.Decimal(x) if (x is not None) else None
        )
    else:
        if offline_type in pandas_offline_dtype_mapping:
            return feature_column.astype(pandas_offline_dtype_mapping[offline_type])
        else:
            return feature_column  # handle gracefully, just return the column as-is


def cast_polars_column_to_offline_type(
    feature_column: pl.Series, offline_type: str
) -> pl.Series:
    offline_type = offline_type.lower()
    if offline_type == "timestamp":
        # convert (if tz!=UTC) to utc, then make timezone unaware
        return feature_column.cast(pl.Datetime(time_zone=None))
    elif offline_type == "date":
        return feature_column.cast(pl.Date)
    elif (
        offline_type.startswith("array<")
        or offline_type.startswith("struct<")
        or offline_type == "boolean"
    ):
        return feature_column.map_elements(
            lambda x: (ast.literal_eval(x) if isinstance(x, str) else x)
            if (x is not None and x != "")
            else None
        )
    elif offline_type == "string":
        return feature_column.map_elements(lambda x: str(x) if x is not None else None)
    elif offline_type.startswith("decimal"):
        return feature_column.map_elements(
            lambda x: decimal.Decimal(x) if (x is not None) else None
        )
    else:
        if offline_type in polars_offline_dtype_mapping:
            return feature_column.cast(polars_offline_dtype_mapping[offline_type])
        else:
            return feature_column  # handle gracefully, just return the column as-is


def cast_column_to_offline_type(
    feature_column: Union[pd.Series, pl.Series], offline_type: str
) -> pd.Series:
    if isinstance(feature_column, pd.Series):
        return cast_pandas_column_to_offline_type(feature_column, offline_type.lower())
    elif isinstance(feature_column, pl.Series):
        return cast_polars_column_to_offline_type(feature_column, offline_type.lower())


def cast_column_to_online_type(
    feature_column: pd.Series, online_type: str
) -> pd.Series:
    online_type = online_type.lower()
    if online_type == "timestamp":
        # convert (if tz!=UTC) to utc, then make timezone unaware
        return pd.to_datetime(feature_column, utc=True).dt.tz_localize(None)
    elif online_type == "date":
        return pd.to_datetime(feature_column, utc=True).dt.date
    elif online_type.startswith("varchar") or online_type == "text":
        return feature_column.apply(lambda x: str(x) if x is not None else None)
    elif online_type == "boolean":
        return feature_column.apply(
            lambda x: (ast.literal_eval(x) if isinstance(x, str) else x)
            if (x is not None and x != "")
            else None
        )
    elif online_type.startswith("decimal"):
        return feature_column.apply(
            lambda x: decimal.Decimal(x) if (x is not None) else None
        )
    else:
        if online_type in pandas_online_dtype_mapping:
            casted_feature = feature_column.astype(
                pandas_online_dtype_mapping[online_type]
            )
            return casted_feature
        else:
            return feature_column  # handle gracefully, just return the column as-is


def convert_simple_pandas_dtype_to_offline_type(arrow_type: str) -> str:
    try:
        return PYARROW_HOPSWORKS_DTYPE_MAPPING[arrow_type]
    except KeyError as err:
        raise ValueError(f"dtype '{arrow_type}' not supported") from err


def translate_legacy_spark_type(
    output_type: str,
) -> Literal[
    "STRING",
    "BINARY",
    "BYTE",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "TIMESTAMP",
    "DATE",
    "BOOLEAN",
]:
    if output_type == "StringType()":
        return "STRING"
    elif output_type == "BinaryType()":
        return "BINARY"
    elif output_type == "ByteType()":
        return "BYTE"
    elif output_type == "ShortType()":
        return "SHORT"
    elif output_type == "IntegerType()":
        return "INT"
    elif output_type == "LongType()":
        return "LONG"
    elif output_type == "FloatType()":
        return "FLOAT"
    elif output_type == "DoubleType()":
        return "DOUBLE"
    elif output_type == "TimestampType()":
        return "TIMESTAMP"
    elif output_type == "DateType()":
        return "DATE"
    elif output_type == "BooleanType()":
        return "BOOLEAN"
    else:
        return "STRING"  # handle gracefully, and return STRING type, the default for spark udfs


def convert_spark_type_to_offline_type(spark_type_string: str) -> str:
    if spark_type_string.endswith("Type()"):
        spark_type_string = translate_legacy_spark_type(spark_type_string)
    if spark_type_string == "STRING":
        return "STRING"
    elif spark_type_string == "BINARY":
        return "BINARY"
    elif spark_type_string == "BYTE":
        return "INT"
    elif spark_type_string == "SHORT":
        return "INT"
    elif spark_type_string == "INT":
        return "INT"
    elif spark_type_string == "LONG":
        return "BIGINT"
    elif spark_type_string == "FLOAT":
        return "FLOAT"
    elif spark_type_string == "DOUBLE":
        return "DOUBLE"
    elif spark_type_string == "TIMESTAMP":
        return "TIMESTAMP"
    elif spark_type_string == "DATE":
        return "DATE"
    elif spark_type_string == "BOOLEAN":
        return "BOOLEAN"
    else:
        raise ValueError(
            f"Return type {spark_type_string} not supported for transformation functions."
        )


def infer_spark_type(output_type):
    if not output_type:
        return "STRING"  # STRING is default type for spark udfs

    if isinstance(output_type, str):
        if output_type.endswith("Type()"):
            return translate_legacy_spark_type(output_type)
        output_type = output_type.lower()

    if output_type in (str, "str", "string"):
        return "STRING"
    elif output_type in (bytes, "binary"):
        return "BINARY"
    elif output_type in (np.int8, "int8", "byte", "tinyint"):
        return "BYTE"
    elif output_type in (np.int16, "int16", "short", "smallint"):
        return "SHORT"
    elif output_type in (int, "int", "integer", np.int32):
        return "INT"
    elif output_type in (np.int64, "int64", "long", "bigint"):
        return "LONG"
    elif output_type in (float, "float"):
        return "FLOAT"
    elif output_type in (np.float64, "float64", "double"):
        return "DOUBLE"
    elif output_type in (
        datetime.datetime,
        np.datetime64,
        "datetime",
        "timestamp",
    ):
        return "TIMESTAMP"
    elif output_type in (datetime.date, "date"):
        return "DATE"
    elif output_type in (bool, "boolean", "bool"):
        return "BOOLEAN"
    else:
        raise TypeError("Not supported type %s." % output_type)
