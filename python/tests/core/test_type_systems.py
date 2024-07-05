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
import datetime

import pytest
from hsfs.core import type_systems
from hsfs.core.constants import HAS_ARROW, HAS_PANDAS


if HAS_ARROW:
    import pyarrow as pa

if HAS_PANDAS:
    import numpy as np
    import pandas as pd

    rng_engine = np.random.default_rng(42)


class TestTypeSystems:
    @pytest.mark.skipif(
        not HAS_ARROW or not HAS_PANDAS, reason="Arrow or Pandas are not installed"
    )
    def test_infer_type_pyarrow_list(self):
        # Act
        result = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_type=pa.list_(pa.int8())
        )

        # Assert
        assert result == "array<int>"

    def test_infer_type_pyarrow_large_list(self):
        # Act
        result = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_type=pa.large_list(pa.int8())
        )

        # Assert
        assert result == "array<int>"

    def test_infer_type_pyarrow_struct(self):
        # Act
        result = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_type=pa.struct([pa.field("f1", pa.int32())])
        )

        # Assert
        assert result == "struct<f1:int>"

    def test_infer_type_pyarrow_date32(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.date32()
        )

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_date64(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.date64()
        )

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_binary(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.binary()
        )

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_large_binary(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.large_binary()
        )

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_string(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.string()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_large_string(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.large_string()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_utf8(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.utf8()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_other(self):
        # Act
        with pytest.raises(ValueError) as e_info:
            type_systems.convert_simple_pandas_dtype_to_offline_type(
                arrow_type=pa.time32("s")
            )

        # Assert
        assert str(e_info.value) == "dtype 'time32[s]' not supported"

    def test_infer_type_pyarrow_struct_with_decimal_fields(self):
        # Arrange
        mapping = {f"user{i}": 2.0 for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:double,user1:double>"

    def test_infer_type_pyarrow_struct_with_decimal_and_string_fields(self):
        # Arrange
        mapping = {"user0": 2.0, "user1": "test"}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:double,user1:string>"

    def test_infer_type_pyarrow_struct_with_list_fields(self):
        # Arrange
        mapping = {"user0": list(rng_engine.normal(size=5)), "user1": ["test", "test"]}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:array<double>,user1:array<string>>"

    def test_infer_type_pyarrow_struct_with_string_fields(self):
        # Arrange
        mapping = {f"user{i}": "test" for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:string,user1:string>"

    def test_infer_type_pyarrow_struct_with_struct_fields(self):
        # Arrange
        mapping = {f"user{i}": {"value": "test"} for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:struct<value:string>,user1:struct<value:string>>"
        )

    def test_infer_type_pyarrow_struct_with_struct_fields_with_list_values(self):
        # Arrange
        mapping = {
            f"user{i}": {"value": list(rng_engine.normal(size=5))} for i in range(2)
        }
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:struct<value:array<double>>,user1:struct<value:array<double>>>"
        )

    def test_infer_type_pyarrow_struct_with_nested_struct_fields(self):
        # Arrange
        mapping = {f"user{i}": {"value": {"value": "test"}} for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:struct<value:struct<value:string>>,user1:struct<value:struct<value:string>>>"
        )

    def test_infer_type_pyarrow_list_of_struct_fields(self):
        # Arrange
        mapping = [{"value": rng_engine.normal(size=5)}]
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "array<struct<value:array<double>>>"

    def test_infer_type_pyarrow_struct_with_list_of_struct_fields(self):
        # Arrange
        mapping = {f"user{i}": [{"value": rng_engine.normal(size=5)}] for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        # Act
        arrow_type = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:array<struct<value:array<double>>>,user1:array<struct<value:array<double>>>>"
        )

    def test_convert_simple_pandas_type_uint8(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.uint8()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_uint16(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.uint16()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int8(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int8()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int16(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int16()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int32(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int32()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_uint32(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.uint32()
        )

        # Assert
        assert result == "bigint"

    def test_convert_simple_pandas_type_int64(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int64()
        )

        # Assert
        assert result == "bigint"

    def test_convert_simple_pandas_type_float16(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.float16()
        )

        # Assert
        assert result == "float"

    def test_convert_simple_pandas_type_float32(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.float32()
        )

        # Assert
        assert result == "float"

    def test_convert_simple_pandas_type_float64(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.float64()
        )

        # Assert
        assert result == "double"

    def test_convert_simple_pandas_type_datetime64ns(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ns")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64nstz(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ns", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64us(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="us")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64ustz(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="us", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64ms(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ms")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64mstz(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ms", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64s(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="s")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64stz(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="s", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_bool(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.bool_()
        )

        # Assert
        assert result == "boolean"

    def test_convert_simple_pandas_type_category_unordered(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.string(), index_type=pa.int8(), ordered=False
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_large_string_category_unordered(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.large_string(), index_type=pa.int64(), ordered=False
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_large_string_category_ordered(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.large_string(), index_type=pa.int64(), ordered=True
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_category_ordered(self):
        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.string(), index_type=pa.int8(), ordered=True
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_other(self):
        # Act
        with pytest.raises(ValueError) as e_info:
            type_systems.convert_simple_pandas_dtype_to_offline_type(arrow_type="other")

        # Assert
        assert str(e_info.value) == "dtype 'other' not supported"

    def test_infer_spark_type_string_type_1(self):
        # Act
        result = type_systems.infer_spark_type(str)

        # Assert
        assert result == "STRING"

    def test_infer_spark_type_string_type_2(self):
        # Act
        result = type_systems.infer_spark_type("str")

        # Assert
        assert result == "STRING"

    def test_infer_spark_type_string_type_3(self):
        # Act
        result = type_systems.infer_spark_type("string")

        # Assert
        assert result == "STRING"

    def test_infer_spark_type_byte_type_1(self):
        # Act
        result = type_systems.infer_spark_type(bytes)
        result1 = type_systems.infer_spark_type("BinaryType()")

        # Assert
        assert result == "BINARY"
        assert result1 == "BINARY"

    def test_infer_spark_type_int8_type_1(self):
        # Act
        result = type_systems.infer_spark_type(np.int8)

        # Assert
        assert result == "BYTE"

    def test_infer_spark_type_int8_type_2(self):
        # Act
        result = type_systems.infer_spark_type("int8")

        # Assert
        assert result == "BYTE"

    def test_infer_spark_type_int8_type_3(self):
        # Act
        result = type_systems.infer_spark_type("byte")
        result1 = type_systems.infer_spark_type("ByteType()")

        # Assert
        assert result == "BYTE"
        assert result1 == "BYTE"

    def test_infer_spark_type_int16_type_1(self):
        # Act
        result = type_systems.infer_spark_type(np.int16)

        # Assert
        assert result == "SHORT"

    def test_infer_spark_type_int16_type_2(self):
        # Act
        result = type_systems.infer_spark_type("int16")

        # Assert
        assert result == "SHORT"

    def test_infer_spark_type_int16_type_3(self):
        # Act
        result = type_systems.infer_spark_type("short")
        result1 = type_systems.infer_spark_type("ShortType()")

        # Assert
        assert result == "SHORT"
        assert result1 == "SHORT"

    def test_infer_spark_type_int_type_1(self):
        # Act
        result = type_systems.infer_spark_type(int)

        # Assert
        assert result == "INT"

    def test_infer_spark_type_int_type_2(self):
        # Act
        result = type_systems.infer_spark_type("int")

        # Assert
        assert result == "INT"

    def test_infer_spark_type_int_type_3(self):
        # Act
        result = type_systems.infer_spark_type(np.int32)
        result1 = type_systems.infer_spark_type("IntegerType()")

        # Assert
        assert result == "INT"
        assert result1 == "INT"

    def test_infer_spark_type_int64_type_1(self):
        # Act
        result = type_systems.infer_spark_type(np.int64)

        # Assert
        assert result == "LONG"

    def test_infer_spark_type_int64_type_2(self):
        # Act
        result = type_systems.infer_spark_type("int64")

        # Assert
        assert result == "LONG"

    def test_infer_spark_type_int64_type_3(self):
        # Act
        result = type_systems.infer_spark_type("long")

        # Assert
        assert result == "LONG"

    def test_infer_spark_type_int64_type_4(self):
        # Act
        result = type_systems.infer_spark_type("bigint")
        result1 = type_systems.infer_spark_type("LongType()")

        # Assert
        assert result == "LONG"
        assert result1 == "LONG"

    def test_infer_spark_type_float_type_1(self):
        # Act
        result = type_systems.infer_spark_type(float)

        # Assert
        assert result == "FLOAT"

    def test_infer_spark_type_float_type_2(self):
        # Act
        result = type_systems.infer_spark_type("float")
        result1 = type_systems.infer_spark_type("FloatType()")

        # Assert
        assert result == "FLOAT"
        assert result1 == "FLOAT"

    def test_infer_spark_type_double_type_1(self):
        # Act
        result = type_systems.infer_spark_type(np.float64)

        # Assert
        assert result == "DOUBLE"

    def test_infer_spark_type_double_type_2(self):
        # Act
        result = type_systems.infer_spark_type("float64")

        # Assert
        assert result == "DOUBLE"

    def test_infer_spark_type_double_type_3(self):
        # Act
        result = type_systems.infer_spark_type("double")
        result1 = type_systems.infer_spark_type("DoubleType()")

        # Assert
        assert result == "DOUBLE"
        assert result1 == "DOUBLE"

    def test_infer_spark_type_timestamp_type_1(self):
        # Act
        result = type_systems.infer_spark_type(datetime.datetime)

        # Assert
        assert result == "TIMESTAMP"

    def test_infer_spark_type_timestamp_type_2(self):
        # Act
        result = type_systems.infer_spark_type(np.datetime64)
        result1 = type_systems.infer_spark_type("TimestampType()")

        # Assert
        assert result == "TIMESTAMP"
        assert result1 == "TIMESTAMP"

    def test_infer_spark_type_date_type_1(self):
        # Act
        result = type_systems.infer_spark_type(datetime.date)
        result1 = type_systems.infer_spark_type("DateType()")

        # Assert
        assert result == "DATE"
        assert result1 == "DATE"

    def test_infer_spark_type_bool_type_1(self):
        # Act
        result = type_systems.infer_spark_type(bool)

        # Assert
        assert result == "BOOLEAN"

    def test_infer_spark_type_bool_type_2(self):
        # Act
        result = type_systems.infer_spark_type("boolean")

        # Assert
        assert result == "BOOLEAN"

    def test_infer_spark_type_bool_type_3(self):
        # Act
        result = type_systems.infer_spark_type("bool")
        result1 = type_systems.infer_spark_type("BooleanType()")

        # Assert
        assert result == "BOOLEAN"
        assert result1 == "BOOLEAN"

    def test_infer_spark_type_wrong_type(self):
        # Act
        with pytest.raises(TypeError) as e_info:
            type_systems.infer_spark_type("wrong")

        # Assert
        assert str(e_info.value) == "Not supported type wrong."
