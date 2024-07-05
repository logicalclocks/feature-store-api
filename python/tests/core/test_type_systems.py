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
import pytest
from hsfs.core import type_systems
from hsfs.core.constants import HAS_ARROW, HAS_PANDAS, HAS_POLARS


if HAS_ARROW:
    import pyarrow as pa

if HAS_PANDAS:
    import numpy as np
    import pandas as pd

if HAS_POLARS:
    pass


class TestTypeSystems:
    @pytest.mark.skipif(
        not HAS_ARROW or not HAS_PANDAS, reason="Arrow or Pandas are not installed"
    )
    def test_infer_type_pyarrow_list(self):
        # Arrange

        # Act
        result = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_type=pa.list_(pa.int8())
        )

        # Assert
        assert result == "array<int>"

    def test_infer_type_pyarrow_large_list(self):
        # Arrange

        # Act
        result = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_type=pa.large_list(pa.int8())
        )

        # Assert
        assert result == "array<int>"

    def test_infer_type_pyarrow_struct(self):
        # Arrange

        # Act
        result = type_systems.convert_pandas_object_type_to_offline_type(
            arrow_type=pa.struct([pa.field("f1", pa.int32())])
        )

        # Assert
        assert result == "struct<f1:int>"

    def test_infer_type_pyarrow_date32(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.date32()
        )

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_date64(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.date64()
        )

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_binary(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.binary()
        )

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_large_binary(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.large_binary()
        )

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_string(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.string()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_large_string(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.large_string()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_utf8(self):
        # Arrange

        # Act
        result = type_systems.convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.utf8()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_other(self):
        # Arrange

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
        mapping = {"user0": list(np.random.normal(size=5)), "user1": ["test", "test"]}
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
            f"user{i}": {"value": list(np.random.normal(size=5))} for i in range(2)
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
        mapping = [{"value": np.random.normal(size=5)}]
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
        mapping = {f"user{i}": [{"value": np.random.normal(size=5)}] for i in range(2)}
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
