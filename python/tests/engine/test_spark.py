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
from __future__ import annotations

import numpy
import pandas as pd
import pytest
from hsfs import (
    engine,
    expectation_suite,
    feature,
    feature_group,
    feature_view,
    storage_connector,
    training_dataset,
    training_dataset_feature,
    transformation_function,
    util,
)
from hsfs.client import exceptions
from hsfs.constructor import hudi_feature_group_alias, query
from hsfs.core import training_dataset_engine
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS
from hsfs.engine import spark
from hsfs.hopsworks_udf import udf
from hsfs.training_dataset_feature import TrainingDatasetFeature
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


engine._engine_type = "spark"


class TestSpark:
    def test_sql(self, mocker):
        # Arrange
        mock_spark_engine_sql_offline = mocker.patch(
            "hsfs.engine.spark.Engine._sql_offline"
        )
        mocker.patch("hsfs.engine.spark.Engine.set_job_group")
        mock_spark_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.spark.Engine._return_dataframe_type"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.sql(
            sql_query=None,
            feature_store=None,
            connector=None,
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_spark_engine_sql_offline.call_count == 1
        assert mock_spark_engine_return_dataframe_type.call_count == 1

    def test_sql_connector(self, mocker):
        # Arrange
        mock_spark_engine_sql_offline = mocker.patch(
            "hsfs.engine.spark.Engine._sql_offline"
        )
        mocker.patch("hsfs.engine.spark.Engine.set_job_group")
        mock_spark_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.spark.Engine._return_dataframe_type"
        )

        spark_engine = spark.Engine()

        connector = mocker.Mock()

        # Act
        spark_engine.sql(
            sql_query=None,
            feature_store=None,
            connector=connector,
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_spark_engine_sql_offline.call_count == 0
        assert connector.read.call_count == 1
        assert mock_spark_engine_return_dataframe_type.call_count == 1

    def test_sql_offline(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._sql_offline(
            sql_query=None,
            feature_store=None,
        )

        # Assert
        assert mock_pyspark_getOrCreate.return_value.sql.call_count == 2

    def test_show(self, mocker):
        # Arrange
        mock_spark_engine_sql = mocker.patch("hsfs.engine.spark.Engine.sql")

        spark_engine = spark.Engine()

        # Act
        spark_engine.show(
            sql_query=None,
            feature_store=None,
            n=None,
            online_conn=None,
        )

        # Assert
        assert mock_spark_engine_sql.call_count == 1

    def test_set_job_group(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.set_job_group(
            group_id=None,
            description=None,
        )

        # Assert
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext.setJobGroup.call_count
            == 1
        )

    def test_register_external_temporary_table(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_sc_read = mocker.patch("hsfs.storage_connector.JdbcConnector.read")
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )

        external_fg = feature_group.ExternalFeatureGroup(
            storage_connector=jdbc_connector, id=10
        )

        # Act
        spark_engine.register_external_temporary_table(
            external_fg=external_fg,
            alias=None,
        )

        # Assert
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext.textFile.call_count == 0
        )
        assert mock_sc_read.return_value.createOrReplaceTempView.call_count == 1

    def test_register_external_temporary_table_external_fg_location(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_sc_read = mocker.patch("hsfs.storage_connector.JdbcConnector.read")
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )

        external_fg = feature_group.ExternalFeatureGroup(
            storage_connector=jdbc_connector, id=10, location="test_location"
        )

        # Act
        spark_engine.register_external_temporary_table(
            external_fg=external_fg,
            alias=None,
        )

        # Assert
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext.textFile.call_count == 1
        )
        assert mock_sc_read.return_value.createOrReplaceTempView.call_count == 1

    def test_register_hudi_temporary_table(self, mocker):
        # Arrange
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")
        mocker.patch("hsfs.feature_group.FeatureGroup.from_response_json")

        spark_engine = spark.Engine()

        hudi_fg_alias = hudi_feature_group_alias.HudiFeatureGroupAlias(
            feature_group=None, alias=None
        )

        # Act
        spark_engine.register_hudi_temporary_table(
            hudi_fg_alias=hudi_fg_alias,
            feature_store_id=None,
            feature_store_name=None,
            read_options=None,
        )

        # Assert
        assert mock_hudi_engine.return_value.register_temporary_table.call_count == 1

    def test_return_dataframe_type_default(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock()

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df

    def test_return_dataframe_type_spark(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock()

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="spark",
        )

        # Assert
        assert result == mock_df

    def test_return_dataframe_type_pandas(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock(spec=DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="pandas",
        )
        # Assert
        assert result == mock_df.toPandas()

        mock_df = mocker.Mock(spec=pd.DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="pandas",
        )
        # Assert
        assert result == mock_df

    def test_return_dataframe_type_numpy(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock(spec=DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="numpy",
        )

        # Assert
        assert result == mock_df.toPandas().values

        mock_df = mocker.Mock(spec=pd.DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="numpy",
        )
        # Assert
        assert result == mock_df.values

    def test_return_dataframe_type_python(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock(spec=DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="python",
        )

        # Assert
        assert result == mock_df.toPandas().values.tolist()

        mock_df = mocker.Mock(spec=pd.DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="python",
        )
        # Assert
        assert result == mock_df.values.tolist()

    def test_return_dataframe_type_other(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock()

        # Act
        with pytest.raises(TypeError) as e_info:
            spark_engine._return_dataframe_type(
                dataframe=mock_df,
                dataframe_type="other",
            )

        # Assert
        assert (
            str(e_info.value)
            == "Dataframe type `other` not supported on this platform."
        )

    def test_convert_to_default_dataframe_list_1dimension(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            spark_engine.convert_to_default_dataframe(
                dataframe=list(),
            )

        # Assert
        assert (
            str(e_info.value)
            == "Cannot convert numpy array that do not have two dimensions to a dataframe. The number of dimensions are: 1"
        )

    def test_convert_to_default_dataframe_list(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=[[1, "test_1"], [2, "test_2"]],
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_numpy_array(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=numpy.array([[1, "test_1"], [2, "test_2"]]),
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pandas_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=expected,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pyspark_rdd(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"_1": [1, 2], "_2": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        rdd = spark_engine._spark_session.sparkContext.parallelize(
            [[1, "test_1"], [2, "test_2"]]
        )

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=rdd,
        )

        # Assert
        result_df = result.toPandas()
        print(result_df)
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pyspark_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        df = spark_engine._spark_session.createDataFrame(expected)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=df,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pyspark_dataframe_capitalized_columns(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "Col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        df = spark_engine._spark_session.createDataFrame(expected)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=df,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) != list(expected)
        for column in list(result_df):
            assert result_df[util.autofix_feature_name(column)].equals(
                result_df[column]
            )

    def test_convert_to_default_dataframe_wrong_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            spark_engine.convert_to_default_dataframe(
                dataframe=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: <class 'NoneType'>"
        )

    def test_convert_to_default_dataframe_nullable(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [None, "test_2"]}
        data = pd.DataFrame(data=d)

        schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=False),
                StructField("col_1", StringType(), nullable=False),
                StructField("col_2", StringType(), nullable=True),
            ]
        )
        original_df = spark_engine._spark_session.createDataFrame(data, schema=schema)

        # Act
        result_df = spark_engine.convert_to_default_dataframe(dataframe=original_df)

        # Assert
        original_schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=False),
                StructField("col_1", StringType(), nullable=False),
                StructField("col_2", StringType(), nullable=True),
            ]
        )
        result_schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=True),
                StructField("col_1", StringType(), nullable=True),
                StructField("col_2", StringType(), nullable=True),
            ]
        )

        assert original_schema == original_df.schema
        assert result_schema == result_df.schema

    def test_convert_to_default_dataframe_nullable_uppercase_and_spaced(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"COL_0": [1, 2], "COL_1": ["test_1", "test_2"], "COL 2": [None, "test_2"]}
        data = pd.DataFrame(data=d)

        schema = StructType(
            [
                StructField("COL_0", IntegerType(), nullable=False),
                StructField("COL_1", StringType(), nullable=False),
                StructField("COL 2", StringType(), nullable=True),
            ]
        )
        original_df = spark_engine._spark_session.createDataFrame(data, schema=schema)

        # Act
        result_df = spark_engine.convert_to_default_dataframe(dataframe=original_df)

        # Assert
        original_schema = StructType(
            [
                StructField("COL_0", IntegerType(), nullable=False),
                StructField("COL_1", StringType(), nullable=False),
                StructField("COL 2", StringType(), nullable=True),
            ]
        )
        result_schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=True),
                StructField("col_1", StringType(), nullable=True),
                StructField("col_2", StringType(), nullable=True),
            ]
        )

        assert original_schema == original_df.schema
        assert result_schema == result_df.schema

    def test_save_dataframe(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_offline(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage="offline",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_offline_online_enabled(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=True,
            storage="offline",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_online(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage="online",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_online_online_enabled(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=True,
            storage="online",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 1
        assert mock_spark_engine_save_offline_dataframe.call_count == 0

    def test_save_dataframe_online_enabled(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=True,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 1
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_fg_stream(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=True,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 1
        assert mock_spark_engine_save_offline_dataframe.call_count == 0

    def test_save_stream_dataframe(self, mocker, backend_fixtures):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine._encode_complex_features")
        mock_spark_engine_online_fg_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._online_fg_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()
        project_id = 1
        fg.feature_store.project_id = project_id

        mock_client_get_instance.return_value._project_name = "test_project_name"

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode="test_mode",
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == f"/Projects/test_project_name/Resources/{self._get_spark_query_name(project_id, fg)}-checkpoint"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == self._get_spark_query_name(project_id, fg)
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 0
        )

    def test_save_stream_dataframe_query_name(self, mocker, backend_fixtures):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine._encode_complex_features")
        mock_spark_engine_online_fg_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._online_fg_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()

        mock_client_get_instance.return_value._project_name = "test_project_name"

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=None,
            query_name="test_query_name",
            output_mode="test_mode",
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == "/Projects/test_project_name/Resources/test_query_name-checkpoint"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == "test_query_name"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 0
        )

    def _get_spark_query_name(self, project_id, feature_group):
        return (
            f"insert_stream_{project_id}_{feature_group.id}"
            f"_{feature_group.name}_{feature_group.version}_onlinefs"
        )

    def test_save_stream_dataframe_checkpoint_dir(self, mocker, backend_fixtures):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine._encode_complex_features")
        mock_spark_engine_online_fg_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._online_fg_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()
        project_id = 1
        fg.feature_store.project_id = project_id
        mock_client_get_instance.return_value._project_name = "test_project_name"

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode="test_mode",
            await_termination=None,
            timeout=None,
            checkpoint_dir="test_checkpoint_dir",
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == "test_checkpoint_dir"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == self._get_spark_query_name(project_id, fg)
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 0
        )

    def test_save_stream_dataframe_await_termination(self, mocker, backend_fixtures):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine._encode_complex_features")
        mock_spark_engine_online_fg_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._online_fg_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()
        project_id = 1
        fg.feature_store.project_id = project_id
        mock_client_get_instance.return_value._project_name = "test_project_name"

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode="test_mode",
            await_termination=True,
            timeout=123,
            checkpoint_dir=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == f"/Projects/test_project_name/Resources/{self._get_spark_query_name(project_id, fg)}-checkpoint"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == self._get_spark_query_name(project_id, fg)
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 1
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_args[
                0
            ][0]
            == 123
        )

    def test_save_offline_dataframe(self, mocker):
        # Arrange
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._get_table_name",
            return_value="test_get_table_name",
        )
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], id=10
        )
        fg.feature_store = mocker.Mock()

        mock_df = mocker.Mock()

        # Act
        spark_engine._save_offline_dataframe(
            feature_group=fg,
            dataframe=mock_df,
            operation=None,
            write_options={"test_name": "test_value"},
            validation_id=None,
        )

        # Assert
        assert mock_df.write.format.call_count == 1
        assert mock_hudi_engine.return_value.save_hudi_fg.call_count == 0
        assert mock_df.write.format.call_args[0][0] == "hive"
        assert mock_df.write.format.return_value.mode.call_args[0][0] == "append"
        assert mock_df.write.format.return_value.mode.return_value.options.call_args[
            1
        ] == {"test_name": "test_value"}
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.call_args[
                0
            ][0]
            == []
        )
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.return_value.saveAsTable.call_args[
                0
            ][0]
            == "test_get_table_name"
        )

    def test_save_offline_dataframe_partition_by(self, mocker):
        # Arrange
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._get_table_name",
            return_value="test_get_table_name",
        )
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        spark_engine = spark.Engine()

        f = feature.Feature(name="f", type="str", partition=True)
        f1 = feature.Feature(name="f1", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=[f, f1],
        )

        mock_df = mocker.Mock()

        # Act
        spark_engine._save_offline_dataframe(
            feature_group=fg,
            dataframe=mock_df,
            operation=None,
            write_options={"test_name": "test_value"},
            validation_id=None,
        )

        # Assert
        assert mock_df.write.format.call_count == 1
        assert mock_hudi_engine.return_value.save_hudi_fg.call_count == 0
        assert mock_df.write.format.call_args[0][0] == "hive"
        assert mock_df.write.format.return_value.mode.call_args[0][0] == "append"
        assert mock_df.write.format.return_value.mode.return_value.options.call_args[
            1
        ] == {"test_name": "test_value"}
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.call_args[
                0
            ][0]
            == ["f"]
        )
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.return_value.saveAsTable.call_args[
                0
            ][0]
            == "test_get_table_name"
        )

    def test_save_offline_dataframe_hudi_time_travel_format(self, mocker):
        # Arrange
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            time_travel_format="HUDI",
        )

        mock_df = mocker.Mock()

        # Act
        spark_engine._save_offline_dataframe(
            feature_group=fg,
            dataframe=mock_df,
            operation=None,
            write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_df.write.format.call_count == 0
        assert mock_hudi_engine.return_value.save_hudi_fg.call_count == 1

    def test_save_online_dataframe(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine._encode_complex_features")
        mock_spark_engine_online_fg_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._online_fg_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()

        # Act
        spark_engine._save_online_dataframe(
            feature_group=fg,
            dataframe=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert mock_spark_engine_online_fg_to_avro.call_count == 1
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.write.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.write.format.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.write.format.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.write.format.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_online_fg_to_avro.return_value.withColumn.return_value.write.format.return_value.options.return_value.option.return_value.save.call_count
            == 1
        )

    def test_encode_complex_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_complex_features",
            return_value=["col_1"],
        )
        mocker.patch("hsfs.feature_group.FeatureGroup._get_feature_avro_schema")

        spark_engine = spark.Engine()

        d = {"col_0": ["test_1", "test_2"], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )
        fg._subject = {"schema": '{"fields": [{"name": "col_0"}]}'}

        expected = pd.DataFrame(data={"col_0": ["test_1", "test_2"]})

        # Act
        result = spark_engine._encode_complex_features(
            feature_group=fg,
            dataframe=spark_df,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(expected[column])

    def test_encode_complex_features_col_in_complex_features(self, mocker):
        # Arrange
        mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_complex_features",
            return_value=["col_0"],
        )
        mocker.patch("hsfs.feature_group.FeatureGroup._get_feature_avro_schema")

        spark_engine = spark.Engine()

        d = {"col_0": ["test_1", "test_2"], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )
        fg._subject = {"schema": '{"fields": [{"name": "col_0"}]}'}

        # Act
        with pytest.raises(
            TypeError
        ) as e_info:  # todo look into this (to_avro has to be mocked)
            spark_engine._encode_complex_features(
                feature_group=fg,
                dataframe=spark_df,
            )

        # Assert
        assert str(e_info.value) == "'JavaPackage' object is not callable"

    def test_online_fg_to_avro(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": ["test_1", "test_2"], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )
        fg._avro_schema = '{"fields": [{"name": "col_0"}]}'

        # Act
        with pytest.raises(
            TypeError
        ) as e_info:  # todo look into this (to_avro has to be mocked)
            spark_engine._online_fg_to_avro(
                feature_group=fg,
                dataframe=spark_df,
            )

        # Assert
        assert str(e_info.value) == "'JavaPackage' object is not callable"

    def test_get_training_data(self, mocker):
        # Arrange
        mock_spark_engine_write_training_dataset = mocker.patch(
            "hsfs.engine.spark.Engine.write_training_dataset"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.get_training_data(
            training_dataset=None,
            feature_view_obj=None,
            query_obj=None,
            read_options=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_spark_engine_write_training_dataset.call_count == 1

    def test_split_labels(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": ["test_1", "test_2"], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        # Act
        result = spark_engine.split_labels(df=df, labels=None, dataframe_type="default")

        # Assert
        assert result == (df, None)

    def test_split_labels_labels(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df_new = pd.DataFrame(data={"col_1": ["test_1", "test_2"]})
        expected_labels_df = pd.DataFrame(data={"col_0": [1, 2]})

        # Act
        df_new, labels_df = spark_engine.split_labels(
            df=spark_df, labels=["col_0"], dataframe_type="default"
        )

        # Assert
        result_df_new = df_new.toPandas()
        result_labels_df = labels_df.toPandas()
        assert result_labels_df.equals(expected_labels_df)
        assert result_df_new.equals(expected_df_new)

    def test_write_training_dataset(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            spark_engine.write_training_dataset(
                training_dataset=td,
                query_obj=None,
                user_write_options=None,
                save_mode=None,
                read_options=None,
                feature_view_obj=None,
                to_df=None,
            )

        # Assert
        assert str(e_info.value) == "Dataset should be a query."
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 0
        assert mock_spark_engine_write_training_dataset_splits.call_count == 0

    def test_write_training_dataset_to_df(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        jsonq = backend_fixtures["query"]["get"]["response"]
        q = query.Query.from_response_json(jsonq)

        mock_query_read = mocker.patch("hsfs.constructor.query.Query.read")
        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "col_2": [3, 4],
            "event_time": [1, 2],
        }
        df = pd.DataFrame(data=d)
        query_df = spark_engine._spark_session.createDataFrame(df)
        mock_query_read.side_effect = [query_df]

        td = training_dataset.TrainingDataset(
            name="test",
            version=None,
            splits={},
            event_start_time=None,
            event_end_time=None,
            description="test",
            storage_connector=None,
            featurestore_id=10,
            data_format="tsv",
            location="",
            statistics_config=None,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=None,
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        # Act
        df_returned = spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options={},
            save_mode=training_dataset_engine.TrainingDatasetEngine.OVERWRITE,
            read_options={},
            feature_view_obj=fv,
            to_df=True,
        )

        # Assert
        assert set(df_returned.columns) == {"col_0", "col_1", "col_2", "event_time"}
        assert df_returned.count() == 2
        assert df_returned.exceptAll(query_df).rdd.isEmpty()

    def test_write_training_dataset_split_to_df(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        jsonq = backend_fixtures["query"]["get"]["response"]
        q = query.Query.from_response_json(jsonq)

        mock_query_read = mocker.patch("hsfs.constructor.query.Query.read")
        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "col_2": [3, 4],
            "event_time": [1, 2],
        }
        df = pd.DataFrame(data=d)
        query_df = spark_engine._spark_session.createDataFrame(df)
        mock_query_read.side_effect = [query_df]

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=None,
            splits={},
            test_size=0.5,
            train_start=None,
            train_end=None,
            test_start=None,
            test_end=None,
            time_split_size=2,
            description="test",
            storage_connector=None,
            featurestore_id=12,
            data_format="tsv",
            location="",
            statistics_config=None,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=None,
            seed=1,
        )

        # Act
        split_dfs_returned = spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options={},
            save_mode=training_dataset_engine.TrainingDatasetEngine.OVERWRITE,
            read_options={},
            feature_view_obj=fv,
            to_df=True,
        )

        # Assert
        sum_rows = 0
        for key in split_dfs_returned:
            df_returned = split_dfs_returned[key]
            assert set(df_returned.columns) == {"col_0", "col_1", "col_2", "event_time"}
            sum_rows += df_returned.count()

        assert sum_rows == 2

    def test_write_training_dataset_query(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        q = query.Query(left_feature_group=None, left_features=None)

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 1
        assert mock_spark_engine_write_training_dataset_splits.call_count == 0

    def test_write_training_dataset_query_coalesce(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            coalesce=True,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 1
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 1
        assert mock_spark_engine_write_training_dataset_splits.call_count == 0

    def test_write_training_dataset_td_splits(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mock_spark_engine_split_df = mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
        )

        q = query.Query(left_feature_group=None, left_features=None)

        m = mocker.Mock()

        mock_spark_engine_split_df.return_value = {"temp": m}

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 0
        assert m.coalesce.call_count == 0
        assert mock_spark_engine_write_training_dataset_splits.call_count == 1

    def test_write_training_dataset_td_splits_coalesce(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mock_spark_engine_split_df = mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
            coalesce=True,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        m = mocker.Mock()

        mock_spark_engine_split_df.return_value = {"temp": m}

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 0
        assert m.coalesce.call_count == 1
        assert mock_spark_engine_write_training_dataset_splits.call_count == 1

    def test_split_df(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
        )

        q = query.Query(left_feature_group=fg, left_features=None)

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 0
        assert mock_spark_engine_random_split.call_count == 1

    def test_split_df_time_split_td_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
            features=[f, f1, f2],
        )

        q = query.Query(left_feature_group=fg, left_features=None)

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 1
        assert mock_spark_engine_random_split.call_count == 0

    def test_split_df_time_split_query_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
        )

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 1
        assert mock_spark_engine_random_split.call_count == 0

    def test_random_split(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            seed=1,
        )

        d = {
            "col_0": [1, 2, 3, 4, 5, 6],
            "col_1": ["test_1", "test_2", "test_3", "test_4", "test_5", "test_6"],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine._random_split(
            dataset=spark_df,
            training_dataset=td,
        )

        # Assert
        assert list(result) == ["test_split1", "test_split2"]
        sum_rows = 0
        for column in list(result):
            assert result[column].schema == spark_df.schema
            sum_rows += result[column].count()
        assert sum_rows == 6

    def test_time_series_split(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": [1000000000, 2000000000],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_date(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=1488600000,
            test_end=1488718800,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": ["2017-03-04", "2017-03-05"],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        spark_df = spark_df.withColumn(
            "event_time", spark_df["event_time"].cast(DateType())
        )

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )
        train_spark_df = train_spark_df.withColumn(
            "event_time", train_spark_df["event_time"].cast(DateType())
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )
        test_spark_df = test_spark_df.withColumn(
            "event_time", test_spark_df["event_time"].cast(DateType())
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_timestamp(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=1488600000,
            test_end=1488718800,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": ["2017-03-04", "2017-03-05"],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        spark_df = spark_df.withColumn(
            "event_time", spark_df["event_time"].cast(TimestampType())
        )

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )
        train_spark_df = train_spark_df.withColumn(
            "event_time", train_spark_df["event_time"].cast(TimestampType())
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )
        test_spark_df = test_spark_df.withColumn(
            "event_time", test_spark_df["event_time"].cast(TimestampType())
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_epoch_sec(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=1488600001,
            test_end=1488718801,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": [1488600000, 1488718800],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_drop_event_time(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": [1000000000, 2000000000],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )

        expected = {"train": train_spark_df, "test": test_spark_df}
        expected["train"] = expected["train"].drop("event_time")
        expected["test"] = expected["test"].drop("event_time")

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=True,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_write_training_dataset_splits(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )

        spark_engine = spark.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=plus_one,
        )

        f = training_dataset_feature.TrainingDatasetFeature(
            name="col_0", type=IntegerType(), index=0
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="col_1", type=StringType(), index=1
        )
        features = [f, f1]

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            features=features,
        )

        # Act
        result = spark_engine._write_training_dataset_splits(
            training_dataset=td,
            feature_dataframes={"col_0": None, "col_1": None},
            write_options=None,
            save_mode=None,
            to_df=False,
            transformation_functions=[tf("col_0")],
        )

        # Assert
        assert result is None
        assert mock_spark_engine_write_training_dataset_single.call_count == 2

    def test_write_training_dataset_splits_to_df(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )

        spark_engine = spark.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=plus_one,
        )

        transformation_fn_dict = dict()

        transformation_fn_dict["col_0"] = tf

        f = training_dataset_feature.TrainingDatasetFeature(
            name="col_0", type=IntegerType(), index=0
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="col_1", type=StringType(), index=1
        )
        features = [f, f1]

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            transformation_functions=transformation_fn_dict,
            features=features,
        )

        # Act
        result = spark_engine._write_training_dataset_splits(
            training_dataset=td,
            feature_dataframes={"col_0": None, "col_1": None},
            write_options=None,
            save_mode=None,
            to_df=True,
            transformation_functions=[tf("col_0")],
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_write_training_dataset_single.call_count == 2

    def test_write_training_dataset_single(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_spark_engine_apply_transformation_function = mocker.patch(
            "hsfs.engine.spark.Engine._apply_transformation_function"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._write_training_dataset_single(
            transformation_functions=None,
            feature_dataframe=None,
            storage_connector=None,
            data_format="csv",
            write_options={},
            save_mode=None,
            path=None,
            to_df=False,
        )

        # Assert
        assert mock_spark_engine_apply_transformation_function.call_count == 1
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_apply_transformation_function.return_value.write.format.call_args[
                0
            ][0]
            == "csv"
        )

    def test_write_training_dataset_single_tsv(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_spark_engine_apply_transformation_function = mocker.patch(
            "hsfs.engine.spark.Engine._apply_transformation_function"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._write_training_dataset_single(
            transformation_functions=None,
            feature_dataframe=None,
            storage_connector=None,
            data_format="tsv",
            write_options={},
            save_mode=None,
            path=None,
            to_df=False,
        )

        # Assert
        assert mock_spark_engine_apply_transformation_function.call_count == 1
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_apply_transformation_function.return_value.write.format.call_args[
                0
            ][0]
            == "csv"
        )

    def test_write_training_dataset_single_to_df(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_spark_engine_apply_transformation_function = mocker.patch(
            "hsfs.engine.spark.Engine._apply_transformation_function"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._write_training_dataset_single(
            transformation_functions=None,
            feature_dataframe=None,
            storage_connector=None,
            data_format=None,
            write_options={},
            save_mode=None,
            path=None,
            to_df=True,
        )

        # Assert
        assert mock_spark_engine_apply_transformation_function.call_count == 1
        assert mock_spark_engine_setup_storage_connector.call_count == 0

    def test_read_none_data_format(self, mocker):
        # Arrange
        mocker.patch("pyspark.sql.session.SparkSession.builder.getOrCreate")

        spark_engine = spark.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            spark_engine.read(
                storage_connector=None,
                data_format=None,
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_empty_data_format(self, mocker):
        # Arrange
        mocker.patch("pyspark.sql.session.SparkSession.builder.getOrCreate")

        spark_engine = spark.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            spark_engine.read(
                storage_connector=None,
                data_format="",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_read_options(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="csv",
            read_options={"name": "value"},
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert mock_spark_engine_setup_storage_connector.call_args[0][1] is None
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0] == "csv"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.return_value.options.call_args[
                1
            ]
            == {"name": "value"}
        )

    def test_read_location_format_delta(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="delta",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1] == "test_location"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0] == "delta"
        )

    def test_read_location_format_parquet(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="parquet",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1] == "test_location"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0]
            == "parquet"
        )

    def test_read_location_format_hudi(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="hudi",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1] == "test_location"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0] == "hudi"
        )

    def test_read_location_format_orc(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="orc",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1] == "test_location"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0] == "orc"
        )

    def test_read_location_format_bigquery(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="bigquery",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1] == "test_location"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0]
            == "bigquery"
        )

    def test_read_location_format_csv(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="csv",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1]
            == "test_location/**"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0] == "csv"
        )

    def test_read_location_format_tsv(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="csv",
            read_options=None,
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_spark_engine_setup_storage_connector.call_args[0][1]
            == "test_location/**"
        )
        assert (
            mock_pyspark_getOrCreate.return_value.read.format.call_args[0][0] == "csv"
        )

    def test_read_stream(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.client.get_instance")
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_spark_engine_read_stream_kafka = mocker.patch(
            "hsfs.engine.spark.Engine._read_stream_kafka"
        )

        spark_engine = spark.Engine()

        kafka_connector = storage_connector.KafkaConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        mock_pyspark_getOrCreate.return_value.read.format.return_value.options.return_value = {}

        # Act
        result = spark_engine.read_stream(
            storage_connector=kafka_connector,
            message_format=None,
            schema=None,
            options={},
            include_metadata=None,
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_read_stream_kafka.call_count == 1
        assert (
            mock_pyspark_getOrCreate.return_value.readStream.format.call_args[0][0]
            == "kafka"
        )

    def test_read_stream_kafka(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_spark_df = spark_df.select("key", "value")

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format=None,
            schema=None,
            include_metadata=None,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_include_metadata(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_spark_df = spark_df

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format=None,
            schema=None,
            include_metadata=True,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_json(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_df = pd.DataFrame(data={"name": ["value1", "value2"]})

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format="json",
            schema=StructType([StructField("name", StringType())]),
            include_metadata=None,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_json_include_metadata(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()
        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_df = pd.DataFrame(
            data={
                "key": [1, 2],
                "topic": ["test_topic", "test_topic"],
                "partition": ["test_partition", "test_partition"],
                "offset": ["test_offset", "test_offset"],
                "timestamp": ["test_timestamp", "test_timestamp"],
                "timestampType": ["test_timestampType", "test_timestampType"],
                "name": ["value1", "value2"],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format="json",
            schema=StructType([StructField("name", StringType())]),
            include_metadata=True,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_avro(self, mocker):
        # Arrange
        mocker.patch("pyspark.context.SparkContext")

        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        schema_string = """{
                                "namespace": "example.avro",
                                "type": "record",
                                "name": "KeyValue",
                                "fields": [
                                    {"name": "name", "type": "string"}
                                ]
                            }"""

        # Act
        with pytest.raises(
            TypeError
        ) as e_info:  # todo look into this (from_avro has to be mocked)
            spark_engine._read_stream_kafka(
                stream=mock_stream,
                message_format="avro",
                schema=schema_string,
                include_metadata=True,
            )

        # Assert
        assert str(e_info.value) == "'JavaPackage' object is not callable"
        # assert result.schema == expected_spark_df.schema
        # assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_avro_include_metadata(self, mocker):
        # Arrange
        mocker.patch("pyspark.context.SparkContext")

        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()
        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        schema_string = """{
                                "namespace": "example.avro",
                                "type": "record",
                                "name": "KeyValue",
                                "fields": [
                                    {"name": "name", "type": "string"}
                                ]
                            }"""

        # Act
        with pytest.raises(
            TypeError
        ) as e_info:  # todo look into this (from_avro has to be mocked)
            spark_engine._read_stream_kafka(
                stream=mock_stream,
                message_format="avro",
                schema=schema_string,
                include_metadata=True,
            )

        # Assert
        assert str(e_info.value) == "'JavaPackage' object is not callable"
        # assert result.schema == expected_spark_df.schema
        # assert result.collect() == expected_spark_df.collect()

    def test_add_file(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )
        mock_pyspark_files_get = mocker.patch("pyspark.files.SparkFiles.get")
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("shutil.copy")

        spark_engine = spark.Engine()

        # Act
        spark_engine.add_file(
            file="test_file",
        )

        # Assert
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext.addFile.call_count == 1
        )
        assert mock_pyspark_files_get.call_count == 1
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext.addFile.call_args[0][0]
            == "hdfs://test_file"
        )
        assert mock_pyspark_files_get.call_args[0][0] == "test_file"

    def test_profile(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.profile(
            dataframe=mocker.Mock(),
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jvm.com.logicalclocks.hsfs.spark.engine.SparkEngine.getInstance.return_value.profile.call_count
            == 1
        )

    @pytest.mark.skipif(
        HAS_GREAT_EXPECTATIONS is False,
        reason="Great Expectations is not installed",
    )
    def test_validate_with_great_expectations(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "event_time": [1, 2]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="es_name", expectations=None, meta={}
        )

        # Act
        result = spark_engine.validate_with_great_expectations(
            dataframe=spark_df,
            expectation_suite=es.to_ge_type(),
            ge_validate_kwargs={"run_name": "test_run_id"},
        )

        # Assert
        assert result.to_json_dict() == {
            "evaluation_parameters": {},
            "meta": {
                "active_batch_definition": {
                    "batch_identifiers": {"batch_id": "default_identifier"},
                    "data_asset_name": "<YOUR_MEANGINGFUL_NAME>",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "datasource_name": "my_spark_dataframe",
                },
                "batch_markers": {"ge_load_time": mocker.ANY},
                "batch_spec": {
                    "batch_data": "SparkDataFrame",
                    "data_asset_name": "<YOUR_MEANGINGFUL_NAME>",
                },
                "checkpoint_name": None,
                "expectation_suite_name": "es_name",
                "great_expectations_version": "0.18.12",
                "run_id": {"run_name": "test_run_id", "run_time": mocker.ANY},
                "validation_time": mocker.ANY,
            },
            "results": [],
            "statistics": {
                "evaluated_expectations": 0,
                "success_percent": None,
                "successful_expectations": 0,
                "unsuccessful_expectations": 0,
            },
            "success": True,
        }

    def test_write_options(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"test_key": "test_value"}

    def test_write_options_tfrecords(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="tfrecords",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_write_options_tfrecord(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="tfrecord",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_write_options_csv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="csv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"delimiter": ",", "header": "true", "test_key": "test_value"}

    def test_write_options_tsv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="tsv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"delimiter": "\t", "header": "true", "test_key": "test_value"}

    def test_read_options(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="",
            provided_options=None,
        )

        # Assert
        assert result == {}

    def test_read_options_provided_options(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"test_key": "test_value"}

    def test_read_options_provided_options_tfrecords(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="tfrecords",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_read_options_provided_options_tfrecord(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="tfrecord",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_read_options_provided_options_csv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="csv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {
            "delimiter": ",",
            "header": "true",
            "inferSchema": "true",
            "test_key": "test_value",
        }

    def test_read_options_provided_options_tsv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="tsv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {
            "delimiter": "\t",
            "header": "true",
            "inferSchema": "true",
            "test_key": "test_value",
        }

    def test_parse_schema_feature_group(self, mocker):
        # Arrange
        mock_spark_engine_convert_spark_type = mocker.patch(
            "hsfs.engine.spark.Engine.convert_spark_type_to_offline_type"
        )

        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.parse_schema_feature_group(
            dataframe=spark_df,
            time_travel_format=None,
        )

        # Assert
        assert result[0].name == "col_0"
        assert result[1].name == "col_1"
        assert mock_spark_engine_convert_spark_type.call_count == 2
        assert mock_spark_engine_convert_spark_type.call_args[0][1] is False

    def test_parse_schema_feature_group_hudi(self, mocker):
        # Arrange
        mock_spark_engine_convert_spark_type = mocker.patch(
            "hsfs.engine.spark.Engine.convert_spark_type_to_offline_type"
        )

        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.parse_schema_feature_group(
            dataframe=spark_df,
            time_travel_format="HUDI",
        )

        # Assert
        assert result[0].name == "col_0"
        assert result[1].name == "col_1"
        assert mock_spark_engine_convert_spark_type.call_count == 2
        assert mock_spark_engine_convert_spark_type.call_args[0][1] is True

    def test_parse_schema_feature_group_value_error(self, mocker):
        # Arrange
        mock_spark_engine_convert_spark_type = mocker.patch(
            "hsfs.engine.spark.Engine.convert_spark_type_to_offline_type"
        )

        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        mock_spark_engine_convert_spark_type.side_effect = ValueError(
            "test error response"
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            spark_engine.parse_schema_feature_group(
                dataframe=spark_df,
                time_travel_format=None,
            )

        # Assert
        assert str(e_info.value) == "Feature 'col_0': test error response"

    def test_parse_schema_training_dataset(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.parse_schema_training_dataset(
            dataframe=spark_df,
        )

        # Assert
        assert result[0].name == "col_0"
        assert result[1].name == "col_1"

    def test_convert_spark_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=IntegerType(),
            using_hudi=False,
        )

        # Assert
        assert result == "int"

    def test_cast_columns(self):
        class LabelIndex:
            def __init__(self, label, index):
                self.label = label
                self.index = index

        spark_engine = spark.Engine()
        d = {
            "string": ["s"],
            "bigint": ["1"],
            "int": ["1"],
            "smallint": ["1"],
            "tinyint": ["1"],
            "float": ["1"],
            "double": ["1"],
            "timestamp": [1641340800000],
            "boolean": ["False"],
            "date": ["2022-01-27"],
            "binary": ["1"],
            "array<string>": [["123"]],
            "struc": [LabelIndex("0", "1")],
            "decimal": ["1.1"],
        }
        df = pd.DataFrame(data=d)
        spark_df = spark_engine._spark_session.createDataFrame(df)
        schema = [
            TrainingDatasetFeature("string", type="string"),
            TrainingDatasetFeature("bigint", type="bigint"),
            TrainingDatasetFeature("int", type="int"),
            TrainingDatasetFeature("smallint", type="smallint"),
            TrainingDatasetFeature("tinyint", type="tinyint"),
            TrainingDatasetFeature("float", type="float"),
            TrainingDatasetFeature("double", type="double"),
            TrainingDatasetFeature("timestamp", type="timestamp"),
            TrainingDatasetFeature("boolean", type="boolean"),
            TrainingDatasetFeature("date", type="date"),
            TrainingDatasetFeature("binary", type="binary"),
            TrainingDatasetFeature("array<string>", type="array<string>"),
            TrainingDatasetFeature("struc", type="struct<label:string,index:int>"),
            TrainingDatasetFeature("decimal", type="decimal"),
        ]
        cast_df = spark_engine.cast_columns(spark_df, schema)
        expected = {
            "string": StringType(),
            "bigint": LongType(),
            "int": IntegerType(),
            "smallint": ShortType(),
            "tinyint": ByteType(),
            "float": FloatType(),
            "double": DoubleType(),
            "timestamp": TimestampType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "binary": BinaryType(),
            "array<string>": ArrayType(StringType()),
            "struc": StructType(
                [
                    StructField("label", StringType(), True),
                    StructField("index", IntegerType(), True),
                ]
            ),
            "decimal": DecimalType(),
        }
        for col in cast_df.dtypes:
            assert col[1] == expected[col[0]].simpleString()

    def test_convert_spark_type_using_hudi_byte_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=ByteType(),
            using_hudi=True,
        )

        # Assert
        assert result == "int"

    def test_convert_spark_type_using_hudi_short_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=ShortType(),
            using_hudi=True,
        )

        # Assert
        assert result == "int"

    def test_convert_spark_type_using_hudi_bool_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=BooleanType(),
            using_hudi=True,
        )

        # Assert
        assert result == "boolean"

    def test_convert_spark_type_using_hudi_int_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=IntegerType(),
            using_hudi=True,
        )

        # Assert
        assert result == "int"

    def test_convert_spark_type_using_hudi_long_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=LongType(),
            using_hudi=True,
        )

        # Assert
        assert result == "bigint"

    def test_convert_spark_type_using_hudi_float_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=FloatType(),
            using_hudi=True,
        )

        # Assert
        assert result == "float"

    def test_convert_spark_type_using_hudi_double_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=DoubleType(),
            using_hudi=True,
        )

        # Assert
        assert result == "double"

    def test_convert_spark_type_using_hudi_decimal_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=DecimalType(),
            using_hudi=True,
        )

        # Assert
        assert result == "decimal(10,0)"

    def test_convert_spark_type_using_hudi_timestamp_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=TimestampType(),
            using_hudi=True,
        )

        # Assert
        assert result == "timestamp"

    def test_convert_spark_type_using_hudi_date_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=DateType(),
            using_hudi=True,
        )

        # Assert
        assert result == "date"

    def test_convert_spark_type_using_hudi_string_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=StringType(),
            using_hudi=True,
        )

        # Assert
        assert result == "string"

    def test_convert_spark_type_using_hudi_struct_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=StructType(),
            using_hudi=True,
        )

        # Assert
        assert result == "struct<>"

    def test_convert_spark_type_using_hudi_binary_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.convert_spark_type_to_offline_type(
            spark_type=BinaryType(),
            using_hudi=True,
        )

        # Assert
        assert result == "binary"

    def test_convert_spark_type_using_hudi_map_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(ValueError) as e_info:
            spark_engine.convert_spark_type_to_offline_type(
                spark_type=MapType(StringType(), StringType()),
                using_hudi=True,
            )

        # Assert
        assert (
            str(e_info.value)
            == "spark type <class 'pyspark.sql.types.MapType'> not supported"
        )

    def test_setup_storage_connector_s3(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.S3Connector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        s3_connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        spark_engine.setup_storage_connector(
            storage_connector=s3_connector,
            path="test_path",
        )

        # Assert
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 1
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 0

    def test_setup_storage_connector_adls(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.AdlsConnector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        adls_connector = storage_connector.AdlsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        spark_engine.setup_storage_connector(
            storage_connector=adls_connector,
            path="test_path",
        )

        # Assert
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 1
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 0

    def test_setup_storage_connector_gcs(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.GcsConnector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        gcs_connector = storage_connector.GcsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        spark_engine.setup_storage_connector(
            storage_connector=gcs_connector,
            path="test_path",
        )

        # Assert
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 1

    def test_setup_storage_connector_jdbc(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.JdbcConnector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        result = spark_engine.setup_storage_connector(
            storage_connector=jdbc_connector,
            path="test_path",
        )

        # Assert
        assert result == "test_path"
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 0

    def test_setup_s3_hadoop_conf(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        s3_connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            access_key="1",
            secret_key="2",
            server_encryption_algorithm="3",
            server_encryption_key="4",
            session_token="5",
            arguments=[{"name": "fs.s3a.endpoint", "value": "testEndpoint"}],
        )

        # Act
        result = spark_engine._setup_s3_hadoop_conf(
            storage_connector=s3_connector,
            path="s3_test_path",
        )

        # Assert
        assert result == "s3a_test_path"
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.call_count
            == 7
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.access.key", s3_connector.access_key
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.secret.key", s3_connector.secret_key
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.server-side-encryption-algorithm",
            s3_connector.server_encryption_algorithm,
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.server-side-encryption-key", s3_connector.server_encryption_key
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.session.token", s3_connector.session_token
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.endpoint", s3_connector.arguments.get("fs.s3a.endpoint")
        )

    def test_setup_adls_hadoop_conf(self, mocker):
        # Arrange
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        adls_connector = storage_connector.AdlsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            spark_options=[
                {"name": "name_1", "value": "value_1"},
                {"name": "name_2", "value": "value_2"},
            ],
        )

        # Act
        result = spark_engine._setup_adls_hadoop_conf(
            storage_connector=adls_connector,
            path="adls_test_path",
        )

        # Assert
        assert result == "adls_test_path"
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.call_count
            == 2
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "name_1", "value_1"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "name_2", "value_2"
        )

    def test_is_spark_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.is_spark_dataframe(
            dataframe=None,
        )

        # Assert
        assert result is False

    def test_is_spark_dataframe_spark_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.is_spark_dataframe(
            dataframe=spark_df,
        )

        # Assert
        assert result is True

    def test_save_empty_dataframe(self, mocker):
        # Arrange
        mock_spark_engine_save_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.save_dataframe"
        )
        mock_spark_table = mocker.patch("pyspark.sql.session.SparkSession.table")

        # Arrange
        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            featurestore_name="test_featurestore",
        )

        # Act
        spark_engine.save_empty_dataframe(feature_group=fg)

        # Assert
        assert mock_spark_engine_save_dataframe.call_count == 1
        assert mock_spark_table.call_count == 1

    def test_apply_transformation_function_single_output(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        engine._engine_type = "spark"
        spark_engine = spark.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_one,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_one_col_0_": [2, 3],
            }
        )  # todo why it doesnt return int?

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_output(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        engine._engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int])
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_two,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_two_col_0_0": [2, 3],
                "plus_two_col_0_1": [3, 4],
            }
        )  # todo why it doesnt return int?

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        engine._engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int])
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "test_col_0-col_2_0": [2, 3],
                "test_col_0-col_2_1": [12, 13],
            }
        )  # todo why it doesnt return int?

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_setup_gcp_hadoop_conf(self, mocker):
        # Arrange
        mock_spark_engine_add_file = mocker.patch("hsfs.engine.spark.Engine.add_file")
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        content = (
            '{"type": "service_account", "project_id": "test", "private_key_id": "123456", '
            '"private_key": "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----", '
            '"client_email": "test@project.iam.gserviceaccount.com"}'
        )
        credentialsFile = "keyFile.json"
        with open(credentialsFile, "w") as f:
            f.write(content)

        gcs_connector = storage_connector.GcsConnector(
            id=1, name="test_connector", featurestore_id=99, key_path=credentialsFile
        )

        mock_spark_engine_add_file.return_value = "keyFile.json"

        # Act
        result = spark_engine._setup_gcp_hadoop_conf(
            storage_connector=gcs_connector,
            path="test_path",
        )

        # Assert
        assert result == "test_path"
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.setIfUnset.call_count
            == 2
        )
        assert mock_spark_engine_add_file.call_count == 1
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.call_count
            == 3
        )
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.unset.call_count
            == 3
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "google.cloud.auth.service.account.enable", "true"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.email", "test@project.iam.gserviceaccount.com"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key.id", "123456"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key",
            "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.unset.assert_any_call(
            "fs.gs.encryption.algorithm"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.unset.assert_any_call(
            "fs.gs.encryption.key"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.unset.assert_any_call(
            "fs.gs.encryption.key.hash"
        )

    def test_setup_gcp_hadoop_conf_algorithm(self, mocker):
        # Arrange
        mock_spark_engine_add_file = mocker.patch("hsfs.engine.spark.Engine.add_file")
        mock_pyspark_getOrCreate = mocker.patch(
            "pyspark.sql.session.SparkSession.builder.getOrCreate"
        )

        spark_engine = spark.Engine()

        content = (
            '{"type": "service_account", "project_id": "test", "private_key_id": "123456", '
            '"private_key": "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----", '
            '"client_email": "test@project.iam.gserviceaccount.com"}'
        )
        credentialsFile = "keyFile.json"
        with open(credentialsFile, "w") as f:
            f.write(content)

        gcs_connector = storage_connector.GcsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            key_path=credentialsFile,
            algorithm="temp_algorithm",
            encryption_key="1",
            encryption_key_hash="2",
        )

        mock_spark_engine_add_file.return_value = "keyFile.json"

        # Act
        result = spark_engine._setup_gcp_hadoop_conf(
            storage_connector=gcs_connector,
            path="test_path",
        )

        # Assert
        assert result == "test_path"
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.setIfUnset.call_count
            == 2
        )
        assert mock_spark_engine_add_file.call_count == 1
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.call_count
            == 6
        )
        assert (
            mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.unset.call_count
            == 0
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "google.cloud.auth.service.account.enable", "true"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.encryption.algorithm", gcs_connector.algorithm
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.encryption.key", gcs_connector.encryption_key
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.encryption.key.hash", gcs_connector.encryption_key_hash
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.email", "test@project.iam.gserviceaccount.com"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key.id", "123456"
        )
        mock_pyspark_getOrCreate.return_value.sparkContext._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key",
            "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",
        )

    def test_get_unique_values(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2, 2], "col_1": ["test_1", "test_2", "test_3"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.get_unique_values(
            feature_dataframe=spark_df,
            feature_name="col_0",
        )

        # Assert
        assert result == [1, 2]

    def test_create_empty_df(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2, 2], "col_1": ["test_1", "test_2", "test_3"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.create_empty_df(
            streaming_df=spark_df,
        )

        # Assert
        assert result.schema == spark_df.schema
        assert result.collect() == []
