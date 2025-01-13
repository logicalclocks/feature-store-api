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

from hsfs.engine import python, spark


class TestPythonSparkConvertDataframe:
    def test_convert_to_default_dataframe_w_timezone_notz(
        self, mocker, dataframe_fixture_times
    ):
        mocker.patch("hsfs.client.get_instance")
        python_engine = python.Engine()

        default_df_python = python_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        spark_engine = spark.Engine()

        default_df_spark_from_pd = spark_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        tz = spark_engine._spark_session.conf.get("spark.sql.session.timeZone")
        print(tz)

        assert (
            default_df_spark_from_pd.head()[2]
            == default_df_python["event_datetime_notz"][0].to_pydatetime()
        )

    def test_convert_to_default_dataframe_w_timezone_utc(
        self, mocker, dataframe_fixture_times
    ):
        mocker.patch("hsfs.client.get_instance")
        python_engine = python.Engine()

        default_df_python = python_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        spark_engine = spark.Engine()

        default_df_spark_from_pd = spark_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        assert (
            default_df_spark_from_pd.head()[3]
            == default_df_python["event_datetime_utc"][0].to_pydatetime()
        )

    def test_convert_to_default_dataframe_w_timezone_utc_3(
        self, mocker, dataframe_fixture_times
    ):
        mocker.patch("hsfs.client.get_instance")
        python_engine = python.Engine()

        default_df_python = python_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        spark_engine = spark.Engine()

        default_df_spark_from_pd = spark_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        assert (
            default_df_spark_from_pd.head()[4]
            == default_df_python["event_datetime_utc_3"][0].to_pydatetime()
        )

    def test_convert_to_default_dataframe_w_timezone_timestamp(
        self, mocker, dataframe_fixture_times
    ):
        mocker.patch("hsfs.client.get_instance")
        python_engine = python.Engine()

        default_df_python = python_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        spark_engine = spark.Engine()

        default_df_spark_from_pd = spark_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        assert (
            default_df_spark_from_pd.head()[5]
            == default_df_python["event_timestamp"][0].to_pydatetime()
        )

    def test_convert_to_default_dataframe_w_timezone_timestamp_pacific(
        self, mocker, dataframe_fixture_times
    ):
        mocker.patch("hsfs.client.get_instance")
        python_engine = python.Engine()

        default_df_python = python_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        spark_engine = spark.Engine()

        default_df_spark_from_pd = spark_engine.convert_to_default_dataframe(
            dataframe_fixture_times
        )

        assert (
            default_df_spark_from_pd.head()[6]
            == default_df_python["event_timestamp_pacific"][0].to_pydatetime()
        )
