#
#   Copyright 2021 Logical Clocks AB
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

from hsfs.storage_connector import JdbcConnector, SnowflakeConnector


class TestJdbcConnector:
    def test_spark_options_arguments_none(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=None,
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string

    def test_spark_options_arguments_empty(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments="",
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string

    def test_spark_options_arguments_arguments(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )
        arguments = "arg1=value1,arg2=value2"

        jdbc_connector = JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=arguments,
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string
        assert spark_options["arg1"] == "value1"
        assert spark_options["arg2"] == "value2"


class TestSnowflakeConnector:
    def test_spark_options_db_table_none(self):
        snowflake_connector = SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=None
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_empty(self):
        snowflake_connector = SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=""
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_value(self):
        snowflake_connector = SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="test"
        )

        spark_options = snowflake_connector.spark_options()

        assert spark_options["dbtable"] == "test"
