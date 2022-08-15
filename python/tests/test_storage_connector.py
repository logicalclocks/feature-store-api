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

from hsfs import storage_connector


class TestHopsfsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_hopsfs_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_hopsfs"
        assert sc._featurestore_id == 67
        assert sc.description == "HOPSFS connector description"
        assert sc._hopsfs_path == "test_path"
        assert sc._dataset_name == "test_dataset_name"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_hopsfs_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_hopsfs"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc._hopsfs_path == None
        assert sc._dataset_name == None


class TestS3Connector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_s3_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_s3"
        assert sc._featurestore_id == 67
        assert sc.description == "S3 connector description"
        assert sc.access_key == "test_access_key"
        assert sc.secret_key == "test_secret_key"
        assert sc.server_encryption_algorithm == "test_server_encryption_algorithm"
        assert sc.server_encryption_key == "test_server_encryption_key"
        assert sc.bucket == "test_bucket"
        assert sc.session_token == "test_session_token"
        assert sc.iam_role == "test_iam_role"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_s3_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_s3"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.access_key == None
        assert sc.secret_key == None
        assert sc.server_encryption_algorithm == None
        assert sc.server_encryption_key == None
        assert sc.bucket == None
        assert sc.session_token == None
        assert sc.iam_role == None


class TestRedshiftConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_redshift_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_redshift"
        assert sc._featurestore_id == 67
        assert sc.description == "Redshift connector description"
        assert sc.cluster_identifier == "test_cluster_identifier"
        assert sc.database_driver == "test_database_driver"
        assert sc.database_endpoint == "test_database_endpoint"
        assert sc.database_name == "test_database_name"
        assert sc.database_port == "test_database_port"
        assert sc.table_name == "test_table_name"
        assert sc.database_user_name == "test_database_user_name"
        assert sc.auto_create == "test_auto_create"
        assert sc.database_password == "test_database_password"
        assert sc.database_group == "test_database_group"
        assert sc.iam_role == "test_iam_role"
        assert sc.arguments == "test_arguments"
        assert sc.expiration == "test_expiration"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_redshift_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_redshift"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.cluster_identifier == None
        assert sc.database_driver == None
        assert sc.database_endpoint == None
        assert sc.database_name == None
        assert sc.database_port == None
        assert sc.table_name == None
        assert sc.database_user_name == None
        assert sc.auto_create == None
        assert sc.database_password == None
        assert sc.database_group == None
        assert sc.iam_role == None
        assert sc.arguments == None
        assert sc.expiration == None


class TestAdlsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_adls_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_adls"
        assert sc._featurestore_id == 67
        assert sc.description == "Adls connector description"
        assert sc.generation == "test_generation"
        assert sc.directory_id == "test_directory_id"
        assert sc.application_id == "test_application_id"
        assert sc.service_credential == "test_service_credential"
        assert sc.account_name == "test_account_name"
        assert sc.container_name == "test_container_name"
        assert sc._spark_options == {'test_name': 'test_value'}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_adls_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_adls"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.generation == None
        assert sc.directory_id == None
        assert sc.application_id == None
        assert sc.service_credential == None
        assert sc.account_name == None
        assert sc.container_name == None
        assert sc._spark_options == {}


class TestSnowflakeConnector:
    def test_spark_options_db_table_none(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=None
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_empty(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=""
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_value(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="test"
        )

        spark_options = snowflake_connector.spark_options()

        assert spark_options["dbtable"] == "test"

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_snowflake_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_snowflake"
        assert sc._featurestore_id == 67
        assert sc.description == "Snowflake connector description"
        assert sc.database == "test_database"
        assert sc.password == "test_password"
        assert sc.token == "test_token"
        assert sc.role == "test_role"
        assert sc.schema == "test_schema"
        assert sc.table == "test_table"
        assert sc.url == "test_url"
        assert sc.user == "test_user"
        assert sc.warehouse == "test_warehouse"
        assert sc.application == "test_application"
        assert sc._options == {'test_name': 'test_value'}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_snowflake_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_snowflake"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.database == None
        assert sc.password == None
        assert sc.token == None
        assert sc.role == None
        assert sc.schema == None
        assert sc.table == None
        assert sc.url == None
        assert sc.user == None
        assert sc.warehouse == None
        assert sc.application == None
        assert sc._options == {}


class TestJdbcConnector:
    def test_spark_options_arguments_none(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = storage_connector.JdbcConnector(
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

        jdbc_connector = storage_connector.JdbcConnector(
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
        arguments = [
            {"name": "arg1", "value": "value1"},
            {"name": "arg2", "value": "value2"},
        ]

        jdbc_connector = storage_connector.JdbcConnector(
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

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_jdbc_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_jdbc"
        assert sc._featurestore_id == 67
        assert sc.description == "JDBC connector description"
        assert sc.connection_string == "test_conn_string"
        assert sc.arguments == [{'name': 'sslTrustStore'},
                                {'name': 'trustStorePassword'},
                                {'name': 'sslKeyStore'},
                                {'name': 'keyStorePassword'}]

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_jdbc_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_jdbc"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.connection_string == None
        assert sc.arguments == None


class TestKafkaConnector:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["get_kafka_storage_connector"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = 'result_from_add_file'

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_kafka"
        assert sc._featurestore_id == 67
        assert sc.description == "Kafka connector description"
        assert sc._bootstrap_servers == "test_bootstrap_servers"
        assert sc.security_protocol == "test_security_protocol"
        assert sc.ssl_truststore_location == "result_from_add_file"
        assert sc._ssl_truststore_password == "test_ssl_truststore_password"
        assert sc.ssl_keystore_location == "result_from_add_file"
        assert sc._ssl_keystore_password == "test_ssl_keystore_password"
        assert sc._ssl_key_password == "test_ssl_key_password"
        assert sc.ssl_endpoint_identification_algorithm == "test_ssl_endpoint_identification_algorithm"
        assert sc.options == {'test_name': 'test_value'}

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["get_kafka_storage_connector_basic_info"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = 'result_from_add_file'

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_kafka"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc._bootstrap_servers == None
        assert sc.security_protocol == None
        assert sc.ssl_truststore_location == "result_from_add_file"
        assert sc._ssl_truststore_password == None
        assert sc.ssl_keystore_location == "result_from_add_file"
        assert sc._ssl_keystore_password == None
        assert sc._ssl_key_password == None
        assert sc.ssl_endpoint_identification_algorithm == None
        assert sc.options == {}


class TestGcsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_gcs_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_gcs"
        assert sc._featurestore_id == 67
        assert sc.description == "Gcs connector description"
        assert sc.key_path == "test_key_path"
        assert sc.bucket == "test_bucket"
        assert sc.algorithm == "test_algorithm"
        assert sc.encryption_key == "test_encryption_key"
        assert sc.encryption_key_hash == "test_encryption_key_hash"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_gcs_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_gcs"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.key_path == None
        assert sc.bucket == None
        assert sc.algorithm == None
        assert sc.encryption_key == None
        assert sc.encryption_key_hash == None


class TestBigQueryConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_big_query_storage_connector"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_big_query"
        assert sc._featurestore_id == 67
        assert sc.description == "BigQuery connector description"
        assert sc.key_path == "test_key_path"
        assert sc.parent_project == "test_parent_project"
        assert sc.dataset == "test_dataset"
        assert sc.query_table == "test_query_table"
        assert sc.query_project == "test_query_project"
        assert sc.materialization_dataset == "test_materialization_dataset"
        assert sc.arguments == {'test_name': 'test_value'}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_big_query_storage_connector_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_big_query"
        assert sc._featurestore_id == 67
        assert sc.description == None
        assert sc.key_path == None
        assert sc.parent_project == None
        assert sc.dataset == None
        assert sc.query_table == None
        assert sc.query_project == None
        assert sc.materialization_dataset == None
        assert sc.arguments == {}
