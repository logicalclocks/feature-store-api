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


from hsfs import expectation_suite, ge_expectation


class TestExpectationSuite:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]

        # Act
        es = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es.id == 21
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._feature_store_id == 99
        assert es._feature_group_id == 10
        assert es.ge_cloud_id == "test_ge_cloud_id"
        assert es.data_asset_type == "test_data_asset_type"
        assert es.run_validation == "test_run_validation"
        assert es.validation_ingestion_policy == "TEST_VALIDATION_INGESTION_POLICY"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {"great_expectations_version": "0.15.12", "key": "value"}

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get_list"]["response"]

        # Act
        es_list = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert len(es_list) == 1
        es = es_list[0]
        assert es.id == 21
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._feature_store_id == 99
        assert es._feature_group_id == 10
        assert es.ge_cloud_id == "test_ge_cloud_id"
        assert es.data_asset_type == "test_data_asset_type"
        assert es.run_validation == "test_run_validation"
        assert es.validation_ingestion_policy == "TEST_VALIDATION_INGESTION_POLICY"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {"great_expectations_version": "0.15.12", "key": "value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get_basic_info"]["response"]

        # Act
        es = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es.id is None
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._feature_store_id is None
        assert es._feature_group_id is None
        assert es.ge_cloud_id is None
        assert es.data_asset_type is None
        assert es.run_validation is True
        assert es.validation_ingestion_policy == "ALWAYS"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {"great_expectations_version": "0.15.12", "key": "value"}

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get_list_empty"]["response"]

        # Act
        es_list = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es_list is None
