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
        json = backend_fixtures["get_expectation_suite"]["response"]

        # Act
        es = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es.id == "test_id"
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._featurestore_id == "test_featurestore_id"
        assert es._featuregroup_id == "test_featuregroup_id"
        assert es.ge_cloud_id == "test_ge_cloud_id"
        assert es.data_asset_type == "test_data_asset_type"
        assert es.run_validation == "test_run_validation"
        assert es.validation_ingestion_policy == "TEST_VALIDATION_INGESTION_POLICY"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {'key': 'value'}

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_expectation_suite_list"]["response"]

        # Act
        es_list = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert len(es_list) == 1
        es = es_list[0]
        assert es.id == "test_id"
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._featurestore_id == "test_featurestore_id"
        assert es._featuregroup_id == "test_featuregroup_id"
        assert es.ge_cloud_id == "test_ge_cloud_id"
        assert es.data_asset_type == "test_data_asset_type"
        assert es.run_validation == "test_run_validation"
        assert es.validation_ingestion_policy == "TEST_VALIDATION_INGESTION_POLICY"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {'key': 'value'}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_expectation_suite_basic_info"]["response"]

        # Act
        es = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es.id == None
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._featurestore_id == None
        assert es._featuregroup_id == None
        assert es.ge_cloud_id == None
        assert es.data_asset_type == None
        assert es.run_validation == True
        assert es.validation_ingestion_policy == 'STRICT'
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {'key': 'value'}

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["get_expectation_suite_list_empty"]["response"]

        # Act
        es_list = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es_list == None