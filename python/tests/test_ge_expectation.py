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


from hsfs import ge_expectation


class TestGeExpectation:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get"]["response"]

        # Act
        ge = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert ge.expectation_type == "1"
        assert ge.kwargs == 2
        assert ge.meta == 3
        assert ge.id == "4"

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get_list"]["response"]

        # Act
        ge_list = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert len(ge_list) == 1
        ge = ge_list[0]
        assert ge.expectation_type == "1"
        assert ge.kwargs == 2
        assert ge.meta == 3
        assert ge.id == "4"

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get_list_empty"]["response"]

        # Act
        ge_list = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert len(ge_list) == 0
