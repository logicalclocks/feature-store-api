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
import pytest
from hsfs import ge_expectation
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS


class TestGeExpectation:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get"]["response"]

        # Act
        expect = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        print(expect.meta)
        assert expect.expectation_type == "1"
        assert expect.kwargs == {"kwargs_key": "kwargs_value"}
        assert expect.meta["meta_key"] == "meta_value"
        assert expect.meta["expectationId"] == 32
        assert expect.id == 32

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get_list"]["response"]

        # Act
        ge_list = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert len(ge_list) == 1
        expect = ge_list[0]
        print(expect.meta)
        assert expect.expectation_type == "1"
        assert expect.kwargs == {"kwargs_key": "kwargs_value"}
        assert expect.meta["meta_key"] == "meta_value"
        assert expect.meta["expectationId"] == 32
        assert expect.id == 32

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get_list_empty"]["response"]

        # Act
        ge_list = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert len(ge_list) == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_from_ge_object(self):
        import great_expectations

        # Arrange
        expectationId = 32
        expectation_type = "expect_column_min_to_be_between"
        kwargs = {"kwargs_key": "kwargs_value"}
        meta = {"meta_key": "meta_value", "expectationId": expectationId}
        ge_object = great_expectations.core.ExpectationConfiguration(
            expectation_type=expectation_type,
            kwargs=kwargs,
            meta=meta,
        )

        # Act
        expect = ge_expectation.GeExpectation.from_ge_type(ge_object)

        # Assert
        assert expect.id == 32
        assert expect.expectation_type == expectation_type
        assert expect.kwargs == kwargs
        assert expect.meta == meta
