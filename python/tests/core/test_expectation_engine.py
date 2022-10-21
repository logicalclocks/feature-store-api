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
from hsfs.core import expectation_engine
from hsfs.ge_expectation import GeExpectation


class TestExpectationEngine:
    def test_create(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        expectation_suite_id = 21

        mocker.patch("hsfs.engine.get_type")
        mock_expectation_api = mocker.patch("hsfs.core.expectation_api.ExpectationApi")

        ge_expectation_engine = expectation_engine.ExpectationEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            expectation_suite_id=expectation_suite_id,
        )

        # Act
        ge_expectation_engine.create(expectation=None)

        # Assert
        assert mock_expectation_api.return_value.create.call_count == 1

    def test_update(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        expectation_suite_id = 21

        mocker.patch("hsfs.engine.get_type")
        mock_expectation_api = mocker.patch("hsfs.core.expectation_api.ExpectationApi")

        ge_expectation_engine = expectation_engine.ExpectationEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            expectation_suite_id=expectation_suite_id,
        )

        # Act
        ge_expectation_engine.update(expectation=None)

        # Assert
        assert mock_expectation_api.return_value.update.call_count == 1

    def test_get(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        expectation_suite_id = 21
        expectation_id = 32

        mocker.patch("hsfs.engine.get_type")
        mock_expectation_api = mocker.patch("hsfs.core.expectation_api.ExpectationApi")

        ge_expectation_engine = expectation_engine.ExpectationEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            expectation_suite_id=expectation_suite_id,
        )

        # Act
        ge_expectation_engine.get(expectation_id=expectation_id)

        # Assert
        assert mock_expectation_api.return_value.get.call_count == 1

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        expectation_suite_id = 21
        expectation_id = 32

        mocker.patch("hsfs.engine.get_type")
        mock_expectation_api = mocker.patch("hsfs.core.expectation_api.ExpectationApi")

        ge_expectation_engine = expectation_engine.ExpectationEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            expectation_suite_id=expectation_suite_id,
        )

        # Act
        ge_expectation_engine.delete(expectation_id)

        # Assert
        assert mock_expectation_api.return_value.delete.call_count == 1

    def test_check_for_id(self):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        expectation_suite_id = 21

        ge_expectation_no_id = GeExpectation(
            expectation_type="expect_column_max_to_be_between",
            kwargs={"column": "my_feature"},
            meta={},
        )
        ge_expectation_set_id = GeExpectation(
            id=32,
            expectation_type="expect_column_max_to_be_between",
            kwargs={"column": "my_feature"},
            meta={},
        )
        ge_expectation_meta_id = GeExpectation(
            expectation_type="expect_column_max_to_be_between",
            kwargs={"column": "my_feature"},
            meta={"expectationId": 32},
        )

        ge_expectation_engine = expectation_engine.ExpectationEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            expectation_suite_id=expectation_suite_id,
        )

        # Act
        ge_expectation_engine.check_for_id(ge_expectation_meta_id)
        ge_expectation_engine.check_for_id(ge_expectation_set_id)
        with pytest.raises(ValueError):
            ge_expectation_engine.check_for_id(ge_expectation_no_id)
