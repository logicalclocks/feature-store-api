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
from hsfs.core.opensearch import OpenSearchClientSingleton
from hsfs.client.exceptions import VectorDatabaseException


class TestOpenSearchClientSingleton:
    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        mocker.patch(
            "hsfs.core.opensearch.OpenSearchClientSingleton._setup_opensearch_client"
        )

        self.target = OpenSearchClientSingleton()

    @pytest.mark.parametrize(
        "message, expected_reason, expected_info",
        [
            (
                "[knn] requires k <= 5",
                VectorDatabaseException.REQUESTED_K_TOO_LARGE,
                {VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K: 5},
            ),
            (
                # Added bracket to number
                "[knn] requires k <= [5]",
                VectorDatabaseException.REQUESTED_K_TOO_LARGE,
                {},
            ),
            (
                "Result window is too large, from + size must be less than or equal to: [10000] but was [80000]",
                VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE,
                {VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N: 10000},
            ),
            (
                # Removed the bracket from numbers
                "Result window is too large, from + size must be less than or equal to: 10000 but was 80000",
                VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE,
                {},
            ),
            (
                "Some other error message",
                VectorDatabaseException.OTHERS,
                {},
            ),
        ],
    )
    def test_create_vector_database_exception(
        self, message, expected_reason, expected_info
    ):
        exception = self.target._create_vector_database_exception(message)
        assert isinstance(exception, VectorDatabaseException)
        assert exception.reason == expected_reason
        assert exception.info == expected_info
