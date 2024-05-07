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
from hsfs.client.exceptions import VectorDatabaseException
from hsfs.core.opensearch import OpenSearchClientSingleton, OpensearchRequestOption


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


class TestOpensearchRequestOption:

    def test_version_1_no_options(self):
        OpensearchRequestOption.get_version = lambda: (1, 1)
        options = OpensearchRequestOption.get_options({})
        assert options == {"timeout": "30s"}

    def test_version_1_with_options_timeout_int(self):
        OpensearchRequestOption.get_version = lambda: (1, 1)
        options = OpensearchRequestOption.get_options({"timeout": 45})
        assert options == {"timeout": "45s"}

    def test_version_2_3_no_options(self):
        OpensearchRequestOption.get_version = lambda: (2, 3)
        options = OpensearchRequestOption.get_options({})
        assert options == {"timeout": 30}

    def test_version_2_3_with_options(self):
        OpensearchRequestOption.get_version = lambda: (2, 3)
        options = OpensearchRequestOption.get_options({"timeout": 50})
        assert options == {"timeout": 50}
