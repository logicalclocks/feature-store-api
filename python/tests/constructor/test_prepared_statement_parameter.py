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


from hsfs.constructor import prepared_statement_parameter


class TestPreparedStatementParameter:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["prepared_statement_parameter"]["get"]["response"]

        # Act
        psp = (
            prepared_statement_parameter.PreparedStatementParameter.from_response_json(
                json
            )
        )

        # Assert
        assert psp.name == "test_name"
        assert psp.index == "test_index"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["prepared_statement_parameter"]["get_basic_info"][
            "response"
        ]

        # Act
        psp = (
            prepared_statement_parameter.PreparedStatementParameter.from_response_json(
                json
            )
        )

        # Assert
        assert psp.name is None
        assert psp.index is None
