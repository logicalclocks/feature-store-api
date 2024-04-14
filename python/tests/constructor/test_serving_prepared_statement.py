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


from hsfs.constructor import prepared_statement_parameter, serving_prepared_statement


class TestServingPreparedStatement:
    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["serving_prepared_statement"]["get_list"]["response"]

        # Act
        sps_list = (
            serving_prepared_statement.ServingPreparedStatement.from_response_json(json)
        )

        # Assert
        assert len(sps_list) == 1
        sps = sps_list[0]
        assert sps._feature_group_id == "test_feature_group_id"
        assert sps._prepared_statement_index == "test_prepared_statement_index"
        assert len(sps._prepared_statement_parameters) == 1
        assert isinstance(
            sps._prepared_statement_parameters[0],
            prepared_statement_parameter.PreparedStatementParameter,
        )
        assert sps._query_online == "test_query_online"
        assert sps._prefix == "test_prefix"

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["serving_prepared_statement"]["get_list_empty"][
            "response"
        ]

        # Act
        sps_list = (
            serving_prepared_statement.ServingPreparedStatement.from_response_json(json)
        )

        # Assert
        assert len(sps_list) == 0
