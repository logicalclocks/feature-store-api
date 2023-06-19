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


from hsfs import feature
from hsfs.constructor import join, query


class TestJoin:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["join"]["get"]["response"]

        # Act
        j = join.Join.from_response_json(json)

        # Assert
        assert isinstance(j.query, query.Query)
        assert len(j._on) == 1
        assert isinstance(j._on[0], feature.Feature)
        assert j._on[0].name == "test_on"
        assert len(j._left_on) == 1
        assert isinstance(j._left_on[0], feature.Feature)
        assert j._left_on[0].name == "test_left_on"
        assert len(j._right_on) == 1
        assert isinstance(j._right_on[0], feature.Feature)
        assert j._right_on[0].name == "test_right_on"
        assert j._join_type == "test_join_type"
        assert j._prefix == "test_prefix"

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["join"]["get_basic_info"]["response"]

        # Act
        j = join.Join.from_response_json(json)

        # Assert
        assert isinstance(j.query, query.Query)
        assert len(j._on) == 0
        assert len(j._left_on) == 0
        assert len(j._right_on) == 0
        assert j._join_type == "INNER"
        assert j._prefix is None

    def test_from_response_json_left_join(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["join"]["left_on_right_on"]["response"]

        # Act
        j = join.Join.from_response_json(json)

        # Assert
        assert isinstance(j.query, query.Query)
        assert len(j._on) == 0
        assert len(j._left_on) == 1
        assert j._left_on[0] == "job_id"
        assert len(j._right_on) == 1
        assert j._right_on[0] == "internal_id"
        assert j._join_type == "LEFT"
        assert j._prefix is None
