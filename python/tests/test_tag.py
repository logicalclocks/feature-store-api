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


from hsfs import tag


class TestTag:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["tag"]["get"]["response"]

        # Act
        t_list = tag.Tag.from_response_json(json)

        # Assert
        assert len(t_list) == 1
        t = t_list[0]
        assert t.name == "test_name"
        assert t.value == "test_value"

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["tag"]["get_empty"]["response"]

        # Act
        t_list = tag.Tag.from_response_json(json)

        # Assert
        assert len(t_list) == 0
