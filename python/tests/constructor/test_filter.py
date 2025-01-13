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
from hsfs.constructor import filter


class TestFilter:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["filter"]["get"]["response"]

        # Act
        f = filter.Filter.from_response_json(json)

        # Assert
        assert isinstance(f._feature, feature.Feature)
        assert f._condition == "test_condition"
        assert f._value == "test_value"


class TestLogic:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["logic"]["get"]["response"]

        # Act
        logic = filter.Logic.from_response_json(json)

        # Assert
        assert logic._type == "test_type"
        assert isinstance(logic._left_f, filter.Filter)
        assert isinstance(logic._right_f, filter.Filter)
        assert isinstance(logic._left_l, filter.Logic)
        assert isinstance(logic._right_l, filter.Logic)
