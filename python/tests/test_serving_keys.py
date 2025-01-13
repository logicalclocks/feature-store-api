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

from hsfs import serving_key


class TestServingKeys:
    def test_repr(self, backend_fixtures):
        # Arrange
        fixture = backend_fixtures["serving_keys"]["get"]["response"]
        serving_keys = [serving_key.ServingKey.from_response_json(sk) for sk in fixture]

        # Act - FSTORE-1268 This method should not throw an exception
        repr(serving_keys)
