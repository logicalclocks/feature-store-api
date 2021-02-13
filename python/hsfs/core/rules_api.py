#
#   Copyright 2020 Logical Clocks AB
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

from hsfs import client, ruledefinition


class RulesApi:
    def __init__(self):
        """Rules endpoint for Feature Store data validation rules."""

    def get(self, name=None):
        """Get the rules available in Hopsworks to be used for data validation.

        Gets all rules if no rule name is specified.

        :param name: rule name
        :type name: str
        :return: list of rules
        :rtype: list
        """
        _client = client.get_instance()
        path_params = ["rules"]

        if name is not None:
            path_params.append(name)

        return ruledefinition.RuleDefinition.from_response_json(
            _client._send_request("GET", path_params)
        )
