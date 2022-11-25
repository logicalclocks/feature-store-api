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

from typing import Union, List, Dict
from hsfs import client, ge_validation_result


class ValidationResultApi:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Validation Result endpoints for the featuregroup resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the respective featuregroup
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id

    def get_validation_history(
        self,
        expectation_id: int,
        query_params: Dict[str, str] = {},
    ) -> Union[
        List[ge_validation_result.ValidationResult],
        ge_validation_result.ValidationResult,
    ]:
        """Get the validation report attached to a featuregroup.

        :return: validation report
        :rtype: Union[List[ValidationResult], ValidationResult]
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "validationresult",
            "history",
            expectation_id,
        ]
        headers = {"content-type": "application/json"}

        return ge_validation_result.ValidationResult.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )
