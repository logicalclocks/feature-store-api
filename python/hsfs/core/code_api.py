#
#   Copyright 2021 Logical Clocks AB
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

from hsfs import client


class CodeApi:
    def __init__(self, feature_store_id, entity_type):
        """Code endpoint for `trainingdatasets` and `featuregroups` resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param entity_type: "trainingdatasets" or "featuregroups"
        :type entity_type: str
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    def post(
        self,
        metadata_instance,
        code,
        entity_id,
        code_type,
        databricks_cluster_id=None,
    ):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "code",
        ]

        headers = {"content-type": "application/json"}

        query_params = {
            "entityId": entity_id,
            "type": code_type,
            "databricksClusterId": databricks_cluster_id,
        }

        _client._send_request(
            "POST", path_params, query_params, headers=headers, data=code.json()
        )
