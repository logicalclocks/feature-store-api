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

from hsfs import client, feature_group_validation as fgv


class FeatureGroupValidationsApi:
    def __init__(self, feature_store_id, entity_type):
        """Data validations endpoint for `trainingdatasets` `featuregroups`.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param entity_type: "featuregroups"
        :type entity_type: str
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    def put(self, metadata_instance, feature_group_validation):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "validations",
        ]
        headers = {"content-type": "application/json"}
        return fgv.FeatureGroupValidation.from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                headers=headers,
                data=feature_group_validation.json(),
            )
        )

    def get(self, metadata_instance, validation_time=None, commit_time=None):
        """Gets the statistics for a specific commit time for an instance."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "validations",
        ]
        headers = {"content-type": "application/json"}
        if validation_time is not None:
            query_params = {
                "filter_by": "validation_time_eq:" + str(validation_time),
            }
        elif commit_time:
            query_params = {
                "filter_by": "commit_time_eq:" + str(commit_time),
            }
        else:
            query_params = None

        return fgv.FeatureGroupValidation.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )
