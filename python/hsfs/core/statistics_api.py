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

from hsfs import client, statistics
from hsfs.core import job


class StatisticsApi:
    def __init__(self, feature_store_id, entity_type):
        """Statistics endpoint for `trainingdatasets` and `featuregroups` resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param entity_type: "trainingdatasets" or "featuregroups"
        :type entity_type: str
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    def post(self, metadata_instance, statistics):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "statistics",
        ]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "POST", path_params, headers=headers, data=statistics.json()
        )

    def get(self, metadata_instance, commit_timestamp):
        """Gets the statistics for a specific commit time for an instance."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "statistics",
        ]
        headers = {"content-type": "application/json"}
        query_params = {
            "filter_by": "commit_time_eq:" + str(commit_timestamp),
            "fields": "content",
        }
        return statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def get_last(self, metadata_instance):
        """Gets the statistics of the last commit for an instance."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "statistics",
        ]
        headers = {"content-type": "application/json"}
        query_params = {
            "sort_by": "commit_time:desc",
            "offset": 0,
            "limit": 1,
            "fields": "content",
        }
        return statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def compute(self, metadata_instance):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "statistics",
            "compute",
        ]
        return job.Job.from_response_json(_client._send_request("POST", path_params))
