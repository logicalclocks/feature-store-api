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

from hsfs import client
from hsfs.core import job, execution


class JobApi:

    def launch(self, job, feature_store_id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "jobs",
            job.name,
            "executions"
        ]

        _client._send_request("POST", path_params)

    def get(self, job, feature_store_id) -> job.Job:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "jobs",
            job.name
        ]

        return job.Job.from_response_json(_client._send_request("GET", path_params))

    def last_execution(self, job, feature_store_id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "jobs",
            job.name,
            "executions"
        ]

        query_params = {"limit": 1, "sort_by": "submissiontime:desc"}

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            )
        )
