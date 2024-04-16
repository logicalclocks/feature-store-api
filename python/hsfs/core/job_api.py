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
from __future__ import annotations

import json
from typing import Any, Dict, Union

from hsfs import client
from hsfs.core import (
    execution,
    ingestion_job_conf,
    job,
    job_configuration,
    job_schedule,
)


class JobApi:
    def create(
        self,
        name: str,
        job_conf: Union[
            job_configuration.JobConfiguration, ingestion_job_conf.IngestionJobConf
        ],
    ) -> job.Job:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=job_conf.json()
            )
        )

    def launch(self, name: str, args: str = None) -> None:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "executions"]

        _client._send_request("POST", path_params, data=args)

    def get(self, name: str) -> job.Job:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name]

        return job.Job.from_response_json(_client._send_request("GET", path_params))

    def last_execution(self, job: job.Job) -> execution.Execution:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", job.name, "executions"]

        query_params = {"limit": 1, "sort_by": "submissiontime:desc"}

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            )
        )

    def create_or_update_schedule_job(
        self, name: str, schedule_config: Dict[str, Any]
    ) -> job_schedule.JobSchedule:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "schedule", "v2"]
        headers = {"content-type": "application/json"}
        method = "PUT" if schedule_config["id"] else "POST"

        return job_schedule.JobSchedule.from_response_json(
            _client._send_request(
                method, path_params, headers=headers, data=json.dumps(schedule_config)
            )
        )

    def delete_schedule_job(self, name: str) -> None:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "schedule", "v2"]

        return _client._send_request(
            "DELETE",
            path_params,
        )
