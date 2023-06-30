#
#   Copyright 2023 Hopsworks AB
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
from typing import List, Optional
from hsfs import client
from hsfs.core import job_scheduler


class JobSchedulerApi:
    def get_job_scheduler(self, job_name: str) -> "job_scheduler.JobScheduler":
        """
        Get a job scheduler by job_name.

        :param job_name: name of the job
        :return: job scheduler object
        """
        _client = client.get_instance()

        return job_scheduler.JobScheduler.from_response_json(
            _client._send_request(
                "GET",
                path_params=self.build_path_params(
                    job_name=job_name,
                ),
            )
        )

    def create_job_scheduler(
        self, the_job_scheduler: "job_scheduler.JobScheduler"
    ) -> "job_scheduler.JobScheduler":
        """
        Create a job scheduler.

        :param the_job_scheduler: job scheduler object
        """
        _client = client.get_instance()
        assert (
            the_job_scheduler.job_name is not None
        ), "Job scheduler job name must be set"

        return job_scheduler.JobScheduler.from_response_json(
            _client._send_request(
                "POST",
                path_params=self.build_path_params(job_name=the_job_scheduler.job_name),
                headers={"content-type": "application/json"},
                data=the_job_scheduler.json(),
            )
        )

    def update_job_scheduler(
        self, the_job_scheduler: "job_scheduler.JobScheduler"
    ) -> "job_scheduler.JobScheduler":
        """
        Update a job scheduler.

        :param the_job_scheduler: job scheduler object
        """
        _client = client.get_instance()
        assert the_job_scheduler.id is not None, "Job scheduler id must be set"
        assert (
            the_job_scheduler.job_name is not None
        ), "Job scheduler job name must be set"

        return job_scheduler.JobScheduler.from_response_json(
            _client._send_request(
                "PUT",
                path_params=self.build_path_params(
                    job_name=the_job_scheduler.job_name,
                ),
                headers={"content-type": "application/json"},
                data=the_job_scheduler.json(),
            )
        )

    def delete_job_scheduler(self, job_name: str):
        """
        Delete a job scheduler.

        :param job_name: name of the job
        """
        _client = client.get_instance()
        _client._send_request(
            "DELETE",
            path_params=self.build_path_params(
                job_name=job_name,
            ),
        )

    def pause_or_resume_job_scheduler(self, job_name: str, pause: bool):
        """
        Pause or resume a job scheduler.

        :param job_name: name of the job
        :param pause: True to pause, False to resume
        """
        _client = client.get_instance()
        _client._send_request(
            "POST",
            path_params=self.build_path_params(
                job_name=job_name,
                extra="pause" if pause else "resume",
            ),
        )

    def build_path_params(
        self, job_name: str, extra: Optional[str] = None
    ) -> List[str]:
        """
        Single source of truth for path parameters of the job scheduler API.

        :param job_name: name of the job
        :param scheduler_id: id of the job scheduler. Optional.

        :return: list of path parameters
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            job_name,
            "scheduler",
            "v2",
        ]

        if extra:
            path_params.append(extra)

        return path_params
