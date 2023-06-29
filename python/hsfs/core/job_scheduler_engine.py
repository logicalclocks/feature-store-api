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
from hsfs.core import job_scheduler, job_scheduler_api
from typing import Optional, Union
from datetime import datetime, date
import pandas as pd
from hsfs.util import convert_event_time_to_timestamp


class JobSchedulerEngine:
    def __init__(self):
        self._job_scheduler_api = job_scheduler_api.JobSchedulerApi()

    def build_job_scheduler(
        self,
        job_frequency: str = "DAILY",
        start_date_time: Optional[Union[str, int, date, datetime, pd.Timestamp]] = None,
        end_date_time: Optional[Union[str, int, date, datetime, pd.Timestamp]] = None,
        next_execution_date_time: Optional[
            Union[str, int, date, datetime, pd.Timestamp]
        ] = None,
        cron_expression: Optional[str] = None,
        job_name: Optional[str] = None,
        enabled: bool = True,
        id: Optional[int] = None,
    ) -> "job_scheduler.JobScheduler":
        """Builds a job scheduler.

        Args:
            job_frequency: str, required
                Frequency of the job. Defaults to daily.
            start_date_time: Union[str, int, date, datetime], optional
                Job will start being executed on schedule from that time.
                Defaults to datetime.now().
            end_date_time: Union[str, int, date, datetime], optional
                Job will stop being executed on schedule from that time.
                Defaults to None.
            cron_expression: str, optional
                Cron expression for the job. If provided, cron expression will be used
                to schedule the job instead of job frequency. Defaults to None.
            job_name: str, optional
                Name of the job. Populated when registering the feature monitoring
                configuration to the backend. Defaults to None.
            enabled: bool, optional
                If enabled is false, the scheduled job is not executed.
                Defaults to True.
            id: int, optional
                Id of the job scheduler. Populated when registering the feature monitoring
                configuration to the backend. Defaults to None.

        Returns:
            JobScheduler The job scheduler.
        """
        if start_date_time is None:
            start_date_time = convert_event_time_to_timestamp(datetime.now())
        else:
            start_date_time = convert_event_time_to_timestamp(start_date_time)

        if job_frequency and job_frequency.upper() not in [
            "HOURLY",
            "DAILY",
            "WEEKLY",
            "CUSTOM",
        ]:
            raise ValueError(
                "Invalid job frequency. Supported frequencies are HOURLY, DAILY, WEEKLY."
            )

        return job_scheduler.JobScheduler(
            id=id,
            job_frequency=job_frequency,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            cron_expression=cron_expression,
            job_name=job_name,
            enabled=enabled,
            next_execution_date_time=next_execution_date_time,
        )

    def create_job_scheduler(
        self,
        the_job_scheduler: "job_scheduler.JobScheduler",
    ) -> "job_scheduler.JobScheduler":
        """Create a job scheduler.

        Args:
            the_job_scheduler: job scheduler object

        Returns:
            JobScheduler The job scheduler.
        """
        assert (
            the_job_scheduler.job_name is not None
        ), "Job name is required to persist a job_scheduler."

        return self._job_scheduler_api.create_job_scheduler(
            the_job_scheduler=the_job_scheduler
        )

    def get_job_scheduler(
        self,
        job_name: str,
    ) -> "job_scheduler.JobScheduler":
        """Get a job scheduler.

        Args:
            job_name: job name

        Returns:
            JobScheduler The job scheduler.
        """
        return self._job_scheduler_api.get_job_scheduler(job_name=job_name)

    def update_job_scheduler(
        self,
        the_job_scheduler: "job_scheduler.JobScheduler",
    ) -> "job_scheduler.JobScheduler":
        """Update a job scheduler.

        Args:
            the_job_scheduler: job scheduler object

        Returns:
            JobScheduler The job scheduler.
        """
        assert (
            the_job_scheduler.id is not None
        ), "Job id is required to update a job_scheduler."
        assert (
            the_job_scheduler.job_name is not None
        ), "Job name is required to update a job_scheduler."

        return self._job_scheduler_api.update_job_scheduler(
            the_job_scheduler=the_job_scheduler
        )

    def delete_job_scheduler(
        self,
        job_name: str,
    ):
        """Delete a job scheduler.

        Args:
            job_name: job name
        """
        return self._job_scheduler_api.delete_job_scheduler(job_name=job_name)

    def pause_or_resume_job_scheduler(
        self,
        job_name: str,
        pause: bool,
    ):
        """Pause or resume a job scheduler.

        Args:
            job_name: job name
            pause: pause if true otherwise resume the job scheduling
        """
        return self._job_scheduler_api.pause_or_resume_job_scheduler(
            job_name=job_name, pause=pause
        )
