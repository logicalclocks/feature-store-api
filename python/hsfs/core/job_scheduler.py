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
import pandas as pd
from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, Optional, Union
import humps
import json
import re

from hsfs import util
from hsfs.core import job_scheduler_engine


class JobScheduler:
    def __init__(
        self,
        start_date_time: Union[int, str, datetime, date, pd.Timestamp],
        job_frequency: Optional[str] = "CUSTOM",
        cron_expression: Optional[str] = None,
        end_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        next_execution_date_time: Optional[
            Union[int, str, datetime, date, pd.Timestamp]
        ] = None,
        enabled: bool = True,
        job_name: Optional[str] = None,
        id: Optional[int] = None,
    ) -> None:
        """Contains the scheduler configuration for a Hopsworks job.

        # Arguments
            id: Id of the job scheduler in Hopsworks.
            start_date_time: Start date and time from which to schedule the job.
                Even if the scheduler is enabled, the job will not be executed until the start date is reached.
            job_frequency: Frequency at which the job should be executed.
                Available options are: "NEAR REAL-TIME", "HOURLY", "DAILY", "WEEKLY".
                Near real-time means that the job will be executed every 10 minutes.
            enabled: Whether the scheduler is enabled or not, useful to pause scheduled execution.
            job_name: Name of the job to be scheduled.
            cron_expression: Cron expression to schedule the job. If provided, the `job_frequency` will be ignored.
            The cron expression must follow the quartz specification, e.g `0 0 12 ? * *` for daily at noon.
            next_execution_date_time: Date and time at which the job will be executed next.
            end_date_time: End date and time until which to schedule the job.

        # Returns
            `JobScheduler` The scheduler configuration.
        """
        if cron_expression is None and (
            job_frequency is None
            or job_frequency.upper()
            not in [
                "NEAR REAL-TIME",
                "HOURLY",
                "DAILY",
                "WEEKLY",
            ]
        ):
            raise ValueError(
                "job_frequency must be one of: 'NEAR REAL-TIME', 'HOURLY', 'DAILY', 'WEEKLY' or a cron_expression must be provided.\n"
                + f" Got: {job_frequency}"
            )
        self._id = id
        self._job_name = job_name
        self._enabled = enabled
        self.start_date_time = start_date_time
        self._cron_expression = cron_expression
        self.job_frequency = job_frequency
        self.end_date_time = end_date_time
        self._next_execution_date_time = util.convert_event_time_to_timestamp(
            next_execution_date_time
        )
        self._strftime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        self._job_scheduler_engine = job_scheduler_engine.JobSchedulerEngine()

    @classmethod
    def from_response_json(cls, json_dict):
        return cls(**humps.decamelize(json_dict))

    def save(self) -> "JobScheduler":
        """Saves the scheduler configuration to Hopsworks.

        # Returns
            `JobScheduler` The scheduler configuration.
        """
        if self._id:
            raise ValueError(
                "Cannot save a scheduler already registered, use `update()` to persist edit to Hopsworks."
            )

        return self._job_scheduler_engine.create_job_scheduler(the_job_scheduler=self)

    def delete(self) -> None:
        """Deletes the scheduler configuration from Hopsworks.

        !!! info
            The job itself is not deleted, only the scheduler configuration.
        """
        if not self._id:
            raise ValueError(
                "Cannot delete a scheduler not registered, use `save()` to register a new scheduler to Hopsworks."
            )
        if not self._job_name:
            raise ValueError("Cannot delete a scheduler without a job name.")

        self._job_scheduler_engine.delete_job_scheduler(job_name=self._job_name)

    def update(self) -> "JobScheduler":
        """Updates the scheduler configuration in Hopsworks.

        # Returns
            `JobScheduler` The scheduler configuration.
        """
        if not self._id:
            raise ValueError(
                "Cannot update a scheduler not registered, use `save()` to register a new scheduler to Hopsworks."
            )

        return self._job_scheduler_engine.update_job_scheduler(the_job_scheduler=self)

    def disable(self):
        """Disables the scheduling of job in Hopsworks, the job can still be triggered manually."""
        if not self._id:
            raise ValueError(
                "Cannot disable a scheduler not registered, use `save()` to register a new scheduler to Hopsworks."
            )
        if not self._job_name:
            raise ValueError("Cannot disable a scheduler without a job name.")

        self._job_scheduler_engine.disable_job_scheduler(job_name=self._job_name)
        self._enabled = False

    def enable(self):
        """Enables the scheduling of job in Hopsworks.

        !!! info
            On enabling, the next execution date and time will be updated based on the job frequency or cron expression.
        """
        if not self._id:
            raise ValueError(
                "Cannot enable a scheduler not registered, use `save()` to register a new scheduler to Hopsworks."
            )
        if not self._job_name:
            raise ValueError("Cannot enable a scheduler without a job name.")

        self._job_scheduler_engine.enable_job_scheduler(job_name=self._job_name)
        self._enabled = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self._id,
            "startDateTime": self._start_date_time,
            "endDateTime": self._end_date_time,
            "nextExecutionDateTime": self._next_execution_date_time,
            "cronExpression": self._cron_expression,
            "jobName": self._job_name,
            "enabled": self._enabled,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        the_dict = self.to_dict()
        the_dict["jobFrequency"] = self.job_frequency
        return json.dumps(humps.decamelize(self.to_dict()), indent=2)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def start_date_time(self, as_date_str: bool = False) -> Union[int, str]:
        """Start date and time from which to schedule the job. Even if the scheduler is enabled,
        the job will not be executed until the start date is reached.

        !!! note
            Setting a start date in the past will not create a backlog of jobs to be executed.
        """
        assert self._start_date_time is not None, "Start date time is not set."
        if as_date_str:
            return datetime.fromtimestamp(self._start_date_time / 1000).strftime(
                self._strftime_format
            )
        return self._start_date_time

    @start_date_time.setter
    def start_date_time(self, start_date_time: Union[int, str, datetime, pd.Timestamp]):
        """Start date and time from which to schedule the job. Even if the scheduler is enabled,
        the job will not be executed until the start date is reached.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.

        # Arguments
            value: Start date and time from which to schedule the job.
        """
        self._start_date_time = util.convert_event_time_to_timestamp(start_date_time)

    @property
    def end_date_time(self, as_str_date: bool = False) -> Optional[Union[int, str]]:
        """End date and time until which to schedule the job. If no end date is set,
        the job will be scheduled indefinitely.
        """
        if self._end_date_time is None:
            return None
        if as_str_date:
            return datetime.fromtimestamp(round(self._end_date_time / 1000)).strftime(
                self._strftime_format
            )
        return self._end_date_time

    @end_date_time.setter
    def end_date_time(
        self, end_date_time: Optional[Union[int, str, datetime, pd.Timestamp]]
    ):
        """End date and time until which to schedule the job. If no end date is set,
        the job will be scheduled indefinitely.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.

        # Arguments
            end_date_time: End date and time until which to schedule the job.
        """
        self._end_date_time = util.convert_event_time_to_timestamp(end_date_time)

    @property
    def next_execution_date_time(
        self, as_date_str: bool = False
    ) -> Optional[Union[int, str]]:
        """Next execution time of the job. This is set by Hopsworks and cannot be set manually."""
        if self._next_execution_date_time is None:
            return None
        if as_date_str:
            return datetime.fromtimestamp(
                self._next_execution_date_time / 1000
            ).strftime(self._strftime_format)
        return self._next_execution_date_time

    @property
    def job_frequency(self) -> str:
        """Frequency at which the job should be executed. Available options are:
        "NEAR REAL-TIME", "HOURLY", "DAILY", "WEEKLY", "CUSTOM".
        """
        assert self.cron_expression is not None, "Cron expression is not set"
        if re.match(r"^0 (\*|10|20|30|40|50)+/10", self._cron_expression):
            return "NEAR REAL-TIME"
        elif re.match(r"^0 (0|10|20|30|40|50) \* ", self._cron_expression):
            return "HOURLY"
        elif re.match(r"^0 (0|10|20|30|40|50) \d+ \? \* \*$", self._cron_expression):
            return "DAILY"
        elif re.match(
            r"^0 (0|10|20|30|40|50) \d+ \? \* (Mon|Tue|Wed|Thu|Fri|Sat|Sun|1|2|3|4|5|6|7)$",
            self._cron_expression,
        ):
            return "WEEKLY"

        return "CUSTOM"

    @job_frequency.setter
    def job_frequency(self, job_frequency: Optional[str]):
        """Frequency at which the job should be executed. Available options are:
        "NEAR REAL-TIME", "HOURLY", "DAILY", "WEEKLY".

        Setting the frequency will update the cron expression accordingly. Set the cron_expression property
        directly if you want a custom frequency.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.

        # Arguments
            job_frequency: Frequency at which the job should be executed.
        """
        if self._start_date_time is None:
            start = datetime.now()
        else:
            start = datetime.fromtimestamp(
                self._start_date_time / 1000, tz=timezone.utc
            )
        rounded_minutes = start.minute - (start.minute % 10) + 10
        if rounded_minutes == 60:
            rounded_minutes = 0
            start = start + timedelta(hours=1)
        if job_frequency == "NEAR REAL-TIME":
            self._cron_expression = f"0 {rounded_minutes}/10 * ? * *"
        elif job_frequency == "HOURLY":
            self._cron_expression = f"0 {rounded_minutes} * ? * *"
        elif job_frequency == "DAILY":
            self._cron_expression = f"0 0 {start.hour} ? * *"
        elif job_frequency == "WEEKLY":
            self._cron_expression = (
                f"0 {rounded_minutes} {start.hour} ? * {start.strftime('%a')}"
            )
        elif job_frequency == "CUSTOM" or job_frequency is None:
            self._job_frequency = "CUSTOM"
        else:
            raise ValueError(
                f"Invalid job frequency: {job_frequency}. "
                + "Set the cron_expression property if you want a custom frequency."
            )
        self._job_frequency = job_frequency

    @property
    def job_name(self) -> Optional[str]:
        return self._job_name

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool):
        """Whether the scheduler is enabled or not, useful to pause scheduled execution.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.

        # Arguments
            enabled: Whether the scheduler is enabled or not.
        """
        self._enabled = enabled

    @property
    def cron_expression(self) -> Optional[str]:
        return self._cron_expression

    @cron_expression.setter
    def cron_expression(self, cron_expression: str):
        """Cron expression to schedule the job. If this is set, the job_frequency will be ignored.

        !!! warning
            Cron expression must be provided in UTC timezone and following the QUARTZ specification.
            Example: `0 0 12 ? * WED` for every Wednesday at 12:00 UTC.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.
            Validation of the cron expression as well as update of the next_execution_date is performed
            by Hopsworks on save.

        # Arguments
            cron_expression: Cron expression to schedule the job.
        """
        self._cron_expression = cron_expression
        self._job_frequency = "CUSTOM"
