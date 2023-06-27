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
from datetime import datetime, timedelta
from typing import Optional, Union
import humps
import json
import re

from hsfs import util


class JobScheduler:
    def __init__(
        self,
        start_date_time: Union[int, str, datetime, pd.Timestamp],
        job_frequency: Optional[str] = "CUSTOM",
        cron_expression: Optional[str] = None,
        end_date_time: Optional[Union[int, str, datetime, pd.Timestamp]] = None,
        next_execution_date_time: Optional[
            Union[int, str, datetime, pd.Timestamp]
        ] = None,
        enabled: bool = True,
        job_name: Optional[str] = None,
        id: Optional[int] = None,
    ) -> None:
        """Contains the scheduler configuration for a Hopsworks job.

        If you provide a `cron_expression`, the `job_frequency` will be ignored.

        # Arguments
            id: Id of the job scheduler in Hopsworks.
            start_date_time: Start date and time from which to schedule the job.
                Even if the scheduler is enabled, the job will not be executed until the start date is reached.
            job_frequency: Frequency at which the job should be executed.
                Available options are: "HOURLY", "DAILY", "WEEKLY".
            enabled: Whether the scheduler is enabled or not, useful to pause scheduled execution.
            job_name: Name of the job to be scheduled.

        # Returns
            `JobScheduler` The scheduler configuration.
        """
        if cron_expression is None and job_frequency not in [
            "HOURLY",
            "DAILY",
            "WEEKLY",
        ]:
            raise ValueError(
                "job_frequency must be one of: 'HOURLY', 'DAILY', 'WEEKLY' or a cron_expression must be provided.\n"
                + f" Got: {job_frequency}"
            )
        self._id = id
        self._job_name = job_name
        self._enabled = enabled
        self._cron_expression = cron_expression
        self.job_frequency = job_frequency
        self.start_date_time = start_date_time
        self.end_date_time = end_date_time
        self._next_execution_date_time = util.convert_event_time_to_timestamp(
            next_execution_date_time
        )

    @classmethod
    def from_response_json(cls, json_dict):
        return cls(**humps.decamelize(json_dict))

    def to_dict(self):
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

    def __str__(self):
        return self.json()

    def __repr__(self):
        return json.dumps(humps.decamelize(self.to_dict()), indent=2)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def start_date_time(self, as_date_str: bool = False) -> int:
        """Start date and time from which to schedule the job. Even if the scheduler is enabled,
        the job will not be executed until the start date is reached.

        !!! note
            Setting a start date in the past will not create a backlog of jobs to be executed.
        """
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
            return datetime.fromtimestamp(self._end_date_time / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
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
            ).strftime("%Y-%m-%d %H:%M:%S")
        return self._next_execution_date_time

    @property
    def job_frequency(self):
        """Frequency at which the job should be executed. Available options are:
        "NEAR REAL-TIME", "HOURLY", "DAILY", "WEEKLY", "CUSTOM".
        """
        assert self.cron_expression is not None, "Cron expression is not set"
        if re.match(r"^0 (\*|10|20|30|40|50)+/10", self._cron_expression):
            return "NEAR REAL-TIME"
        elif re.match(r"^0 (0|10|20|30|40|50) \* ", self._cron_expression):
            return "HOURLY"
        elif re.match(r"^0 (0|10|20|30|40|50) \d+ \? \* \* \*$", self._cron_expression):
            return "DAILY"
        elif re.match(
            r"^0 (0|10|20|30|40|50) \d+ \? \* \* (Mon|Tue|Wed|Thu|Fri|Sat|Sun|1|2|3|4|5|6|7)$",
            self._cron_expression,
        ):
            return "WEEKLY"

        return "CUSTOM"

    @job_frequency.setter
    def job_frequency(self, job_frequency: str):
        """Frequency at which the job should be executed. Available options are:
        "NEAR REAL-TIME", "HOURLY", "DAILY", "WEEKLY".

        Setting the frequency will update the cron expression accordingly. Set the cron_expression property
        directly if you want a custom frequency.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.

        # Arguments
            job_frequency: Frequency at which the job should be executed.
        """
        now = datetime.now()
        rounded_minutes = now.minute - (now.minute % 10) + 10
        if rounded_minutes == 60:
            rounded_minutes = 0
            now = now + timedelta(hours=1)
        if job_frequency == "NEAR REAL-TIME":
            self._cron_expression = f"0 {rounded_minutes}/10 * ? * * *"
        elif job_frequency == "HOURLY":
            self._cron_expression = f"0 {rounded_minutes} * ? * * *"
        elif job_frequency == "DAILY":
            self._cron_expression = f"0 0 {now.hour} ? * * *"
        elif job_frequency == "WEEKLY":
            self._cron_expression = (
                f"0 {rounded_minutes} {now.hour} ? * * {now.strftime('%a')}"
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
            Example: `0 0 12 ? * * WED` for every Wednesday at 12:00 UTC.

        !!! info
            The setter does not persist change in the backend, call `save()` to persist the change.
            Validation of the cron expression as well as update of the next_execution_date is performed
            by Hopsworks on save.

        # Arguments
            cron_expression: Cron expression to schedule the job.
        """
        self._cron_expression = cron_expression
        self._job_frequency = "CUSTOM"
