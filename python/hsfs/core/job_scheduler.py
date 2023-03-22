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
from datetime import datetime
from typing import Optional, Union
import humps
import json

from hsfs import util


class JobScheduler:
    def __init__(
        self,
        start_date_time: Union[int, str, datetime, pd.Timestamp],
        job_frequency: str,
        enabled: bool = True,
        job_name: Optional[str] = None,
        id: Optional[int] = None,
        href: Optional[str] = None,
    ) -> None:
        """Contains the scheduler configuration for a Hopsworks job.

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
        self._id = id
        self._start_date_time = util.convert_event_time_to_timestamp(start_date_time)
        self._job_frequency = job_frequency
        self._job_name = job_name
        self._enabled = enabled
        self._href = href

    @classmethod
    def from_response_json(cls, json_dict):
        return cls(**humps.decamelize(json_dict))

    def to_dict(self):
        return {
            "id": self._id,
            "startDateTime": self._start_date_time,
            "jobFrequency": self._job_frequency,
            "jobName": self._job_name,
            "enabled": self._enabled,
            "href": self._href,
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
    def start_date_time(self) -> int:
        return self._start_date_time

    @property
    def job_frequency(self) -> str:
        return self._job_frequency

    @property
    def job_name(self) -> Optional[str]:
        return self._job_name

    @property
    def enabled(self) -> bool:
        return self._enabled
