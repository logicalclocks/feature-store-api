#
#   Copyright 2024 Hopsworks AB
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

import humps
import json
from datetime import datetime, timezone
from hsfs import util


class JobSchedule:
    def __init__(
        self,
        start_date_time,
        enabled,
        cron_expression,
        next_execution_date_time=None,
        id=None,
        end_date_time=None,
        **kwargs,
    ):
        self._id = id
        self._start_date_time = (
            datetime.fromtimestamp(start_date_time / 1000, tz=timezone.utc)
            if isinstance(start_date_time, int)
            else start_date_time
        )

        self._end_date_time = (
            datetime.fromtimestamp(end_date_time / 1000, tz=timezone.utc)
            if isinstance(end_date_time, int)
            else end_date_time
        )
        self._enabled = enabled
        self._cron_expression = cron_expression

        self._next_execution_date_time = (
            datetime.fromtimestamp(next_execution_date_time / 1000, tz=timezone.utc)
            if isinstance(next_execution_date_time, int)
            else next_execution_date_time
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "startDateTime": int(self._start_date_time.timestamp() * 1000.0)
            if self._start_date_time
            else None,
            "endDateTime": int(self._end_date_time.timestamp() * 1000.0)
            if self._end_date_time
            else None,
            "cronExpression": self._cron_expression,
            "enabled": self._enabled,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def id(self):
        """Return the schedule id"""
        return self._id

    @property
    def start_date_time(self):
        """Return the schedule start time"""
        return self._start_date_time

    @property
    def end_date_time(self):
        """Return the schedule end time"""
        return self._end_date_time

    @property
    def enabled(self):
        """Return whether the schedule is enabled or not"""
        return self._enabled

    @property
    def cron_expression(self):
        """Return the schedule cron expression"""
        return self._cron_expression

    @property
    def next_execution_date_time(self):
        """Return the next execution time"""
        return self._next_execution_date_time
