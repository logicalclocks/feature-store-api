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

import json
import humps
from typing import Optional
from hsfs.util import FeatureStoreEncoder


class WindowConfigType:
    INSERT = "INSERT"
    SNAPSHOT = "SNAPSHOT"
    BATCH = "BATCH"
    TRAINING_DATASET = "TRAINING_DATASET"
    SPECIFIC_VALUE = "SPECIFIC_VALUE"


class MonitoringWindowConfig:
    def __init__(
        self,
        id: Optional[int] = None,
        window_config_type: Optional[str] = WindowConfigType.SPECIFIC_VALUE,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        specific_id: Optional[int] = None,
        specific_value: Optional[float] = None,
        row_percentage: Optional[int] = None,
    ):
        self._id = id
        self._window_config_type = window_config_type
        self._time_offset = time_offset
        self._window_length = window_length
        self._specific_id = specific_id
        self._specific_value = specific_value
        self._row_percentage = row_percentage

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "windowConfigType": self._window_config_type,
            "timeOffset": self._time_offset,
            "windowLength": self._window_length,
            "specificId": self._specific_id,
            "specificValue": self._specific_value,
            "rowPercentage": self._row_percentage,
        }

    def json(self) -> str:
        return json.dumps(self, cls=FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return json.dumps(humps.decamelize(self.to_dict()), indent=2)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def window_config_type(self) -> Optional[str]:
        return self._window_config_type

    @property
    def time_offset(self) -> Optional[str]:
        return self._time_offset

    @property
    def window_length(self) -> Optional[str]:
        return self._window_length

    @property
    def specific_id(self) -> Optional[int]:
        return self._specific_id

    @property
    def specific_value(self) -> Optional[float]:
        return self._specific_value

    @property
    def row_percentage(self) -> Optional[int]:
        return self._row_percentage
