#
#   Copyright 2022 Logical Clocks AB
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
from hsfs import util
import humps


class TrainingDatasetSplit:

    TIME_SERIES_SPLIT = "TIME_SERIES_SPLIT"
    RANDOM_SPLIT = "RANDOM_SPLIT"
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"

    def __init__(
        self, name, split_type, percentage=None, start_time=None, end_time=None
    ):
        self._name = name
        self._percentage = percentage
        self._split_type = split_type
        self._start_time = start_time
        self._end_time = end_time

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def percentage(self):
        return self._percentage

    @percentage.setter
    def percentage(self, percentage):
        self._percentage = percentage

    @property
    def split_type(self):
        return self._split_type

    @split_type.setter
    def split_type(self, split_type):
        self._split_type = split_type

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, end_time):
        self._end_time = end_time

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "percentage": self._percentage,
            "splitType": self._split_type,
            "startTime": self._start_time,
            "endTime": self._end_time,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            name=json_decamelized["name"],
            split_type=json_decamelized.get(
                "split_type", TrainingDatasetSplit.RANDOM_SPLIT
            ),
            percentage=json_decamelized.get("percentage", None),
            start_time=json_decamelized.get("start_time", None),
            end_time=json_decamelized.get("end_time", None),
        )
