#
#  Copyright 2021. Logical Clocks AB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import json
import humps

from hsfs import util
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class SplitStatistics:
    def __init__(
        self,
        name,
        feature_descriptive_statistics,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        **kwargs
    ):
        self._name = name
        self._feature_descriptive_statistics = feature_descriptive_statistics
        self._feature_descriptive_statistics = [
            (
                FeatureDescriptiveStatistics.from_response_json(fds)
                if isinstance(fds, dict)
                else fds
            )
            for fds in feature_descriptive_statistics
        ]

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "name": self._name,
            "featureDescriptiveStatistics": [
                fds.to_dict() for fds in self._feature_descriptive_statistics
            ],
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def name(self):
        """Name of the training dataset split."""
        return self._name

    @property
    def feature_descriptive_statistics(self):
        """List of feature descriptive statistics."""
        return self._feature_descriptive_statistics
