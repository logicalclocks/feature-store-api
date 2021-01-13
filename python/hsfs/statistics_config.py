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

import json
import humps

from hsfs import util


class StatisticsConfig:
    def __init__(
        self,
        enabled=True,
        correlations=None,
        histograms=None,
        columns=None,
    ):
        self._enabled = enabled
        # use setters for input validation
        self.correlations = correlations
        self.histograms = histograms
        self._columns = columns

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "enabled": self._enabled,
            "correlations": self._correlations,
            "histograms": self._histograms,
            "columns": self._columns,
        }

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, enabled):
        self._enabled = enabled

    @property
    def correlations(self):
        return self._correlations

    @correlations.setter
    def correlations(self, correlations):
        if correlations and not self._enabled:
            # do validation to fail fast, backend implements same logic
            raise ValueError(
                "Correlations can only be enabled with general statistics enabled. Set `enabled` in config to `True`."
            )
        self._correlations = correlations

    @property
    def histograms(self):
        return self._histograms

    @histograms.setter
    def histograms(self, histograms):
        if histograms and not self._enabled:
            # do validation to fail fast, backend implements same logic
            raise ValueError(
                "Histograms can only be enabled with general statistics enabled. Set `enabled` in config to `True`."
            )
        self._histograms = histograms

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = columns

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"StatisticsConfig({self._enabled}, {self._correlations}, {self._histograms}, {self._columns})"
