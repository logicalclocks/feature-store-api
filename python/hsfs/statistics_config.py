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
        correlations=False,
        histograms=False,
        exact_uniqueness=False,
        columns=[],
        **kwargs,
    ):
        self._enabled = enabled
        # use setters for input validation
        self.correlations = correlations
        self.histograms = histograms
        self.exact_uniqueness = exact_uniqueness
        # overwrite default with new empty [] but keep the empty list default for documentation
        self._columns = columns or []

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
            "exactUniqueness": self._exact_uniqueness,
            "columns": self._columns,
        }

    @property
    def enabled(self):
        """Enable statistics, by default this computes only descriptive statistics."""
        return self._enabled

    @enabled.setter
    def enabled(self, enabled):
        self._enabled = enabled

    @property
    def correlations(self):
        """Enable correlations as an additional statistic to be computed for each
        feature pair."""
        return self._correlations

    @correlations.setter
    def correlations(self, correlations):
        self._correlations = correlations

    @property
    def histograms(self):
        """Enable histograms as an additional statistic to be computed for each
        feature."""
        return self._histograms

    @histograms.setter
    def histograms(self, histograms):
        self._histograms = histograms

    @property
    def exact_uniqueness(self):
        """Enable exact uniqueness as an additional statistic to be computed for each
        feature."""
        return self._exact_uniqueness

    @exact_uniqueness.setter
    def exact_uniqueness(self, exact_uniqueness):
        self._exact_uniqueness = exact_uniqueness

    @property
    def columns(self):
        """Specify a subset of columns to compute statistics for."""
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = columns

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"StatisticsConfig({self._enabled}, {self._correlations}, {self._histograms},"
            f" {self._exact_uniqueness},"
            f" {self._columns})"
        )
