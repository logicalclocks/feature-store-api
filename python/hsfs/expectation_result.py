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

import humps
import json

from hsfs import util


class ExpectationResult:
    """Metadata object representing the expectation results of the data into a Feature Group."""

    def __init__(
        self,
        expectation,
        results,
        status=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._status = status
        self._expectation = expectation
        self._results = results

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        return [
            cls(**expectation_result)
            for expectation_result in json_decamelized["items"]
        ]

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "status": self._status,
            "expectation": self._expectation,
            "results": self._results,
        }

    @property
    def status(self):
        """Status of the expectation after feature ingestion, one of "NONE", "SUCCESS", "WARNING", "FAILURE"."""
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    @property
    def expectation(self):
        """The expectation this result refers to."""
        return self._expectation

    @expectation.setter
    def expectation(self, expectation):
        self._expectation = expectation

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, results):
        """List of validation results, that is results for all feature-rule pairs."""
        self._results = results
