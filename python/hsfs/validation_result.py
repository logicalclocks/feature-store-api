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


class ValidationResult:
    """Metadata object representing the validation result of a single rule of an expectation result of a Feature Group."""

    def __init__(
        self,
        status,
        message,
        value,
        features,
        rule,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._status = status
        self._message = message
        self._value = value
        self._features = features
        self._rule = rule

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        return [
            cls(**validation_result) for validation_result in json_decamelized["items"]
        ]

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "status": self._status,
            "message": self._message,
            "value": self._value,
            "features": self._features,
            "rule": self._rule,
        }

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        """Status of the expectation after feature ingestion, one of "NONE", "SUCCESS", "WARNING", "FAILURE"."""
        self._status = status

    @property
    def message(self):
        """Message describing the outcome of applying the rule against the feature."""
        return self._message

    @message.setter
    def message(self, message):
        self._message = message

    @property
    def value(self):
        """The computed value of the feature according to the rule."""
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def features(self):
        """Feature of the validation result on which the rule was applied."""
        return self._features

    @features.setter
    def features(self, features):
        self._features = features

    @property
    def rule(self):
        """Feature of the validation result on which the rule was applied."""
        return self._rule

    @rule.setter
    def rule(self, rule):
        self._rule = rule
