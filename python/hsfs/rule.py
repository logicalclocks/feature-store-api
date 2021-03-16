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


class Rule:
    """Metadata object representing the validation rule that is used by feature group expectations.

    This class is made for hsfs internal use only.
    """

    def __init__(
        self,
        name: str,
        level,
        min=None,
        max=None,
        value=None,
        pattern=None,
        accepted_type=None,
        legal_values=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self.name = name
        self._level = level
        self._min = min
        self._max = max
        self._value = value
        self._pattern = pattern
        self._accepted_type = accepted_type
        self._legal_values = legal_values

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        return [cls(**rule) for rule in json_decamelized["items"]]

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "level": self._level,
            "min": self._min,
            "max": self._max,
            "value": self._value,
            "pattern": self._pattern,
            "acceptedType": self._accepted_type,
            "legalValues": self._legal_values,
        }

    @property
    def name(self):
        """Name of the rule as found in rule definitions."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name.upper()

    @property
    def level(self):
        """Severity level of a rule, one of "WARNING" or "ERROR"."""
        return self._level

    @level.setter
    def level(self, level):
        self._level = level

    @property
    def min(self):
        """The lower bound of the value range this feature should fall into."""
        return self._min

    @min.setter
    def min(self, min):
        self._min = min

    @property
    def max(self):
        """The upper bound of the value range this feature should fall into."""
        return self._max

    @max.setter
    def max(self, max):
        self._max = max

    @property
    def value(self):
        """The upper bound of the value range this feature should fall into."""
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def pattern(self):
        """Pattern to check for a feature's pattern compliance. Applicable only to the HAS_PATTERN rule."""
        return self._pattern

    @pattern.setter
    def pattern(self, pattern):
        self._pattern = pattern

    @property
    def accepted_type(self):
        """Data type accepted for a feature. Applicable only to the HAS_DATATYPE rule."""
        return self._accepted_type

    @accepted_type.setter
    def accepted_type(self, accepted_type):
        self._accepted_type = accepted_type

    @property
    def legal_values(self):
        """List of legal values a feature should be found int. feature.Applicable only to IS_CONTAINED_IN rule."""
        return self._legal_values

    @legal_values.setter
    def legal_values(self, legal_values):
        self._legal_values = legal_values
