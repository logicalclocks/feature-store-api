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


class Tag:
    def __init__(
        self,
        name,
        value,
        schema=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._name = name
        self._value = value

    def to_dict(self):
        return {
            "name": self._name,
            "value": self._value,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized or json_decamelized["count"] == 0:
            return []
        return [cls(**tag) for tag in json_decamelized["items"]]

    @property
    def name(self):
        """Name of the tag."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def value(self):
        """Value of tag."""
        return self.value

    @value.setter
    def value(self, value):
        self._value = value

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Tag({self._name!r}, {self._value!r})"
