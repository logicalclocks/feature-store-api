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


class RuleDefinition:
    """Metadata object representing the validation rule that is used by feature group expectations.

    The set of rule definitions, for example "has max", "has avg" is provided by hsfs and cannot be modified.
    """

    def __init__(
        self,
        name,
        predicate,
        accepted_type,
        description,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._name = name
        self._predicate = predicate
        self._accepted_type = accepted_type
        self._description = description

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**ruledefinition) for ruledefinition in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "predicate": self._predicate,
            "acceptedType": self._accepted_type,
            "description": self._description,
        }

    @property
    def name(self):
        """Name of the rule definition. Unique across all features stores."""
        return self._name

    @property
    def predicate(self):
        """Predicate of the rule definition, one of "VALUE", "LEGAL_VALUES", "ACCEPTED_TYPE", "PATTERN"."""
        return self._predicate

    @property
    def accepted_type(self):
        """The type of the feature, one of "Null", "Fractional", "Integral", "Boolean", "String"."""
        return self._accepted_type

    @property
    def description(self):
        return self._description
