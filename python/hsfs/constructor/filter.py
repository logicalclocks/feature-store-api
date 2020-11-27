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

from hsfs import util


class Filter:
    GE = "GREATER_THAN_OR_EQUAL"
    GT = "GREATER_THAN"
    NE = "NOT_EQUALS"
    EQ = "EQUALS"
    LE = "LESS_THAN_OR_EQUAL"
    LT = "LESS_THAN"
    AND = "AND"
    OR = "OR"

    def __init__(self, feature, condition, value, logic=None, right_filter=None):
        self._feature = feature
        self._condition = condition
        self._value = value
        self._logic = logic
        self._right_filter = right_filter

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "feature": self._feature,
            "condition": self._condition,
            "value": str(self._value),
            "rightFilter": self._right_filter,
            "logic": self._logic,
        }

    def __and__(self, other):
        if not isinstance(other, Filter):
            raise TypeError(
                "Operator `&` expected type `Filter`, got `{}`".format(type(other))
            )
        if self._right_filter is None:
            self._right_filter = other
            self._logic = self.AND
        else:
            self._right_filter.__and__(other)
        return self

    def __or__(self, other):
        if not isinstance(other, Filter):
            raise TypeError(
                "Operator `|` expected type `Filter`, got `{}`".format(type(other))
            )
        self._right_filter = other
        self._logic = self.OR
        return self

    def __repr__(self):
        return f"Filter({self._feature!r}, {self._condition!r}, {self._value!r}, {self._right_filter!r}, {self._logic!r})"

    def __str__(self):
        return self.json()
