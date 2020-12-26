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

    def __init__(self, feature, condition, value):
        self._feature = feature
        self._condition = condition
        self._value = value

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "feature": self._feature,
            "condition": self._condition,
            "value": str(self._value),
        }

    def __and__(self, other):
        if isinstance(other, Filter):
            return Logic.And(left_f=self, right_f=other)
        elif isinstance(other, Logic):
            return Logic.And(left_f=self, right_l=other)
        else:
            raise TypeError(
                "Operator `&` expected type `Filter` or `Logic`, got `{}`".format(
                    type(other)
                )
            )

    def __or__(self, other):
        if isinstance(other, Filter):
            return Logic.Or(left_f=self, right_f=other)
        elif isinstance(other, Logic):
            return Logic.Or(left_f=self, right_l=other)
        else:
            raise TypeError(
                "Operator `|` expected type `Filter` or `Logic`, got `{}`".format(
                    type(other)
                )
            )

    def __repr__(self):
        return f"Filter({self._feature!r}, {self._condition!r}, {self._value!r})"

    def __str__(self):
        return self.json()


class Logic:
    AND = "AND"
    OR = "OR"
    SINGLE = "SINGLE"

    def __init__(self, type, left_f=None, right_f=None, left_l=None, right_l=None):
        self._type = type
        self._left_f = left_f
        self._right_f = right_f
        self._left_l = left_l
        self._right_l = right_l

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "type": self._type,
            "leftFilter": self._left_f,
            "rightFilter": self._right_f,
            "leftLogic": self._left_l,
            "rightLogic": self._right_l,
        }

    @classmethod
    def And(cls, left_f=None, right_f=None, left_l=None, right_l=None):
        return cls(cls.AND, left_f, right_f, left_l, right_l)

    @classmethod
    def Or(cls, left_f=None, right_f=None, left_l=None, right_l=None):
        return cls(cls.OR, left_f, right_f, left_l, right_l)

    @classmethod
    def Single(cls, left_f):
        return cls(cls.SINGLE, left_f)

    def __and__(self, other):
        if isinstance(other, Filter):
            return Logic.And(left_l=self, right_f=other)
        elif isinstance(other, Logic):
            return Logic.And(left_l=self, right_l=other)
        else:
            raise TypeError(
                "Operator `&` expected type `Filter` or `Logic`, got `{}`".format(
                    type(other)
                )
            )

    def __or__(self, other):
        if isinstance(other, Filter):
            return Logic.Or(left_l=self, right_f=other)
        elif isinstance(other, Logic):
            return Logic.Or(left_l=self, right_l=other)
        else:
            raise TypeError(
                "Operator `|` expected type `Filter` or `Logic`, got `{}`".format(
                    type(other)
                )
            )

    def __repr__(self):
        return f"Logic({self._type!r}, {self._left_f!r}, {self._right_f!r}, {self._left_l!r}, {self._right_l!r})"

    def __str__(self):
        return self.json()
