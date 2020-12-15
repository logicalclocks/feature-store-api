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
from hsfs.constructor import filter


class Feature:
    """Metadata object representing a feature in a feature group in the Feature Store.

    See Training Dataset Feature for the
    feature representation of training dataset schemas.
    """

    def __init__(
        self,
        name,
        type=None,
        description=None,
        primary=None,
        partition=None,
        hudi_precombine_key=None,
        online_type=None,
        default_value=None,
        feature_group_id=None,
        feature_group=None,
    ):
        self._name = name
        self._type = type
        self._description = description
        self._primary = primary or False
        self._partition = partition or False
        self._hudi_precombine_key = hudi_precombine_key or False
        self._online_type = online_type
        self._default_value = default_value
        if feature_group is not None:
            self._feature_group_id = feature_group.id
        else:
            self._feature_group_id = feature_group_id

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
            "partition": self._partition,
            "hudiPrecombineKey": self._hudi_precombine_key,
            "primary": self._primary,
            "onlineType": self._online_type,
            "defaultValue": self._default_value,
            "featureGroupId": self._feature_group_id,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def name(self):
        """Name of the feature."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def type(self):
        """Data type of the feature in the offline feature store.

        !!! danger "Not a Python type"
            This type property is not to be confused with Python types.
            The type property represents the actual data type of the feature in
            the feature store.
        """
        return self._type

    @type.setter
    def type(self, type):
        self._type = type

    @property
    def online_type(self):
        """Data type of the feature in the online feature store."""
        return self._online_type

    @online_type.setter
    def online_type(self, online_type):
        self._online_type = online_type

    @property
    def primary(self):
        """Whether the feature is part of the primary key of the feature group."""
        return self._primary

    @primary.setter
    def primary(self, primary):
        self._primary = primary

    @property
    def partition(self):
        """Whether the feature is part of the partition key of the feature group."""
        return self._partition

    @partition.setter
    def partition(self, partition):
        self._partition = partition

    @property
    def hudi_precombine_key(self):
        """Whether the feature is part of the hudi precombine key of the feature group."""
        return self._hudi_precombine_key

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key):
        self._hudi_precombine_key = hudi_precombine_key

    @property
    def default_value(self):
        """Default value of the feature as string, if the feature was appended to the
        feature group."""
        return self._default_value

    @default_value.setter
    def default_value(self, default_value):
        self._default_value = default_value

    def __lt__(self, other):
        return filter.Filter(self, filter.Filter.LT, other)

    def __le__(self, other):
        return filter.Filter(self, filter.Filter.LE, other)

    def __eq__(self, other):
        return filter.Filter(self, filter.Filter.EQ, other)

    def __ne__(self, other):
        return filter.Filter(self, filter.Filter.NE, other)

    def __ge__(self, other):
        return filter.Filter(self, filter.Filter.GE, other)

    def __gt__(self, other):
        return filter.Filter(self, filter.Filter.GT, other)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Feature({self._name!r}, {self._type!r}, {self._description!r}, {self._primary}, {self._partition}, {self._online_type!r}, {self._default_value!r}, {self._feature_group_id!r})"
