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


class Feature:
    """Metadata object representing a feature in a feature group in the Feature Store.

    See [Training Dataset Feature](generated/training_dataset_feature.md) for the
    feature representation of training dataset schemas.
    """

    def __init__(
        self,
        name,
        type=None,
        description=None,
        primary=None,
        partition=None,
        online_type=None,
        default_value=None,
    ):
        self._name = name
        self._type = type
        self._description = description
        self._primary = primary or False
        self._partition = partition or False
        self._online_type = online_type
        self._default_value = default_value

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
            "partition": self._partition,
            "primary": self._primary,
            "onlineType": self._online_type,
            "defaultValue": self._default_value,
        }

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
        """Data type of the feature in the feature store.

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
    def default_value(self):
        """Default value of the feature as string, if the feature was appended to the
        feature group."""
        return self._default_value

    @default_value.setter
    def default_value(self, default_value):
        self._default_value = default_value
