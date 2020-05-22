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
    def __init__(
        self,
        name,
        type=None,
        description=None,
        primary=None,
        partition=None,
        online_type=None,
    ):
        self._name = name
        self._type = type
        self._description = description
        # TODO: imo should be managed by the backend
        self._primary = primary or False
        self._partition = partition or False
        self._online_type = online_type

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
            "partition": self._partition,
            "primary": self._primary,
            "onlineType": self._online_type,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def primary(self):
        return self._primary

    @primary.setter
    def primary(self, primary):
        self._primary = primary

    @property
    def partition(self):
        return self._partition

    @partition.setter
    def partition(self, partition):
        self._partition
