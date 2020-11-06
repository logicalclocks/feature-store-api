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


class TrainingDatasetFeature:
    def __init__(self, name, type, index=None, featuregroup=None, label=False):
        self._name = name
        self._type = type
        self._index = index
        self._featuregroup = featuregroup
        self._label = label

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "index": self._index,
            "label": self._label,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def name(self):
        """Name of the feature."""
        return self._name

    @property
    def type(self):
        """Data type of the feature in the feature store.

        !!! danger "Not a Python type"
            This type property is not to be confused with Python types.
            The type property represents the actual data type of the feature in
            the feature store.
        """
        return self._type

    @property
    def index(self):
        """Index of the feature in the training dataset, required to restore the correct
        order of features."""
        return self._index

    @property
    def label(self):
        """Indicator if the feature is part of the prediction label."""
        return self._label

    @label.setter
    def label(self, label):
        self._label = label
