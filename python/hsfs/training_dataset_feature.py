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

import hsfs.feature_group as feature_group
from hsfs.feature import Feature
from hsfs.transformation_function import TransformationFunction


class TrainingDatasetFeature:
    def __init__(
        self,
        name,
        type=None,
        index=None,
        featuregroup=None,
        feature_group_feature_name=None,
        label=False,
        transformation_function=None,
    ):
        self._name = name.lower()
        self._type = type
        self._index = index
        self._feature_group = (
            feature_group.FeatureGroup.from_response_json(featuregroup)
            if isinstance(featuregroup, dict)
            else featuregroup
        )
        self._feature_group_feature_name = feature_group_feature_name
        self._label = label
        self._transformation_function = (
            TransformationFunction.from_response_json(transformation_function)
            if isinstance(transformation_function, dict)
            else transformation_function
        )

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "index": self._index,
            "label": self._label,
            "transformationFunction": self._transformation_function,
            "featureGroupFeatureName": self._feature_group_feature_name,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def is_complex(self):
        """Returns true if the feature has a complex type."""
        return any(map(self._type.upper().startswith, Feature.COMPLEX_TYPES))

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

    @property
    def transformation_function(self):
        """Set transformation functions."""
        return self._transformation_function

    @transformation_function.setter
    def transformation_function(self, transformation_function):
        self._transformation_function = transformation_function
