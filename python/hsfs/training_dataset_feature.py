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
from __future__ import annotations

import humps
from hsfs import feature as feature_mod
from hsfs import feature_group as feature_group_mod
from hsfs import util


class TrainingDatasetFeature:
    def __init__(
        self,
        name,
        type=None,
        index=None,
        featuregroup=None,
        feature_group_feature_name=None,
        label=False,
        inference_helper_column=False,
        training_helper_column=False,
        **kwargs,
    ):
        self._name = util.autofix_feature_name(name)
        self._type = type
        self._index = index
        self._feature_group = (
            feature_group_mod.FeatureGroup.from_response_json(featuregroup)
            if isinstance(featuregroup, dict)
            else featuregroup
        )
        self._feature_group_feature_name = feature_group_feature_name
        self._label = label
        self._inference_helper_column = inference_helper_column
        self._training_helper_column = training_helper_column

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "index": self._index,
            "label": self._label,
            "inferenceHelperColumn": self._inference_helper_column,
            "trainingHelperColumn": self._training_helper_column,
            "featureGroupFeatureName": self._feature_group_feature_name,
            "featuregroup": self._feature_group,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def is_complex(self):
        """Returns true if the feature has a complex type."""
        return any(
            map(str(self._type).upper().startswith, feature_mod.Feature.COMPLEX_TYPES)
        )

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
    def inference_helper_column(self):
        """Indicator if it is feature."""
        return self._inference_helper_column

    @inference_helper_column.setter
    def inference_helper_column(self, inference_helper_column):
        self._inference_helper_column = inference_helper_column

    @property
    def training_helper_column(self):
        """Indicator if it is feature."""
        return self._training_helper_column

    @training_helper_column.setter
    def training_helper_column(self, training_helper_column):
        self._training_helper_column = training_helper_column

    @property
    def feature_group(self):
        return self._feature_group

    @property
    def feature_group_feature_name(self):
        return self._feature_group_feature_name

    def __repr__(self):
        return f"Training Dataset Feature({self._name!r}, {self._type!r}, {self._index!r}, {self._label}, {self._feature_group_feature_name}, {self._feature_group.id!r})"
