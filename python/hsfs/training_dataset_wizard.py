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
import humps
from hsfs import util
from hsfs.core import training_dataset_wizard_api


class TrainingDatasetWizard:

    def __init__(
            self,
            name,
            label,
            feature_group_id,
            feature_store_id
    ):
        self._name = name
        self._label = label
        self._feature_group_id = feature_group_id
        self._feature_store_id = feature_store_id
        self._training_dataset_wizard_api = training_dataset_wizard_api.TrainingDatasetWizardApi(
            feature_store_id
        )

    def discover_related_featuregroups(self):
        return self._training_dataset_wizard_api.discover(self)

    def add_related_featuregroup(self, foreign_key, feature_group):
        pass

    def run_feature_selection(self):
        return self._training_dataset_wizard_api.featureselection(self)

    def create_training_dataset(self):
        pass

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "label": self._label,
            "feature_group_id": self._feature_group_id
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        # here we lose the information that the user set, e.g. write_options
        self.__init__(**json_decamelized)
        return self

    @property
    def name(self):
        """Name of the training dataset."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def label(self):
        """The label/prediction feature of the training dataset."""
        return self._label

    @label.setter
    def label(self, label):
        self._label = label

    @property
    def feature_group_id(self):
        """The id of the label's feature group."""
        return self._label

    @feature_group_id.setter
    def feature_group_id(self, feature_group_id):
        self._feature_group_id = feature_group_id