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
from hsfs.constructor.query import Query
from hsfs.core import training_dataset_wizard_api
from hsfs.core import training_dataset_wizard_engine
from hsfs.constructor.join_suggestion import JoinSuggestion

class TrainingDatasetWizard:

    def __init__(
            self,
            label,
            feature_group_id,
            feature_store_id,
            feature_store_name,
            accepted_suggestions,
            new_suggestions,
            #selected_features,
            current_round,
            min_relatedness
    ):
        self._label = label
        self._feature_group_id = feature_group_id
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name
        self._accepted_suggestions = [JoinSuggestion.from_response_json(v) for v in accepted_suggestions]
        self._new_suggestions = [JoinSuggestion.from_response_json(v) for v in new_suggestions]
        #self._selected_features = selected_features
        self._current_round = current_round
        self._min_relatedness = min_relatedness
        self._training_dataset_wizard_api = training_dataset_wizard_api.TrainingDatasetWizardApi(
            feature_store_id
        )
        self._training_dataset_wizard_engine = training_dataset_wizard_engine.TrainingDatasetWizardEngine(
            feature_store_id
        )

    def discover_related_featuregroups(self, min_relatedness = 0.80):
        self._min_relatedness = min_relatedness
        self._new_suggestions = []
        self._training_dataset_wizard_api.discover(self)
        return self._new_suggestions

    def accept_suggestion(self, suggestion):
        self._accepted_suggestions.append(suggestion)
        return self

    def get_query(self):
        return self._training_dataset_wizard_api.construct_query(self)

    def run_feature_selection(self, query: Query, num_selected_features: int):
        return self._training_dataset_wizard_engine.run_feature_selection(query, num_selected_features)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "label": self._label.to_dict(),
            "featureGroupId": self._feature_group_id,
            "featureStoreId": self._feature_store_id,
            "featureStoreName": self._feature_store_name,
            "acceptedSuggestions": [v.to_dict() for v in self._accepted_suggestions] if self._accepted_suggestions
            else [],
            "newSuggestions": [v.to_dict() for v in self._new_suggestions] if self._new_suggestions
            else [],
            #"selectedFeatures": self._selected_features,
            "currentRound": self._current_round,
            "minRelatedness": self._min_relatedness
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # here we lose the information that the user set, e.g. write_options
        self.__init__(**json_decamelized)
        return self

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
        return self._feature_group_id

    @feature_group_id.setter
    def feature_group_id(self, feature_group_id):
        self._feature_group_id = feature_group_id

    @property
    def accepted_suggestions(self):
        """The accepted join suggestions."""
        return self._accepted_suggestions

    @property
    def current_round(self):
        """The number of times .discover() was called."""
        return self._current_round
