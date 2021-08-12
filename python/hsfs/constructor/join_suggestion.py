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
from hsfs import util, feature


class JoinSuggestion:

    def __init__(
            self,
            left_on,
            right_featuregroup_id,
            right_featuregroup_name,
            right_on,
            relatedness,
            round
    ):
        #  this assumes that the data is coming from Hopsworks
        self._left_on = (
            [feature.Feature.from_response_json(feat) for feat in left_on]
            if left_on
            else None
        )
        self._right_on = (
            [feature.Feature.from_response_json(feat) for feat in right_on]
            if right_on
            else None
        )
        self._right_featuregroup_id = right_featuregroup_id
        self._right_featuregroup_name = right_featuregroup_name
        self._relatedness = relatedness
        self._round = round


    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "leftOn": [v.to_dict() for v in self._left_on],
            "rightFeaturegroupId": self._right_featuregroup_id,
            "rightFeaturegroupName": self._right_featuregroup_name,
            "rightOn": [v.to_dict() for v in self._right_on],
            "relatedness": self._relatedness,
            "round": self._round
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
    def left_on(self):
        """Names of the features."""
        return self._left_on

    @left_on.setter
    def left_on(self, left_on):
        self._left_on = left_on

    @property
    def other_featuregroup_id(self):
        """The feature group id of the related features."""
        return self._other_featuregroup_id

    @other_featuregroup_id.setter
    def label(self, other_featuregroup_id):
        self._other_featuregroup_id = other_featuregroup_id

    @property
    def other_featuregroup_name(self):
        """The feature group name of the related features."""
        return self._other_featuregroup_name

    @other_featuregroup_name.setter
    def label(self, other_featuregroup_name):
        self._other_featuregroup_name = other_featuregroup_name

    @property
    def right_on(self):
        """The names of the related features."""
        return self._right_on

    @right_on.setter
    def right_on(self, right_on):
        self._right_on = right_on

    @property
    def relatedness(self):
        """The relatedness to the other feature (on a 0-100 scale)."""
        return self._relatedness

    @relatedness.setter
    def relatedness(self, relatedness):
        self._relatedness = relatedness

    @property
    def round(self):
        """The round in which the join was suggested."""
        return self._round

    @round.setter
    def round(self, round):
        self._round = round