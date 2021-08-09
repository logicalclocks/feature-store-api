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
from hsfs.core import expectations_engine


class Expectation:
    """Metadata object representing an feature validation expectation in the Feature Store."""

    def __init__(
        self,
        name,
        features,
        rules,
        description=None,
        featurestore_id=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._name = name
        self._features = features
        self._rules = rules
        self._description = description
        self._featurestore_id = featurestore_id

    def save(self):
        """Persist the expectation metadata object to the feature store."""
        expectations_engine.ExpectationsEngine(self._featurestore_id).save(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**expectation) for expectation in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "description": self._description,
            "features": self._features,
            "rules": self._rules,
        }

    @property
    def name(self):
        """Name of the expectation, unique per feature store (project)."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def description(self):
        """Description of the expectation."""
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    @property
    def features(self):
        """Optional list of features this expectation is applied to. If no features are provided, the expectation
        will be applied to all the feature group features."""
        return self._features

    @features.setter
    def features(self, features):
        self._features = features

    @property
    def rules(self):
        """List of rules applied to the features of the expectation."""
        return self._rules

    @rules.setter
    def rules(self, rules):
        self._rules = rules
