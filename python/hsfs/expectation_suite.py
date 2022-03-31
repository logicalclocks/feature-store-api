#
#   Copyright 2022 Logical Clocks AB
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
from hsfs.core import expectation_suite_engine


class ExpectationSuite:
    """Metadata object representing an feature validation expectation in the Feature Store."""

    def __init__(
        self,
        id,
        expectation_suite_name,
        expectations,
        meta,
        featurestore_id=None,
        featuregroup_id=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        created=None,
    ):
        self._id = id
        self._expectation_suite_name = expectation_suite_name
        self._expectations = expectations
        self._meta = meta
        self._featurestore_id = featurestore_id
        self._featuregroup_id = featuregroup_id

    def save(self):
        """Persist the expectation metadata object to the feature store."""
        expectation_suite_engine.ExpectationSuiteEngine(self._featurestore_id, self._featuregroup_id).save(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**expectation_suite) for expectation_suite in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "expectation_suite_name": self.expectation_suite_name,
            "expectations": self._expectations,
            "meta": self._meta,
        }

    @property
    def id(self):
        """Id of the expectation suite, set by backend."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def expectation_suite_name(self):
        """Name of the expectation suite."""
        return self._expectation_suite_name

    @expectation_suite_name.setter
    def expectation_suite_name(self, expectation_suite_name):
        self._expectation_suite_name = expectation_suite_name


    @property
    def expectations(self):
        """List of expectations to run at validation."""
        return self._expectations

    @expectations.setter
    def expectations(self, expectations):
        self._expectations = expectations

    @property
    def meta(self):
        """Meta field of the expectation suite to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta):
        self._meta = meta

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"ExpectationSuite({self._expectation_suite_name}, {len(self._expectations)} expectations , {self._meta})"
        )