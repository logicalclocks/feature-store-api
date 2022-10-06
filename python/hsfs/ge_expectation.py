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
from typing import Any, Dict, Optional
from great_expectations.core import ExpectationConfiguration

from hsfs import util


class GeExpectation:
    """Metadata object representing an feature validation expectation in the Feature Store."""

    def __init__(
        self,
        expectation_type : str,
        kwargs : Dict[str, Any],
        meta : Dict[str, Any],
        id: Optional[int]=None,
        href=None
    ):
        self._id = id
        self._expectation_type = expectation_type
        self.kwargs = kwargs
        self.meta = meta

        if not self.id:
            if "expectationId" in self.meta.keys():
                self.id = int(self.meta["expectationId"])
                
    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**expectation_suite)
                for expectation_suite in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def to_dict(self) -> Dict[str, Any]:
        return {
            # "id": self._id,
            "expectationType": self._expectation_type,
            "kwargs": json.dumps(self._kwargs),
            "meta": json.dumps(self._meta),
        }

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            # "id": self._id,
            "expectationType": self._expectation_type,
            "kwargs": self._kwargs,
            "meta": self._meta,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Expectation({self._expectation_type}, {self._kwargs}, {self._meta})"

    def to_ge_type(self) -> ExpectationConfiguration:
        return ExpectationConfiguration(
            expectation_type=self.expectation_type, kwargs=self.kwargs, meta=self.meta
        )

    @property
    def id(self) -> int:
        """Id of the expectation, set by backend."""
        if self._id:
            return self._id
        else:
            if "expectationId" in self._meta.keys():
                self._id = self._meta["expectationId"]
                return self._id
            else:
                return None

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def expectation_type(self) -> str:
        """Type of the expectation."""
        return self._expectation_type

    @expectation_type.setter
    def expectation_type(self, expectation_type):
        self._expectation_type = expectation_type

    @property
    def kwargs(self) -> Dict[str, Any]:
        """Kwargs to run the expectation."""
        return self._kwargs

    @kwargs.setter
    def kwargs(self, kwargs):
        if isinstance(kwargs, dict):
            self._kwargs = kwargs
        elif isinstance(kwargs, str):
            self._kwargs = json.loads(kwargs)
        else:
            raise ValueError("Kwargs field must be stringified json or dict.")

    @property
    def meta(self) -> Dict[str, Any]:
        """Meta field of the expectation to store additional information."""
        return self._meta

    @meta.setter
    def meta(self, meta):
        if isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")
