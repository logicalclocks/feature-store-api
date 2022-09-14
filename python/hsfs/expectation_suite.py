#
#   Copyright 2022 Hopsworks AB
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
from typing import Optional

import humps
import great_expectations as ge

from hsfs import util
from hsfs.ge_expectation import GeExpectation


class ExpectationSuite:
    """Metadata object representing an feature validation expectation in the Feature Store."""

    def __init__(
        self,
        expectation_suite_name,
        expectations,
        meta,
        id=None,
        data_asset_type=None,
        ge_cloud_id=None,
        run_validation=True,
        validation_ingestion_policy="ALWAYS",
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
        self._featurestore_id = featurestore_id
        self._featuregroup_id = featuregroup_id
        self._ge_cloud_id = ge_cloud_id
        self._data_asset_type = data_asset_type
        self._run_validation = run_validation
        self._validation_ingestion_policy = validation_ingestion_policy.upper()
        self._expectations = []

        # use setters because these need to be transformed from stringified json
        self.expectations = expectations
        self.meta = meta

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return None
            return [
                cls(**expectation_suite)
                for expectation_suite in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    @classmethod
    def from_ge_type(
        cls,
        ge_expectation_suite: ge.core.ExpectationSuite,
        run_validation: Optional[bool] = True,
        validation_ingestion_policy: Optional[str] = "ALWAYS",
    ):
        return cls(
            **ge_expectation_suite.to_json_dict(),
            run_validation=run_validation,
            validation_ingestion_policy=validation_ingestion_policy,
        )

    def to_dict(self):
        return {
            "id": self._id,
            "expectationSuiteName": self._expectation_suite_name,
            "expectations": self._expectations,
            "meta": json.dumps(self._meta),
            "geCloudId": self._ge_cloud_id,
            "dataAssetType": self._data_asset_type,
            "runValidation": self._run_validation,
            "validationIngestionPolicy": self._validation_ingestion_policy,
        }

    def to_json_dict(self):
        return {
            "id": self._id,
            "expectationSuiteName": self._expectation_suite_name,
            "expectations": [
                expectation.to_json_dict() for expectation in self._expectations
            ],
            "meta": self._meta,
            "geCloudId": self._ge_cloud_id,
            "dataAssetType": self._data_asset_type,
            "runValidation": self._run_validation,
            "validationIngestionPolicy": self._validation_ingestion_policy,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_ge_type(self):
        return ge.core.ExpectationSuite(
            expectation_suite_name=self._expectation_suite_name,
            ge_cloud_id=self._ge_cloud_id,
            data_asset_type=self._data_asset_type,
            expectations=[
                expectation.to_ge_type() for expectation in self._expectations
            ],
            meta=self._meta,
        )

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"ExpectationSuite({self._expectation_suite_name}, {len(self._expectations)} expectations , {self._meta})"

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
    def data_asset_type(self):
        """Data asset type of the expectation suite, not used by backend."""
        return self._data_asset_type

    @data_asset_type.setter
    def data_asset_type(self, data_asset_type):
        self._data_asset_type = data_asset_type

    @property
    def ge_cloud_id(self):
        """ge_cloud_id of the expectation suite, not used by backend."""
        return self._ge_cloud_id

    @ge_cloud_id.setter
    def ge_coud_id(self, ge_cloud_id):
        self._ge_cloud_id = ge_cloud_id

    @property
    def run_validation(self):
        """Boolean to determine whether or not the expectation suite shoudl run on ingestion."""
        return self._run_validation

    @run_validation.setter
    def run_validation(self, run_validation):
        self._run_validation = run_validation

    @property
    def validation_ingestion_policy(self):
        """Whether to ingest a df based on the validation result.

        "STRICT" : ingest df only if all expectations succeed,
        "ALWAYS" : always ingest df, even if one or more expectations fail
        """
        return self._validation_ingestion_policy

    @validation_ingestion_policy.setter
    def validation_ingestion_policy(self, validation_ingestion_policy):
        self._validation_ingestion_policy = validation_ingestion_policy.upper()

    @property
    def expectations(self):
        """List of expectations to run at validation."""
        return self._expectations

    @expectations.setter
    def expectations(self, expectations):
        if expectations is None:
            pass
        elif isinstance(expectations, list):
            for expectation in expectations:
                if isinstance(expectation, ge.core.ExpectationConfiguration):
                    self._expectations.append(
                        GeExpectation(**expectation.to_json_dict())
                    )
                elif isinstance(expectation, GeExpectation):
                    self._expectations.append(expectation)
                elif isinstance(expectation, dict):
                    self._expectations.append(GeExpectation(**expectation))
                else:
                    raise TypeError(
                        "Expectation of type {} is not supported.".format(
                            type(expectation)
                        )
                    )

    @property
    def meta(self):
        """Meta field of the expectation suite to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta):
        if isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")
