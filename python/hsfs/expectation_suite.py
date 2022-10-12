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
from typing import Optional, Union, List, Dict, Any
import re

import humps
import great_expectations as ge

from hsfs import util
from hsfs.ge_expectation import GeExpectation
from hsfs.core.expectation_engine import ExpectationEngine
from hsfs.core import expectation_suite_engine
from hsfs.client.exceptions import FeatureStoreException


class ExpectationSuite:
    """Metadata object representing an feature validation expectation in the Feature Store."""

    def __init__(
        self,
        expectation_suite_name: str,
        expectations: List[
            Union[ge.core.ExpectationConfiguration, dict, GeExpectation]
        ],
        meta: Dict[str, Any],
        id: Optional[int] = None,
        data_asset_type=None,
        ge_cloud_id=None,
        run_validation: bool = True,
        validation_ingestion_policy: str = "ALWAYS",
        feature_store_id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
        href: Optional[str] = None,
        expand=None,
        items=None,
        count=None,
        type=None,
        created=None,
    ):
        self._id = id
        # Empty object to set the fields and avoid errors in setters
        self._ge_object = ge.core.ExpectationSuite("empty_suite")
        self._expectation_suite_name = expectation_suite_name
        self._ge_cloud_id = ge_cloud_id
        self._data_asset_type = data_asset_type
        self._run_validation = run_validation
        self._validation_ingestion_policy = validation_ingestion_policy.upper()
        self._expectations = []
        self._href = href

        if href is not None:
            self._init_feature_store_and_feature_group_ids_from_href(href)
        else:
            self._feature_store_id = feature_store_id
            self._feature_group_id = feature_group_id

        # use setters because these need to be transformed from stringified json
        self.expectations = expectations
        self.meta = meta

        if self.id:
            self._expectation_engine = ExpectationEngine(
                feature_store_id=self._feature_store_id,
                feature_group_id=self._feature_group_id,
                expectation_suite_id=self.id,
            )
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=self._feature_store_id,
                    feature_group_id=self._feature_group_id,
                )
            )

        self._expectation_engine = None
        self._expectation_suite_engine = None
        self._ge_object = self.to_ge_type()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if (
            "count" in json_decamelized
        ):  # todo count is expected also when providing dict
            if json_decamelized["count"] == 0:
                return None  # todo sometimes empty list returns [] others None
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
        feature_store_id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
    ):
        return cls(
            **ge_expectation_suite.to_json_dict(),
            run_validation=run_validation,
            validation_ingestion_policy=validation_ingestion_policy,
            feature_group_id=feature_group_id,
            feature_store_id=feature_store_id,
        )

    def to_dict(self) -> dict:
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

    def to_json_dict(self) -> dict:
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

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_ge_type(self) -> ge.core.ExpectationSuite:
        return ge.core.ExpectationSuite(
            expectation_suite_name=self._expectation_suite_name,
            ge_cloud_id=self._ge_cloud_id,
            data_asset_type=self._data_asset_type,
            expectations=[
                expectation.to_ge_type() for expectation in self._expectations
            ],
            meta=self._meta,
        )

    def _init_feature_store_and_feature_group_ids_from_href(self, href: str) -> None:
        self._feature_store_id, self._feature_group_id = re.search(
            r"\/featurestores\/([0-9]+)\/featuregroups\/([0-9]+)\/expectationsuite*",
            href,
        ).groups(0)

    def _init_expectation_engine(
        self, feature_store_id: int, feature_group_id: int
    ) -> None:
        if self.id:
            self._expectation_engine = ExpectationEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                expectation_suite_id=self.id,
            )
        else:
            raise ValueError(
                "Initialise the Expectation Suite first by attaching to a Feature Group"
            )

    def _init_expectation_suite_engine(
        self, feature_store_id: Optional[int], feature_group_id: Optional[int]
    ) -> None:
        if feature_group_id and feature_store_id:
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=feature_store_id,
                    feature_group_id=feature_group_id,
                )
            )
        elif self._feature_group_id and self._feature_store_id:
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=self._feature_store_id,
                    feature_group_id=self._feature_group_id,
                )
            )
        else:
            raise ValueError(
                "Provide feature_store_id or feature_group_id to use the expectation suite API"
            )

    # Expectation Suite Convenience Operations
    def save(
        self,
        feature_store_id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
    ):
        """
        Attach suite to a Feature Group or persist edit to the backend.

        # Arguments
            feature_group_id: Id of the Feature Group to which the Expectation Suite should attach.
            feature_store_id: Id of the Feature Store in which the Feature Group is registered.

        # Raises
            `RestAPIException`
        """
        if self._expectation_suite_engine:
            self._expectation_suite_engine.save(self)
        elif feature_store_id and feature_group_id:
            self._feature_store_id = feature_store_id
            self._feature_group_id = feature_group_id
            self._init_expectation_suite_engine(
                feature_group_id=feature_group_id, feature_store_id=feature_store_id
            )
            self._expectation_suite_engine.save(self)
        elif self._feature_store_id and self._feature_group_id:
            self._init_expectation_suite_engine(
                feature_group_id=feature_group_id, feature_store_id=feature_store_id
            )
            self._expectation_suite_engine.save(self)
        else:
            raise ValueError(
                "Cannot attach or update the expectation suite without providing feature_store_id and feature_group_id."
            )

    def delete(self):
        """Detach Expectation Suite from its Feature Group. The suite still exist locally."""
        if self._expectation_suite_engine and self.id:
            self._expectation_suite_engine.delete(self.id)
        self.__del__()

    # Emulate GE single expectation api to edit list of expectations
    def _convert_expectation(
        self, expectation: Union[GeExpectation, ge.core.ExpectationConfiguration, dict]
    ) -> GeExpectation:
        """
        Convert different representation of expectation to Hopsworks GeExpectation type.

        # Arguments
            expectation: An expectation to convert to Hopsworks GeExpectation type

        # Returns
            An expectation converted to Hopsworks GeExpectation type

        # Raises
            `TypeError`
        """
        if isinstance(expectation, ge.core.ExpectationConfiguration):
            return GeExpectation(**expectation.to_json_dict())
        elif isinstance(expectation, GeExpectation):
            return expectation
        elif isinstance(expectation, dict):
            return GeExpectation(**expectation)
        else:
            raise TypeError(
                "Expectation of type {} is not supported.".format(type(expectation))
            )

    def get_expectation(
        self, expectation_id: int, ge_type: bool = True
    ) -> Union[GeExpectation, ge.core.ExpectationConfiguration]:
        """
        Fetch expectation with expectation_id from the backend.

        # Arguments
            expectation_id: Id of the expectation to fetch from the backend.
            ge_type: Whether to return native Great Expectations object or Hopsworks abstraction, defaults to True.

        # Returns
            The expectation with expectation_id registered in the backend.

        # Raises
            `RestAPIException`
            `FeatureStoreException`
        """
        if self.id and self._expectation_engine:
            if ge_type:
                return self._expectation_engine.get(expectation_id).to_ge_type()
            else:
                return self._expectation_engine.get(expectation_id)
        else:
            raise FeatureStoreException(
                "Initialize Expectation Suite by attaching to a Feature Group to enable single expectation API"
            )

    def add_expectation(
        self,
        expectation: Union[GeExpectation, ge.core.ExpectationConfiguration],
        ge_type: bool = True,
    ) -> Union[GeExpectation, ge.core.ExpectationConfiguration]:
        """
        Append an expectation to the local suite or in the backend if attached to a Feature Group.

        # Arguments
            expectation: The new expectation object.
            ge_type: Whether to return native Great Expectations object or Hopsworks abstraction, defaults to True.

        # Returns
            The new expectation attached to the Feature Group.

        # Raises
            `RestAPIException`
            `FeatureStoreException`
        """
        if self.id:
            converted_expectation = self._convert_expectation(expectation=expectation)
            converted_expectation = self._expectation_engine.create(
                expectation=converted_expectation
            )
            self.expectations = self._expectation_engine.get_expectations_by_suite_id(
                expectation_suite_id=self.id
            )
        else:
            raise FeatureStoreException(
                "Initialize Expectation Suite by attaching to a Feature Group to enable single expectation API"
            )

    def replace_expectation(
        self,
        expectation: Union[GeExpectation, ge.core.ExpectationConfiguration],
        ge_type: bool = True,
    ) -> Union[GeExpectation, ge.core.ExpectationConfiguration]:
        """
        Update an expectation from the suite locally or from the backend if attached to a Feature Group.

        # Arguments
            expectation: The updated expectation object. The meta field should contain an expectationId field.
            ge_type: Whether to return native Great Expectations object or Hopsworks abstraction, defaults to True.

        # Returns
            The updated expectation attached to the Feature Group.

        # Raises
            `RestAPIException`
            `FeatureStoreException`
        """
        if self.id:
            converted_expectation = self._convert_expectation(expectation=expectation)
            # To update an expectation we need an id either from meta field or from self.id
            self._expectation_engine.check_for_id(converted_expectation)
            converted_expectation = self._expectation_engine.update(
                expectation=converted_expectation
            )
            # Fetch the expectations from backend to avoid sync issues
            self.expectations = self._expectation_engine.get_expectations_by_suite_id(
                expectation_suite_id=self.id
            )

            if ge_type:
                return converted_expectation.to_ge_type()
            else:
                return converted_expectation
        else:
            raise FeatureStoreException(
                "Initialize Expectation Suite by attaching to a Feature Group to enable single expectation API"
            )

    def remove_expectation(self, expectation_id: Optional[int] = None) -> None:
        """
        Remove an expectation from the suite locally and from the backend if attached to a Feature Group.

        # Arguments
            expectation_id: Id of the expectation to remove. The expectation will be deleted both locally and from the backend.

        # Raises
            `RestAPIException`
            `FeatureStoreException`
        """
        if self.id:
            self._expectation_engine.delete(expectation_id=expectation_id)
            self.expectations = self._expectation_engine.get_expectations_by_suite_id(
                expectation_suite_id=self.id
            )
        else:
            raise FeatureStoreException(
                "Initialize Expectation Suite by attaching to a Feature Group to enable single expectation API"
            )

    # End of single expectation API

    def __str__(self) -> str:
        return self.json()

    def __repr__(self):
        return f"ExpectationSuite({self._expectation_suite_name}, {len(self._expectations)} expectations , {self._meta})"

    @property
    def id(self) -> Optional[int]:
        """Id of the expectation suite, set by backend."""
        return self._id

    @id.setter
    def id(self, id: int) -> int:
        self._id = id

    @property
    def expectation_suite_name(self) -> str:
        """Name of the expectation suite."""
        return self._expectation_suite_name

    @expectation_suite_name.setter
    def expectation_suite_name(self, expectation_suite_name: str):
        self._expectation_suite_name = expectation_suite_name
        self._ge_object.expectation_suite_name = self._expectation_suite_name

    @property
    def data_asset_type(self) -> str:
        """Data asset type of the expectation suite, not used by backend."""
        return self._data_asset_type

    @data_asset_type.setter
    def data_asset_type(self, data_asset_type: str):
        self._data_asset_type = data_asset_type
        self._ge_object.data_asset_type = self._data_asset_type

    @property
    def ge_cloud_id(self):
        """ge_cloud_id of the expectation suite, not used by backend."""
        return self._ge_cloud_id

    @ge_cloud_id.setter
    def ge_coud_id(self, ge_cloud_id):
        self._ge_cloud_id = ge_cloud_id
        self._ge_object.ge_cloud_id = ge_cloud_id

    @property
    def run_validation(self) -> bool:
        """Boolean to determine whether or not the expectation suite shoudl run on ingestion."""
        return self._run_validation

    @run_validation.setter
    def run_validation(self, run_validation: bool):
        self._run_validation = run_validation

    @property
    def validation_ingestion_policy(self) -> str:
        """Whether to ingest a df based on the validation result.

        "STRICT" : ingest df only if all expectations succeed,
        "ALWAYS" : always ingest df, even if one or more expectations fail
        """
        return self._validation_ingestion_policy

    @validation_ingestion_policy.setter
    def validation_ingestion_policy(self, validation_ingestion_policy: str):
        self._validation_ingestion_policy = validation_ingestion_policy.upper()

    @property
    def expectations(self) -> List[GeExpectation]:
        """List of expectations to run at validation."""
        return self._expectations

    @expectations.setter
    def expectations(
        self,
        expectations: Union[
            List[ge.core.ExpectationConfiguration], List[GeExpectation], List[dict]
        ],
    ):
        if expectations is None:
            self._expectations = []
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
                        f"Expectation of type {type(expectation)} is not supported."
                    )
        self._ge_object.expectations = [
            expec.to_ge_type() for expec in self._expectations
        ]

    @property
    def meta(self) -> dict:
        """Meta field of the expectation suite to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta: Union[str, dict]):
        if isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")
        self._ge_object.meta = self._meta
