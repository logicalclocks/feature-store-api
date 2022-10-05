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


class ExpectationSuite:
    """Metadata object representing an feature validation expectation in the Feature Store."""

    def __init__(
        self,
        expectation_suite_name : str,
        expectations : List[Union[ge.core.ExpectationConfiguration, dict, GeExpectation]],
        meta : Dict[str, Any],
        id : Optional[int]=None,
        data_asset_type=None,
        ge_cloud_id=None,
        run_validation: bool=True,
        validation_ingestion_policy: str="ALWAYS",
        feature_store_id : Optional[int]=None,
        feature_group_id : Optional[int]=None,
        href : Optional[str]=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        created=None,
    ):
        self._id = id
        self._expectation_suite_name = expectation_suite_name
        self._ge_cloud_id = ge_cloud_id
        self._data_asset_type = data_asset_type
        self._run_validation = run_validation
        self._validation_ingestion_policy = validation_ingestion_policy.upper()
        self._expectations = []
        self._href = href
        
        if (href != None):
            print("Populating feature_store_id and feature_group_id from href")
            self._init_feature_store_and_feature_group_ids_from_href(href)
            print(f"self._feature_store_id: {self._feature_store_id}, self._feature_group_id: {self._feature_group_id}")
        else:
            print(f"Not populating from href: fs_id {feature_store_id} and fg_id {feature_group_id}")
            self._feature_store_id = feature_store_id
            self._feature_group_id = feature_group_id

        # use setters because these need to be transformed from stringified json
        self.expectations = expectations
        self.meta = meta

        if self.id:
            print(f"Expectation suite init, suite has an id: {self.id}. Initialising engine with : ")
            print(f"self._feature_store_id : {self._feature_store_id}, self._feature_group_id: {self._feature_group_id}, expectation_suite_id: {self.id}")
            self._expectation_engine = ExpectationEngine(
                feature_store_id=self._feature_store_id,
                feature_group_id=self._feature_group_id,
                expectation_suite_id=self.id
            )

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
    ):
        return cls(
            **ge_expectation_suite.to_json_dict(),
            run_validation=run_validation,
            validation_ingestion_policy=validation_ingestion_policy,
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
        self._feature_store_id, self._feature_group_id = re.search(r"\/featurestores\/([0-9]+)\/featuregroups\/([0-9]+)\/expectationsuite*", href).groups(0)

    def _init_expectation_engine(self, feature_store_id: int, feature_group_id: int) -> None:
        print("init expectation engine of suite:")
        if self.id:
            print(f"feature_store_id:{feature_store_id}, feature_group_id:{feature_group_id}, expectation_suite_id: {self.id}")
            self._expectation_engine = ExpectationEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                expectation_suite_id=self.id
            )
        else:
            raise ValueError("Initialise the Expectation Suite first by attaching to a Feature Group")

    # Emulate GE single expectation api to edit list of expectations
    def _convert_expectation(self, expectation: Union[GeExpectation, ge.core.ExpectationConfiguration, dict]) -> GeExpectation:
        """Convert different representation of expectation to Hopsworks GeExpectation type.
        
        :param expectation: An expectation to convert to Hopsworks GeExpectation type
        :type Union[GeExpectation, ge.core.ExpectationConfiguration, dict]
        :return: An expectation converted to Hopsworks GeExpectation type
        :rtype `GeExpectation`
        """
        if isinstance(expectation, ge.core.ExpectationConfiguration):
            return GeExpectation(**expectation.to_json_dict())
        elif isinstance(expectation, GeExpectation):
            return expectation
        elif isinstance(expectation, dict):
            return GeExpectation(**expectation)
        else:
            raise TypeError(
                "Expectation of type {} is not supported.".format(
                    type(expectation)
                )
            )

    def add_expectation(self, expectation: Union[GeExpectation, ge.core.ExpectationConfiguration], ge_type: bool=True) -> Union[GeExpectation, ge.core.ExpectationConfiguration]:
        converted_expectation = self._convert_expectation(expectation=expectation)
        if self.id:
            converted_expectation = self._expectation_engine.create(expectation=converted_expectation)
            self.expectations.append(converted_expectation)
            if ge_type:
                return converted_expectation.to_ge_type()
            else:
                return converted_expectation
        else:
            self._ge_object.add_expectation(converted_expectation.to_ge_type())
            self.expectations = self._ge_object.expectations

            

    def replace_expectation(self, expectation : Union[GeExpectation, ge.core.ExpectationConfiguration], ge_type : bool=True) -> Union[GeExpectation, ge.core.ExpectationConfiguration]:
        converted_expectation = self._convert_expectation(expectation=expectation)
        if self.id:
            # To update an expectation we need an id either from meta field or from self.id
            self._expectation_engine.check_for_id(converted_expectation)
            converted_expectation = self._expectation_engine.update(expectation=converted_expectation)
            self._replace_expectation_local(converted_expectation)
        else:
            self._ge_object.replace_expectation(converted_expectation.to_ge_type())
            self.expectations = self._ge_object.expectations
        
        if ge_type:
            return converted_expectation.to_ge_type()
        else:
            return converted_expectation

    def _replace_expectation_local(self, expectation: GeExpectation) -> None:
        matches = [list_index for list_index, expec in enumerate(self._expectations) if expec.id == expectation.id]
        if len(matches) == 0:
            raise ValueError(f"""Expectation not found in local expectation suite based on id : {expectation.id}. 
            Fetch suite using fg.get_expectation_suite() to start over with correct ids.""")
        elif len(matches) > 1:
            raise ValueError(f"""Found multiple expectations in local expectation suite based on id : {expectation.id}. 
            Fetch suite using fg.get_expectation_suite() to start over with correct ids.""")
        else:
            self.expectations[matches[0]] = expectation
        
    def remove_expectation(self, expectation_id : Optional[int], expectation: Union[ge.core.ExpectationConfiguration, GeExpectation, None]) -> None:
        if self.id and expectation_id:
            self._expectation_engine.delete(expectation_id=expectation_id)
            self._remove_expectation_local(expectation_id=expectation_id)
        elif self.id and expectation:
            converted_expectation = self._convert_expectation(expectation)
            self._expectation_engine.delete(converted_expectation.id)
            self._remove_expectation_local(expectation_id=converted_expectation.id)
        else:
            self._ge_object.remove_expectation(expectation)
            self.expectations = self._ge_object.expectations

    def _remove_expectation_local(self, expectation_id: int) -> None:
        matches = [list_index for list_index, expec in enumerate(self._expectations) if expec.id == expectation_id]
        if len(matches) == 0:
            raise ValueError(f"""Expectation not found in local expectation suite based on id : {expectation_id}. 
            Fetch suite using fg.get_expectation_suite() to start over with correct ids.""")
        elif len(matches) > 1:
            raise ValueError(f"""Found multiple expectations in local expectation suite based on id : {expectation_id}. 
            Fetch suite using fg.get_expectation_suite() to start over with correct ids.""")
        else:
            self.expectations.pop(matches[0])
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

    @property
    def data_asset_type(self) -> str:
        """Data asset type of the expectation suite, not used by backend."""
        return self._data_asset_type

    @data_asset_type.setter
    def data_asset_type(self, data_asset_type: str):
        self._data_asset_type = data_asset_type

    @property
    def ge_cloud_id(self):
        """ge_cloud_id of the expectation suite, not used by backend."""
        return self._ge_cloud_id

    @ge_cloud_id.setter
    def ge_coud_id(self, ge_cloud_id):
        self._ge_cloud_id = ge_cloud_id

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
    def expectations(self, expectations: Union[List[ge.core.ExpectationConfiguration], List[GeExpectation], List[dict]]):
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
                        f"Expectation of type {type(expectation)} is not supported."
                    )

    @property
    def meta(self) -> dict:
        """Meta field of the expectation suite to store additional informations."""
        return self._meta

    @meta.setter
    def meta(self, meta : Union[str, dict]):
        if isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")

