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

import copy
from hsfs.ge_validation_result import ValidationResult
import humps
import json
import warnings
import pandas as pd
import numpy as np
import great_expectations as ge
import avro.schema
from typing import Optional, Union, Any, Dict, List, TypeVar, Tuple

from datetime import datetime, date

from hsfs import util, engine, feature, user, storage_connector as sc
from hsfs.core import (
    feature_group_engine,
    statistics_engine,
    expectation_suite_engine,
    validation_report_engine,
    code_engine,
    external_feature_group_engine,
    validation_result_engine,
)

from hsfs.statistics_config import StatisticsConfig
from hsfs.expectation_suite import ExpectationSuite
from hsfs.validation_report import ValidationReport
from hsfs.constructor import query, filter
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.job import Job
from hsfs.core.variable_api import VariableApi
from hsfs.core import great_expectation_engine


class FeatureGroupBase:
    def __init__(self, featurestore_id, location, event_time=None):
        self.event_time = event_time
        self._location = location
        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )
        self._code_engine = code_engine.CodeEngine(featurestore_id, self.ENTITY_TYPE)
        self._great_expectation_engine = (
            great_expectation_engine.GreatExpectationEngine(featurestore_id)
        )
        self._feature_store_id = featurestore_id
        self._variable_api = VariableApi()

    def delete(self):
        """Drop the entire feature group along with its feature data.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(
                    name='bitcoin_price',
                    version=1
                    )

            # delete the feature group
            fg.delete()
            ```

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature group **and** all the feature data in offline and online storage
            associated with it.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        warnings.warn(
            "All jobs associated to feature group `{}`, version `{}` will be removed.".format(
                self._name, self._version
            ),
            util.JobWarning,
        )
        self._feature_group_engine.delete(self)

    def select_all(
        self,
        include_primary_key: Optional[bool] = True,
        include_event_time: Optional[bool] = True,
    ):
        """Select all features in the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a
        feature view.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instances
            fg1 = fs.get_or_create_feature_group(...)
            fg2 = fs.get_or_create_feature_group(...)

            # construct the query
            query = fg1.select_all().join(fg2.select_all())

            # show first 5 rows
            query.show(5)


            # select all features exclude primary key and event time
            from hsfs.feature import Feature
            fg = fs.create_feature_group(
                    "fg",
                    features=[
                            Feature("id", type="string"),
                            Feature("ts", type="bigint"),
                            Feature("f1", type="date"),
                            Feature("f2", type="double")
                            ],
                    primary_key=["id"],
                    event_time="ts")

            query = fg.select_all()
            query.features
            # [Feature('id', ...), Feature('ts', ...), Feature('f1', ...), Feature('f2', ...)]

            query = fg.select_all(include_primary_key=False, include_event_time=False)
            query.features
            # [Feature('f1', ...), Feature('f2', ...)]
            ```

        # Arguments
            include_primary_key: If True, include primary key of the feature group
                to the feature list. Defaults to True.
            include_event_time: If True, include event time of the feature group
                to the feature list. Defaults to True.
        # Returns
            `Query`. A query object with all features of the feature group.
        """
        if include_event_time and include_primary_key:
            return query.Query(
                left_feature_group=self,
                left_features=self._features,
                feature_store_name=self._feature_store_name,
                feature_store_id=self._feature_store_id,
            )
        elif include_event_time:
            return self.select_except(self.primary_key)
        elif include_primary_key:
            return self.select_except([self.event_time])
        else:
            return self.select_except(self.primary_key + [self.event_time])

    def select(self, features: Optional[List[Union[str, feature.Feature]]] = []):
        """Select a subset of features of the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a
        feature view with a subset of features of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            from hsfs.feature import Feature
            fg = fs.create_feature_group(
                    "fg",
                    features=[
                            Feature("id", type="string"),
                            Feature("ts", type="bigint"),
                            Feature("f1", type="date"),
                            Feature("f2", type="double")
                            ],
                    primary_key=["id"],
                    event_time="ts")

            # construct query
            query = fg.select(["id", "f1"])
            query.features
            # [Feature('id', ...), Feature('f1', ...)]
            ```

        # Arguments
            features: A list of `Feature` objects or feature names as
                strings to be selected, defaults to [].

        # Returns
            `Query`: A query object with the selected features of the feature group.
        """
        return query.Query(
            left_feature_group=self,
            left_features=features,
            feature_store_name=self._feature_store_name,
            feature_store_id=self._feature_store_id,
        )

    def select_except(self, features: Optional[List[Union[str, feature.Feature]]] = []):
        """Select all features including primary key and event time feature
        of the feature group except provided `features` and return a query object.

        The query can be used to construct joins of feature groups or create a
        feature view with a subset of features of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            from hsfs.feature import Feature
            fg = fs.create_feature_group(
                    "fg",
                    features=[
                            Feature("id", type="string"),
                            Feature("ts", type="bigint"),
                            Feature("f1", type="date"),
                            Feature("f2", type="double")
                            ],
                    primary_key=["id"],
                    event_time="ts")

            # construct query
            query = fg.select_except(["ts", "f1"])
            query.features
            # [Feature('id', ...), Feature('f1', ...)]
            ```

        # Arguments
            features: A list of `Feature` objects or feature names as
                strings to be excluded from the selection. Defaults to [],
                selecting all features.

        # Returns
            `Query`: A query object with the selected features of the feature group.
        """
        if features:
            except_features = [
                f.name if isinstance(f, feature.Feature) else f for f in features
            ]
            return query.Query(
                left_feature_group=self,
                left_features=[
                    f for f in self._features if f.name not in except_features
                ],
                feature_store_name=self._feature_store_name,
                feature_store_id=self._feature_store_id,
            )
        else:
            return self.select_all()

    def filter(self, f: Union[filter.Filter, filter.Logic]):
        """Apply filter to the feature group.

        Selects all features and returns the resulting `Query` with the applied filter.
        !!! example
            ```python
            from hsfs.feature import Feature

            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.filter(Feature("weekly_sales") > 1000)
            ```

        If you are planning to join the filtered feature group later on with another
        feature group, make sure to select the filtered feature explicitly from the
        respective feature group:
        !!! example
            ```python
            fg.filter(fg.feature1 == 1).show(10)
            ```

        Composite filters require parenthesis:
        !!! example
            ```python
            fg.filter((fg.feature1 == 1) | (fg.feature2 >= 2))
            ```

        # Arguments
            f: Filter object.

        # Returns
            `Query`. The query object with the applied filter.
        """
        return self.select_all().filter(f)

    def add_tag(self, name: str, value):
        """Attach a tag to a feature group.

        A tag consists of a <name,value> pair. Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.add_tag(name="example_tag", value="42")
            ```

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to add the tag.
        """

        self._feature_group_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag attached to a feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.delete_tag("example_tag")
            ```

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to delete the tag.
        """
        self._feature_group_engine.delete_tag(self, name)

    def get_tag(self, name: str):
        """Get the tags of a feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_tag_value = fg.get_tag("example_tag")
            ```

        # Arguments
            name: Name of the tag to get.

        # Returns
            tag value

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to retrieve the tag.
        """
        return self._feature_group_engine.get_tag(self, name)

    def get_tags(self):
        """Retrieves all tags attached to a feature group.

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to retrieve the tags.
        """
        return self._feature_group_engine.get_tags(self)

    def get_parent_feature_groups(self):
        """Get the parents of this feature group, based on explicit provenance.
        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only a minimal information is
        returned.

        # Returns
            `ProvenanceLinks`: Object containing the section of provenance graph requested.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        return self._feature_group_engine.get_parent_feature_groups(self)

    def get_generated_feature_views(self):
        """Get the generated feature view using this feature group, based on explicit
        provenance. These feature views can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature view links, so deleted
        will always be empty.
        For inaccessible feature views, only a minimal information is returned.

        # Returns
            `ProvenanceLinks`: Object containing the section of provenance graph requested.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        return self._feature_group_engine.get_generated_feature_views(self)

    def get_generated_feature_groups(self):
        """Get the generated feature groups using this feature group, based on explicit
        provenance. These feature groups can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature group links, so deleted
        will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        # Returns
            `ProvenanceLinks`: Object containing the section of provenance graph requested.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        return self._feature_group_engine.get_generated_feature_groups(self)

    def get_feature(self, name: str):
        """Retrieve a `Feature` object from the schema of the feature group.

        There are several ways to access features of a feature group:

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # get Feature instanse
            fg.feature1
            fg["feature1"]
            fg.get_feature("feature1")
            ```

        !!! note
            Attribute access to features works only for non-reserved names. For example
            features named `id` or `name` will not be accessible via `fg.name`, instead
            this will return the name of the feature group itself. Fall back on using
            the `get_feature` method.

        # Arguments:
            name: The name of the feature to retrieve

        # Returns:
            Feature: The feature object

        # Raises
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        try:
            return self.__getitem__(name)
        except KeyError:
            raise FeatureStoreException(
                f"'FeatureGroup' object has no feature called '{name}'."
            )

    def update_statistics_config(self):
        """Update the statistics configuration of the feature group.

        Change the `statistics_config` object and persist the changes by calling
        this method.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_statistics_config()
            ```

        # Returns
            `FeatureGroup`. The updated metadata object of the feature group.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        self._feature_group_engine.update_statistics_config(self)
        return self

    def update_description(self, description: str):
        """Update the description of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_description(description="Much better description.")
            ```

        !!! info "Safe update"
            This method updates the feature group description safely. In case of failure
            your local metadata object will keep the old description.

        # Arguments
            description: New description string.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        self._feature_group_engine.update_description(self, description)
        return self

    def update_features(self, features: Union[feature.Feature, List[feature.Feature]]):
        """Update metadata of features in this feature group.

        Currently it's only supported to update the description of a feature.

        !!! danger "Unsafe update"
            Note that if you use an existing `Feature` object of the schema in the
            feature group metadata object, this might leave your metadata object in a
            corrupted state if the update fails.

        # Arguments
            features: `Feature` or list of features. A feature object or list thereof to
                be updated.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        new_features = []
        if isinstance(features, feature.Feature):
            new_features.append(features)
        elif isinstance(features, list):
            for feat in features:
                if isinstance(feat, feature.Feature):
                    new_features.append(feat)
                else:
                    raise TypeError(
                        "The argument `features` has to be of type `Feature` or "
                        "a list thereof, but an element is of type: `{}`".format(
                            type(features)
                        )
                    )
        else:
            raise TypeError(
                "The argument `features` has to be of type `Feature` or a list "
                "thereof, but is of type: `{}`".format(type(features))
            )
        self._feature_group_engine.update_features(self, new_features)
        return self

    def update_feature_description(self, feature_name: str, description: str):
        """Update the description of a single feature in this feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_feature_description(feature_name="min_temp",
                                          description="Much better feature description.")
            ```

        !!! info "Safe update"
            This method updates the feature description safely. In case of failure
            your local metadata object will keep the old description.

        # Arguments
            feature_name: Name of the feature to be updated.
            description: New description string.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        f_copy = copy.deepcopy(self[feature_name])
        f_copy.description = description
        self._feature_group_engine.update_features(self, [f_copy])
        return self

    def append_features(self, features: Union[feature.Feature, List[feature.Feature]]):
        """Append features to the schema of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # define features to be inserted in the feature group
            features = [
                Feature(name="id",type="int",online_type="int"),
                Feature(name="name",type="string",online_type="varchar(20)")
            ]

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.append_features(features)
            ```

        !!! info "Safe append"
            This method appends the features to the feature group description safely.
            In case of failure your local metadata object will contain the correct
            schema.

        It is only possible to append features to a feature group. Removing
        features is considered a breaking change.

        # Arguments
            features: Feature or list. A feature object or list thereof to append to
                the schema of the feature group.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        new_features = []
        if isinstance(features, feature.Feature):
            new_features.append(features)
        elif isinstance(features, list):
            for feat in features:
                if isinstance(feat, feature.Feature):
                    new_features.append(feat)
                else:
                    raise TypeError(
                        "The argument `features` has to be of type `Feature` or "
                        "a list thereof, but an element is of type: `{}`".format(
                            type(features)
                        )
                    )
        else:
            raise TypeError(
                "The argument `features` has to be of type `Feature` or a list "
                "thereof, but is of type: `{}`".format(type(features))
            )
        self._feature_group_engine.append_features(self, new_features)
        return self

    def get_expectation_suite(
        self, ge_type: bool = True
    ) -> Union[ExpectationSuite, ge.core.ExpectationSuite, None]:
        """Return the expectation suite attached to the feature group if it exists.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            exp_suite = fg.get_expectation_suite()
            ```

        # Arguments
            ge_type: If `True` returns a native Great Expectation type, Hopsworks
                custom type otherwise. Conversion can be performed via the `to_ge_type()`
                method on hopsworks type. Defaults to `True`.

        # Returns
            `ExpectationSuite`. The expectation suite attached to the feature group.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        # Avoid throwing an error if Feature Group not initialised.
        if self._id:
            self._expectation_suite = self._expectation_suite_engine.get()

        if self._expectation_suite is not None and ge_type is True:
            return self._expectation_suite.to_ge_type()
        else:
            return self._expectation_suite

    def save_expectation_suite(
        self,
        expectation_suite: Union[ExpectationSuite, ge.core.ExpectationSuite],
        run_validation: bool = True,
        validation_ingestion_policy: str = "ALWAYS",
        overwrite: bool = False,
    ) -> Union[ExpectationSuite, ge.core.ExpectationSuite]:
        """Attach an expectation suite to a feature group and saves it for future use. If an expectation
        suite is already attached, it is replaced. Note that the provided expectation suite is modified
        inplace to include expectationId fields.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.save_expectation_suite(expectation_suite, run_validation=True)
            ```

        # Arguments
            expectation_suite: The expectation suite to attach to the Feature Group.
            overwrite: If an Expectation Suite is already attached, overwrite it.
                The new suite will have its own validation history, but former reports are preserved.
            run_validation: Set whether the expectation_suite will run on ingestion
            validation_ingestion_policy: Set the policy for ingestion to the Feature Group.
                - "STRICT" only allows DataFrame passing validation to be inserted into Feature Group.
                - "ALWAYS" always insert the DataFrame to the Feature Group, irrespective of overall validation result.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        if isinstance(expectation_suite, ge.core.ExpectationSuite):
            tmp_expectation_suite = ExpectationSuite.from_ge_type(
                ge_expectation_suite=expectation_suite,
                run_validation=run_validation,
                validation_ingestion_policy=validation_ingestion_policy,
                feature_store_id=self._feature_store_id,
                feature_group_id=self._id,
            )
        elif isinstance(expectation_suite, ExpectationSuite):
            tmp_expectation_suite = expectation_suite.to_json_dict(decamelize=True)
            tmp_expectation_suite["feature_group_id"] = self._id
            tmp_expectation_suite["feature_store_id"] = self._feature_store_id
            tmp_expectation_suite = ExpectationSuite(**tmp_expectation_suite)
        else:
            raise TypeError(
                "The provided expectation suite type `{}` is not supported. Use Great Expectation `ExpectationSuite` or HSFS' own `ExpectationSuite` object.".format(
                    type(expectation_suite)
                )
            )

        if overwrite:
            self.delete_expectation_suite()

        if self._id:
            self._expectation_suite = self._expectation_suite_engine.save(
                tmp_expectation_suite
            )
            expectation_suite = self._expectation_suite.to_ge_type()
        else:
            # Added to avoid throwing an error if Feature Group is not initialised with the backend
            self._expectation_suite = tmp_expectation_suite

    def delete_expectation_suite(self) -> None:
        """Delete the expectation suite attached to the Feature Group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.delete_expectation_suite()
            ```

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        if self.get_expectation_suite() is not None:
            self._expectation_suite_engine.delete(self._expectation_suite.id)
        self._expectation_suite = None

    def get_latest_validation_report(
        self, ge_type: bool = True
    ) -> Union[ValidationReport, ge.core.ExpectationSuiteValidationResult, None]:
        """Return the latest validation report attached to the Feature Group if it exists.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            latest_val_report = fg.get_latest_validation_report()
            ```

        # Arguments
            ge_type: If `True` returns a native Great Expectation type, Hopsworks
                custom type otherwise. Conversion can be performed via the `to_ge_type()`
                method on hopsworks type. Defaults to `True`.

        # Returns
            `ValidationReport`. The latest validation report attached to the Feature Group.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        return self._validation_report_engine.get_last(ge_type=ge_type)

    def get_all_validation_reports(
        self, ge_type: bool = True
    ) -> List[Union[ValidationReport, ge.core.ExpectationSuiteValidationResult]]:
        """Return the latest validation report attached to the feature group if it exists.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            val_reports = fg.get_all_validation_reports()
            ```

        # Arguments
            ge_type: If `True` returns a native Great Expectation type, Hopsworks
                custom type otherwise. Conversion can be performed via the `to_ge_type()`
                method on hopsworks type. Defaults to `True`.

        # Returns
            Union[List[`ValidationReport`], `ValidationReport`]. All validation reports attached to the feature group.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        if self._id:
            return self._validation_report_engine.get_all(ge_type=ge_type)
        else:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch validation reports."
            )

    def save_validation_report(
        self,
        validation_report: Union[
            dict,
            ValidationReport,
            ge.core.expectation_validation_result.ExpectationSuiteValidationResult,
        ],
        ingestion_result: str = "UNKNOWN",
        ge_type: bool = True,
    ) -> Union[ValidationReport, ge.core.ExpectationSuiteValidationResult]:
        """Save validation report to hopsworks platform along previous reports of the same Feature Group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(..., expectation_suite=expectation_suite)

            validation_report = great_expectations.from_pandas(
                my_experimental_features_df,
                fg.get_expectation_suite()).validate()

            fg.save_validation_report(validation_report, ingestion_result="EXPERIMENT")
            ```

        # Arguments
            validation_report: The validation report to attach to the Feature Group.
            ingestion_result: Specify the fate of the associated data, defaults
                to "UNKNOWN". Supported options are  "UNKNOWN", "INGESTED", "REJECTED",
                "EXPERIMENT", "FG_DATA". Use "INGESTED" or "REJECTED" for validation
                of DataFrames to be inserted in the Feature Group. Use "EXPERIMENT"
                for testing and development and "FG_DATA" when validating data
                already in the Feature Group.
            ge_type: If `True` returns a native Great Expectation type, Hopsworks
                custom type otherwise. Conversion can be performed via the `to_ge_type()`
                method on hopsworks type. Defaults to `True`.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        if self._id:
            if isinstance(
                validation_report,
                ge.core.expectation_validation_result.ExpectationSuiteValidationResult,
            ):
                report = ValidationReport(
                    **validation_report.to_json_dict(),
                    ingestion_result=ingestion_result,
                )
            elif isinstance(validation_report, dict):
                report = ValidationReport(
                    **validation_report, ingestion_result=ingestion_result
                )
            elif isinstance(validation_report, ValidationReport):
                report = validation_report
                if ingestion_result != "UNKNOWN":
                    report.ingestion_result = ingestion_result

            return self._validation_report_engine.save(
                validation_report=report, ge_type=ge_type
            )
        else:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can upload validation reports."
            )

    def get_validation_history(
        self,
        expectation_id: int,
        start_validation_time: Union[str, int, datetime, date, None] = None,
        end_validation_time: Union[str, int, datetime, date, None] = None,
        filter_by: List[str] = [],
        ge_type: bool = True,
    ) -> Union[List[ValidationResult], List[ge.core.ExpectationValidationResult]]:
        """Fetch validation history of an Expectation specified by its id.

        !!! example
        ```python3
        validation_history = fg.get_validation_history(
            expectation_id=1,
            filter_by=["REJECTED", "UNKNOWN"],
            start_validation_time="2022-01-01 00:00:00",
            end_validation_time=datetime.datetime.now(),
            ge_type=False
        )
        ```

        # Arguments
            expectation_id: id of the Expectation for which to fetch the validation history
            filter_by: list of ingestion_result category to keep. Ooptions are "INGESTED", "REJECTED", "FG_DATA", "EXPERIMENT", "UNKNOWN".
            start_validation_time: fetch only validation result posterior to the provided time, inclusive.
            Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable. See examples above.
            end_validation_time: fetch only validation result prior to the provided time, inclusive.
            Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable. See examples above.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.

        # Return
            Union[List[`ValidationResult`], List[`ExpectationValidationResult`]] A list of validation result connected to the expectation_id
        """
        major, minor = self._variable_api.parse_major_and_minor(
            self._variable_api.get_version("hopsworks")
        )
        if major == "3" and minor == "0":
            raise FeatureStoreException(
                "The hopsworks server does not support this operation. Update server to hopsworks >3.1 to enable support."
            )

        if self._id:
            return self._validation_result_engine.get_validation_history(
                expectation_id=expectation_id,
                start_validation_time=start_validation_time,
                end_validation_time=end_validation_time,
                filter_by=filter_by,
                ge_type=ge_type,
            )
        else:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch validation history."
            )

    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except KeyError:
            raise AttributeError(
                f"'FeatureGroup' object has no attribute '{name}'. "
                "If you are trying to access a feature, fall back on "
                "using the `get_feature` method."
            )

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError(
                f"Expected type `str`, got `{type(name)}`. "
                "Features are accessible by name."
            )
        feature = [f for f in self.__getattribute__("_features") if f.name == name]
        if len(feature) == 1:
            return feature[0]
        else:
            raise KeyError(f"'FeatureGroup' object has no feature called '{name}'.")

    @property
    def statistics_config(self):
        """Statistics configuration object defining the settings for statistics
        computation of the feature group."""
        return self._statistics_config

    @statistics_config.setter
    def statistics_config(self, statistics_config):
        if isinstance(statistics_config, StatisticsConfig):
            self._statistics_config = statistics_config
        elif isinstance(statistics_config, dict):
            self._statistics_config = StatisticsConfig(**statistics_config)
        elif isinstance(statistics_config, bool):
            self._statistics_config = StatisticsConfig(statistics_config)
        elif statistics_config is None:
            self._statistics_config = StatisticsConfig()
        else:
            raise TypeError(
                "The argument `statistics_config` has to be `None` of type `StatisticsConfig, `bool` or `dict`, but is of type: `{}`".format(
                    type(statistics_config)
                )
            )

    @property
    def statistics(self):
        """Get the latest computed statistics for the feature group."""
        return self._statistics_engine.get_last(self)

    @property
    def primary_key(self):
        """List of features building the primary key."""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, new_primary_key):
        self._primary_key = [pk.lower() for pk in new_primary_key]

    def get_statistics(
        self, commit_time: Optional[Union[str, int, datetime, date]] = None
    ):
        """Returns the statistics for this feature group at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_statistics = fg.get_statistics(commit_time=None)
            ```

        # Arguments
            commit_time: Date and time of the commit. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.

        # Returns
            `Statistics`. Statistics object.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        if commit_time is None:
            return self.statistics
        else:
            return self._statistics_engine.get(self, commit_time)

    def compute_statistics(self):
        """Recompute the statistics for the feature group and save them to the
        feature store.
        Statistics are only computed for data in the offline storage of the feature
        group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            statistics_metadata = fg.compute_statistics()
            ```

        # Returns
            `Statistics`. The statistics metadata object.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. Unable to persist the statistics.
        """
        if self.statistics_config.enabled:
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Python engine. The Python engine is going to setup a Spark Job
            # to update the statistics.
            return self._statistics_engine.compute_statistics(self)
        else:
            warnings.warn(
                (
                    "The statistics are not enabled of feature group `{}`, with version"
                    " `{}`. No statistics computed."
                ).format(self._name, self._version),
                util.StorageWarning,
            )

    @property
    def event_time(self):
        """Event time feature in the feature group."""
        return self._event_time

    @event_time.setter
    def event_time(self, feature_name: Optional[str]):
        if feature_name is None:
            self._event_time = None
            return
        elif isinstance(feature_name, str):
            self._event_time = feature_name
            return
        elif isinstance(feature_name, list) and len(feature_name) == 1:
            if isinstance(feature_name[0], str):
                warnings.warn(
                    "Providing event_time as a single-element list is deprecated"
                    + " and will be dropped in future versions. Provide the feature_name string instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                self._event_time = feature_name[0]
                return

        raise ValueError(
            "event_time must be a string corresponding to an existing feature name of the Feature Group."
        )

    @property
    def location(self):
        return self._location

    @property
    def expectation_suite(
        self,
    ) -> Optional[ExpectationSuite]:
        """Expectation Suite configuration object defining the settings for
        data validation of the feature group."""
        return self._expectation_suite

    @expectation_suite.setter
    def expectation_suite(
        self,
        expectation_suite: Union[
            ExpectationSuite, ge.core.ExpectationSuite, dict, None
        ],
    ):
        if isinstance(expectation_suite, ExpectationSuite):
            tmp_expectation_suite = expectation_suite.to_json_dict()
            tmp_expectation_suite["featuregroup_id"] = self._id
            tmp_expectation_suite["featurestore_id"] = self._feature_store_id
            self._expectation_suite = ExpectationSuite(**tmp_expectation_suite)
        elif isinstance(expectation_suite, ge.core.expectation_suite.ExpectationSuite):
            self._expectation_suite = ExpectationSuite(
                **expectation_suite.to_json_dict(),
                feature_store_id=self._feature_store_id,
                feature_group_id=self._id,
            )
        elif isinstance(expectation_suite, dict):
            tmp_expectation_suite = expectation_suite.copy()
            tmp_expectation_suite["feature_store_id"] = self._feature_store_id
            tmp_expectation_suite["feature_group_id"] = self._id
            self._expectation_suite = ExpectationSuite(**tmp_expectation_suite)
        elif expectation_suite is None:
            self._expectation_suite = None
        else:
            raise TypeError(
                "The argument `expectation_suite` has to be `None` of type `ExpectationSuite` or `dict`, but is of type: `{}`".format(
                    type(expectation_suite)
                )
            )


class FeatureGroup(FeatureGroupBase):
    CACHED_FEATURE_GROUP = "CACHED_FEATURE_GROUP"
    STREAM_FEATURE_GROUP = "STREAM_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        name,
        version,
        featurestore_id,
        description="",
        partition_key=None,
        primary_key=None,
        hudi_precombine_key=None,
        featurestore_name=None,
        created=None,
        creator=None,
        id=None,
        features=None,
        location=None,
        online_enabled=False,
        time_travel_format=None,
        statistics_config=None,
        online_topic_name=None,
        event_time=None,
        stream=False,
        expectation_suite=None,
        parents=None,
        href=None,
    ):
        super().__init__(featurestore_id, location, event_time=event_time)

        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = user.User.from_response_json(creator)
        self._version = version
        self._name = name
        self._id = id
        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._online_enabled = online_enabled
        self._time_travel_format = (
            time_travel_format.upper() if time_travel_format is not None else None
        )

        self._subject = None
        self._online_topic_name = online_topic_name
        self._stream = stream
        self._parents = parents
        self._deltastreamer_jobconf = None

        if self._id:
            # initialized by backend
            self.primary_key = [
                feat.name for feat in self._features if feat.primary is True
            ]
            self._partition_key = [
                feat.name for feat in self._features if feat.partition is True
            ]
            if (
                time_travel_format is not None
                and time_travel_format.upper() == "HUDI"
                and self._features
            ):
                # hudi precombine key is always a single feature
                self._hudi_precombine_key = [
                    feat.name
                    for feat in self._features
                    if feat.hudi_precombine_key is True
                ][0]
            else:
                self._hudi_precombine_key = None

            self.statistics_config = statistics_config
            self.expectation_suite = expectation_suite
            if expectation_suite:
                self._expectation_suite._init_expectation_engine(
                    feature_store_id=featurestore_id, feature_group_id=self._id
                )
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=self._feature_store_id, feature_group_id=self._id
                )
            )
            self._validation_report_engine = (
                validation_report_engine.ValidationReportEngine(
                    self._feature_store_id, self._id
                )
            )
            self._validation_result_engine = (
                validation_result_engine.ValidationResultEngine(
                    self._feature_store_id, self._id
                )
            )

        else:
            # initialized by user
            # for python engine we always use stream feature group
            if engine.get_type() == "python":
                self._stream = True
            # for stream feature group time travel format is always HUDI
            if self._stream:
                expected_format = "HUDI"
                if self._time_travel_format != expected_format:
                    warnings.warn(
                        (
                            "The provided time travel format `{}` has been overwritten "
                            "because Stream enabled feature groups only support `{}`"
                        ).format(self._time_travel_format, expected_format),
                        util.FeatureGroupWarning,
                    )
                    self._time_travel_format = expected_format

            self.primary_key = primary_key
            self.partition_key = partition_key
            self._hudi_precombine_key = (
                hudi_precombine_key.lower()
                if hudi_precombine_key is not None
                and self._time_travel_format is not None
                and self._time_travel_format == "HUDI"
                else None
            )
            self.statistics_config = statistics_config
            self.expectation_suite = expectation_suite

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(
            featurestore_id
        )
        self._href = href

    def read(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        online: Optional[bool] = False,
        dataframe_type: Optional[str] = "default",
        read_options: Optional[dict] = {},
    ):
        """
        Read the feature group into a dataframe.

        Reads the feature group by default from the offline storage as Spark DataFrame
        on Hopsworks and Databricks, and as Pandas dataframe on AWS Sagemaker and pure
        Python environments.

        Set `online` to `True` to read from the online storage, or change
        `dataframe_type` to read as a different format.

        !!! example "Read feature group as of latest state:"
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)
            fg.read()
            ```

        !!! example "Read feature group as of specific point in time:"
            ```python
            fg = fs.get_or_create_feature_group(...)
            fg.read("2020-10-20 07:34:11")
            ```

        # Arguments
            wallclock_time: If specified will retrieve feature group as of specific point in time. Defaults to `None`.
                If not specified, will return as of most recent time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.
            online: bool, optional. If `True` read from online feature store, defaults
                to `False`.
            dataframe_type: str, optional. Possible values are `"default"`, `"spark"`,
                `"pandas"`, `"numpy"` or `"python"`, defaults to `"default"`.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                For python engine:
                * key `"hive_config"` to pass a dictionary of hive or tez configurations.
                  For example: `{"hive_config": {"hive.tez.cpu.vcores": 2, "tez.grouping.split-count": "3"}}`
                * key `"pandas_types"` and value `True` to retrieve columns as Pandas nullable types
                  rather than numpy/object(string) types (experimental).
                  (see https://pandas.pydata.org/docs/user_guide/integer_na.html).
                Defaults to `{}`.

        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`. A Spark DataFrame.
            `pandas.DataFrame`. A Pandas DataFrame.
            `numpy.ndarray`. A two-dimensional Numpy array.
            `list`. A two-dimensional Python list.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. No data is available for feature group with this commit date, If time travel enabled.
        """
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        if wallclock_time:
            return (
                self.select_all()
                .as_of(wallclock_time)
                .read(
                    online,
                    dataframe_type,
                    read_options,
                )
            )
        else:
            return self.select_all().read(
                online,
                dataframe_type,
                read_options,
            )

    def read_changes(
        self,
        start_wallclock_time: Union[str, int, datetime, date],
        end_wallclock_time: Union[str, int, datetime, date],
        read_options: Optional[dict] = {},
    ):
        """Reads updates of this feature that occurred between specified points in time.

        !!! warning "Deprecated"
                    `read_changes` method is deprecated. Use
                    `as_of(end_wallclock_time, exclude_until=start_wallclock_time).read(read_options=read_options)`
                    instead.

        This function only works on feature groups with `HUDI` time travel format.

        # Arguments
            start_wallclock_time: Start time of the time travel query. Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            end_wallclock_time: End time of the time travel query. Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                For python engine:
                * key `"hive_config"` to pass a dictionary of hive or tez configurations.
                  For example: `{"hive_config": {"hive.tez.cpu.vcores": 2, "tez.grouping.split-count": "3"}}`
                Defaults to `{}`.

        # Returns
            `DataFrame`. The spark dataframe containing the incremental changes of
            feature data.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.  No data is available for feature group with this commit date.
            `hsfs.client.exceptions.FeatureStoreException`. If the feature group does not have `HUDI` time travel format
        """
        return (
            self.select_all()
            .pull_changes(start_wallclock_time, end_wallclock_time)
            .read(False, "default", read_options)
        )

    def show(self, n: int, online: Optional[bool] = False):
        """Show the first `n` rows of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # make a query and show top 5 rows
            fg.select(['date','weekly_sales','is_holiday']).show(5)
            ```

        # Arguments
            n: int. Number of rows to show.
            online: bool, optional. If `True` read from online feature store, defaults
                to `False`.
        """
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().show(n, online)

    def save(
        self,
        features: Union[
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        write_options: Optional[Dict[Any, Any]] = {},
        validation_options: Optional[Dict[Any, Any]] = {},
    ):
        """Persist the metadata and materialize the feature group to the feature store.

        !!! warning "Deprecated"
            `save` method is deprecated. Use the `insert` method instead.

        Calling `save` creates the metadata for the feature group in the feature store
        and writes the specified `features` dataframe as feature group to the
        online/offline feature store as specified.
        By default, this writes the feature group to the offline storage, and if
        `online_enabled` for the feature group, also to the online feature store.
        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame,
        or a two-dimensional Numpy array or a two-dimensional Python nested list.
        # Arguments
            features: Query, DataFrame, RDD, Ndarray, list. Features to be saved.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to write data into the
                  feature group.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.
                * key `start_offline_backfill` and value `True` or `False` to configure
                  whether or not to start the backfill job to write data to the offline
                  storage. By default the backfill job gets started immediately.
                * key `internal_kafka` and value `True` or `False` in case you established
                  connectivity from you Python environment to the internal advertised
                  listeners of the Hopsworks Kafka Cluster. Defaults to `False` and
                  will use external listeners when connecting from outside of Hopsworks.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to ingest the feature group data.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. Unable to create feature group.
        """
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        user_version = self._version

        # fg_job is used only if the python engine is used
        fg_job, ge_report = self._feature_group_engine.save(
            self, feature_dataframe, write_options, validation_options
        )
        if ge_report is None or ge_report.ingestion_result == "INGESTED":
            self._code_engine.save_code(self)

        if self.statistics_config.enabled and engine.get_type() == "spark":
            # Only compute statistics if the engine is Spark.
            # For Python engine, the computation happens in the Hopsworks application
            self._statistics_engine.compute_statistics(self, feature_dataframe)
        if user_version is None:
            warnings.warn(
                "No version provided for creating feature group `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )
        return (
            fg_job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    def insert(
        self,
        features: Union[
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        overwrite: Optional[bool] = False,
        operation: Optional[str] = "upsert",
        storage: Optional[str] = None,
        write_options: Optional[Dict[str, Any]] = {},
        validation_options: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Job], Optional[ValidationReport]]:
        """Persist the metadata and materialize the feature group to the feature store
        or insert data from a dataframe into the existing feature group.

        Incrementally insert data to a feature group or overwrite all  data contained in the feature group. By
        default, the data is inserted into the offline storag as well as the online storage if the feature group is
        `online_enabled=True`. To insert only into the online storage, set `storage="online"`, or oppositely
        `storage="offline"`.

        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame,
        or a two-dimensional Numpy array or a two-dimensional Python nested list.
        If statistics are enabled, statistics are recomputed for the entire feature
        group.
        If feature group's time travel format is `HUDI` then `operation` argument can be
        either `insert` or `upsert`.

        If feature group doesn't exists  the insert method will create the necessary metadata the first time it is
        invoked and writes the specified `features` dataframe as feature group to the online/offline feature store.

        !!! example "Upsert new feature data with time travel format `HUDI`"
            ```python
            # connect to the Feature Store
            fs = ...

            fg = fs.get_or_create_feature_group(
                name='bitcoin_price',
                description='Bitcoin price aggregated for days',
                version=1,
                primary_key=['unix'],
                online_enabled=True,
                event_time='unix'
            )

            fg.insert(df_bitcoin_processed)
            ```

        !!! example "Async insert"
            ```python
            # connect to the Feature Store
            fs = ...

            fg1 = fs.get_or_create_feature_group(
                name='feature_group_name1',
                description='Description of the first FG',
                version=1,
                primary_key=['unix'],
                online_enabled=True,
                event_time='unix'
            )
            # async insertion in order not to wait till finish of the job
            fg.insert(df_for_fg1, write_options={"wait_for_job" : False})

            fg2 = fs.get_or_create_feature_group(
                name='feature_group_name2',
                description='Description of the second FG',
                version=1,
                primary_key=['unix'],
                online_enabled=True,
                event_time='unix'
            )
            fg.insert(df_for_fg2)
            ```

        # Arguments
            features: DataFrame, RDD, Ndarray, list. Features to be saved.
            overwrite: Drop all data in the feature group before
                inserting new data. This does not affect metadata, defaults to False.
            operation: Apache Hudi operation type `"insert"` or `"upsert"`.
                Defaults to `"upsert"`.
            storage: Overwrite default behaviour, write to offline
                storage only with `"offline"` or online only with `"online"`, defaults
                to `None`.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to write data into the
                  feature group.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the insert call should return only
                  after the Hopsworks Job has finished. By default it waits.
                * key `start_offline_backfill` and value `True` or `False` to configure
                  whether or not to start the backfill job to write data to the offline
                  storage. By default the backfill job gets started immediately.
                * key `internal_kafka` and value `True` or `False` in case you established
                  connectivity from you Python environment to the internal advertised
                  listeners of the Hopsworks Kafka Cluster. Defaults to `False` and
                  will use external listeners when connecting from outside of Hopsworks.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.

        # Returns
            (`Job`, `ValidationReport`) A tuple with job information if python engine is used and the validation report if validation is enabled.
        """
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        job, ge_report = self._feature_group_engine.insert(
            self,
            feature_dataframe=feature_dataframe,
            overwrite=overwrite,
            operation=operation,
            storage=storage.lower() if storage is not None else None,
            write_options=write_options,
            validation_options=validation_options
            if validation_options is not None
            else {"save_report": True},
        )

        if ge_report is None or ge_report.ingestion_result == "INGESTED":
            self._code_engine.save_code(self)
        if engine.get_type() == "spark":
            # Only compute statistics if the engine is Spark,
            # if Python, the statistics are computed by the application doing the insert
            self.compute_statistics()

        return (
            job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    def insert_stream(
        self,
        features: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        query_name: Optional[str] = None,
        output_mode: Optional[str] = "append",
        await_termination: Optional[bool] = False,
        timeout: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Ingest a Spark Structured Streaming Dataframe to the online feature store.

        This method creates a long running Spark Streaming Query, you can control the
        termination of the query through the arguments.

        It is possible to stop the returned query with the `.stop()` and check its
        status with `.isActive`.

        To get a list of all active queries, use:

        ```python
        sqm = spark.streams

        # get the list of active streaming queries
        [q.name for q in sqm.active]
        ```

        !!! warning "Engine Support"
            **Spark only**

            Stream ingestion using Pandas/Python as engine is currently not supported.
            Python/Pandas has no notion of streaming.

        !!! warning "Data Validation Support"
            `insert_stream` does not perform any data validation using Great Expectations
            even when a expectation suite is attached.

        # Arguments
            features: Features in Streaming Dataframe to be saved.
            query_name: It is possible to optionally specify a name for the query to
                make it easier to recognise in the Spark UI. Defaults to `None`.
            output_mode: Specifies how data of a streaming DataFrame/Dataset is
                written to a streaming sink. (1) `"append"`: Only the new rows in the
                streaming DataFrame/Dataset will be written to the sink. (2)
                `"complete"`: All the rows in the streaming DataFrame/Dataset will be
                written to the sink every time there is some update. (3) `"update"`:
                only the rows that were updated in the streaming DataFrame/Dataset will
                be written to the sink every time there are some updates.
                If the query doesn’t contain aggregations, it will be equivalent to
                append mode. Defaults to `"append"`.
            await_termination: Waits for the termination of this query, either by
                query.stop() or by an exception. If the query has terminated with an
                exception, then the exception will be thrown. If timeout is set, it
                returns whether the query has terminated or not within the timeout
                seconds. Defaults to `False`.
            timeout: Only relevant in combination with `await_termination=True`.
                Defaults to `None`.
            checkpoint_dir: Checkpoint directory location. This will be used to as a reference to
                from where to resume the streaming job. If `None` then hsfs will construct as
                "insert_stream_" + online_topic_name. Defaults to `None`.
                write_options: Additional write options for Spark as key-value pairs.
                Defaults to `{}`.

        # Returns
            `StreamingQuery`: Spark Structured Streaming Query object.
        """
        if (
            not engine.get_instance().is_spark_dataframe(features)
            or not features.isStreaming
        ):
            raise TypeError(
                "Features have to be a streaming type spark dataframe. Use `insert()` method instead."
            )
        else:
            # lower casing feature names
            feature_dataframe = engine.get_instance().convert_to_default_dataframe(
                features
            )
            warnings.warn(
                (
                    "Stream ingestion for feature group `{}`, with version"
                    " `{}` will not compute statistics."
                ).format(self._name, self._version),
                util.StatisticsWarning,
            )

            return self._feature_group_engine.insert_stream(
                self,
                feature_dataframe,
                query_name,
                output_mode,
                await_termination,
                timeout,
                checkpoint_dir,
                write_options,
            )

    def commit_details(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        limit: Optional[int] = None,
    ):
        """Retrieves commit timeline for this feature group. This method can only be used
        on time travel enabled feature groups

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            commit_details = fg.commit_details()
            ```

        # Arguments
            wallclock_time: Commit details as of specific point in time. Defaults to `None`.
                 Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            limit: Number of commits to retrieve. Defaults to `None`.

        # Returns
            `Dict[str, Dict[str, str]]`. Dictionary object of commit metadata timeline, where Key is commit id and value
            is `Dict[str, str]` with key value pairs of date committed on, number of rows updated, inserted and deleted.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
            `hsfs.client.exceptions.FeatureStoreException`. If the feature group does not have `HUDI` time travel format
        """
        return self._feature_group_engine.commit_details(self, wallclock_time, limit)

    def commit_delete_record(
        self,
        delete_df: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Drops records present in the provided DataFrame and commits it as update to this
        Feature group. This method can only be used on time travel enabled feature groups

        # Arguments
            delete_df: dataFrame containing records to be deleted.
            write_options: User provided write options. Defaults to `{}`.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        self._feature_group_engine.commit_delete(self, delete_df, write_options)

    def as_of(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        exclude_until: Optional[Union[str, int, datetime, date]] = None,
    ):
        """Get Query object to retrieve all features of the group at a point in the past.

        This method selects all features in the feature group and returns a Query object
        at the specified point in time. Optionally, commits before a specified point in time can be
        excluded from the query. The Query can then either be read into a Dataframe
        or used further to perform joins or construct a training dataset.

        !!! example "Reading features at a specific point in time:"
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # get data at a specific point in time and show it
            fg.as_of("2020-10-20 07:34:11").read().show()
            ```

        !!! example "Reading commits incrementally between specified points in time:"
            ```python
            fg.as_of("2020-10-20 07:34:11", exclude_until="2020-10-19 07:34:11").read().show()
            ```

        The first parameter is inclusive while the latter is exclusive.
        That means, in order to query a single commit, you need to query that commit time
        and exclude everything just before the commit.

        !!! example "Reading only the changes from a single commit"
            ```python
            fg.as_of("2020-10-20 07:31:38", exclude_until="2020-10-20 07:31:37").read().show()
            ```

        When no wallclock_time is given, the latest state of features is returned. Optionally, commits before
        a specified point in time can still be excluded.

        !!! example "Reading the latest state of features, excluding commits before a specified point in time:"
            ```python
            fg.as_of(None, exclude_until="2020-10-20 07:31:38").read().show()
            ```

        Note that the interval will be applied to all joins in the query.
        If you want to query different intervals for different feature groups in
        the query, you have to apply them in a nested fashion:
        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg1 = fs.get_or_create_feature_group(...)
            fg2 = fs.get_or_create_feature_group(...)

            fg1.select_all().as_of("2020-10-20", exclude_until="2020-10-19")
                .join(fg2.select_all().as_of("2020-10-20", exclude_until="2020-10-19"))
            ```

        If instead you apply another `as_of` selection after the join, all
        joined feature groups will be queried with this interval:
        !!! example
            ```python
            fg1.select_all().as_of("2020-10-20", exclude_until="2020-10-19")  # as_of is not applied
                .join(fg2.select_all().as_of("2020-10-20", exclude_until="2020-10-15"))  # as_of is not applied
                .as_of("2020-10-20", exclude_until="2020-10-19")
            ```

        !!! warning
            This function only works for feature groups with time_travel_format='HUDI'.

        !!! warning
            Excluding commits via exclude_until is only possible within the range of the Hudi active timeline.
            By default, Hudi keeps the last 20 to 30 commits in the active timeline.
            If you need to keep a longer active timeline, you can overwrite the options:
            `hoodie.keep.min.commits` and `hoodie.keep.max.commits`
            when calling the `insert()` method.

        # Arguments
            wallclock_time: Read data as of this point in time. Strings should be formatted in one of the
                following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.
            exclude_until: Exclude commits until this point in time. String should be formatted in one of the
                following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.

        # Returns
            `Query`. The query object with the applied time travel condition.
        """
        return self.select_all().as_of(wallclock_time, exclude_until)

    def validate(
        self,
        dataframe: Optional[
            Union[pd.DataFrame, TypeVar("pyspark.sql.DataFrame")]  # noqa: F821
        ] = None,
        expectation_suite: Optional[ExpectationSuite] = None,
        save_report: Optional[bool] = False,
        validation_options: Optional[Dict[Any, Any]] = {},
        ingestion_result: str = "UNKNOWN",
        ge_type: bool = True,
    ) -> Union[ge.core.ExpectationSuiteValidationResult, ValidationReport, None]:
        """Run validation based on the attached expectations.

        Runs any expectation attached with Deequ. But also runs attached Great Expectation
        Suites.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get feature group instance
            fg = fs.get_or_create_feature_group(...)

            ge_report = fg.validate(df, save_report=False)
            ```

        # Arguments
            dataframe: The dataframe to run the data validation expectations against.
            expectation_suite: Optionally provide an Expectation Suite to override the
                one that is possibly attached to the feature group. This is useful for
                testing new Expectation suites. When an extra suite is provided, the results
                will never be persisted. Defaults to `None`.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
            ingestion_result: Specify the fate of the associated data, defaults
                to "UNKNOWN". Supported options are  "UNKNOWN", "INGESTED", "REJECTED",
                "EXPERIMENT", "FG_DATA". Use "INGESTED" or "REJECTED" for validation
                of DataFrames to be inserted in the Feature Group. Use "EXPERIMENT"
                for testing and development and "FG_DATA" when validating data
                already in the Feature Group.
            save_report: Whether to save the report to the backend. This is only possible if the Expectation suite
                is initialised and attached to the Feature Group. Defaults to False.
            ge_type: Whether to return a Great Expectations object or Hopsworks own abstraction. Defaults to True.

        # Returns
            A Validation Report produced by Great Expectations.
        """
        # Activity is logged only if a the validation concerns the feature group and not a specific dataframe
        if dataframe is None:
            dataframe = self.read()
            if ingestion_result == "UNKNOWN":
                ingestion_result = "FG_DATA"

        return self._great_expectation_engine.validate(
            self,
            dataframe=engine.get_instance().convert_to_default_dataframe(dataframe),
            expectation_suite=expectation_suite,
            save_report=save_report,
            validation_options=validation_options,
            ingestion_result=ingestion_result,
            ge_type=ge_type,
        )

    def compute_statistics(
        self, wallclock_time: Optional[Union[str, int, datetime, date]] = None
    ):
        """Recompute the statistics for the feature group and save them to the
        feature store.

        Statistics are only computed for data in the offline storage of the feature
        group.

        # Arguments
            wallclock_time: If specified will recompute statistics on
                feature group as of specific point in time. If not specified then will compute statistics
                as of most recent time of this feature group. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.

        # Returns
            `Statistics`. The statistics metadata object.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. Unable to persist the statistics.
        """
        if self.statistics_config.enabled:
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Python engine. The Python engine is going to setup a Spark Job
            # to update the statistics.

            fg_commit_id = None
            if wallclock_time is not None:
                # Retrieve fg commit id related to this wall clock time and recompute statistics. It will throw
                # exception if its not time travel enabled feature group.
                fg_commit_id = [
                    commit_id
                    for commit_id in self._feature_group_engine.commit_details(
                        self, wallclock_time, 1
                    ).keys()
                ][0]

            return self._statistics_engine.compute_statistics(
                self,
                feature_group_commit_id=fg_commit_id
                if fg_commit_id is not None
                else None,
            )
        else:
            warnings.warn(
                (
                    "The statistics are not enabled of feature group `{}`, with version"
                    " `{}`. No statistics computed."
                ).format(self._name, self._version),
                util.StorageWarning,
            )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            if "type" in json_decamelized:
                json_decamelized["stream"] = (
                    json_decamelized["type"] == "streamFeatureGroupDTO"
                )
            _ = json_decamelized.pop("type", None)
            json_decamelized.pop("validation_type", None)
            return cls(**json_decamelized)
        for fg in json_decamelized:
            if "type" in fg:
                fg["stream"] = fg["type"] == "streamFeatureGroupDTO"
            _ = fg.pop("type", None)
            fg.pop("validation_type", None)
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        json_decamelized["stream"] = json_decamelized["type"] == "streamFeatureGroupDTO"
        _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        """Get specific Feature Group metadata in json format.

        !!! example
            ```python
            fg.json()
            ```
        """
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        """Get structured info about specific Feature Group in python dictionary format.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.to_dict()
            ```
        """
        fg_meta_dict = {
            "id": self._id,
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "onlineEnabled": self._online_enabled,
            "timeTravelFormat": self._time_travel_format,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": "cachedFeaturegroupDTO"
            if not self._stream
            else "streamFeatureGroupDTO",
            "statisticsConfig": self._statistics_config,
            "eventTime": self.event_time,
            "expectationSuite": self._expectation_suite,
            "parents": self._parents,
        }
        if self._stream:
            fg_meta_dict["deltaStreamerJobConf"] = self._deltastreamer_jobconf
        return fg_meta_dict

    def _get_table_name(self):
        return self.feature_store_name + "." + self.name + "_" + str(self.version)

    def _get_online_table_name(self):
        return self.name + "_" + str(self.version)

    def get_complex_features(self):
        """Returns the names of all features with a complex data type in this
        feature group.

        !!! example
            ```python
            complex_dtype_features = fg.get_complex_features()
            ```
        """
        return [f.name for f in self.features if f.is_complex()]

    def _get_encoded_avro_schema(self):
        complex_features = self.get_complex_features()
        schema = json.loads(self.avro_schema)

        for field in schema["fields"]:
            if field["name"] in complex_features:
                field["type"] = ["null", "bytes"]

        schema_s = json.dumps(schema)
        try:
            avro.schema.parse(schema_s)
        except avro.schema.SchemaParseException as e:
            raise FeatureStoreException("Failed to construct Avro Schema: {}".format(e))
        return schema_s

    def _get_feature_avro_schema(self, feature_name):
        for field in json.loads(self.avro_schema)["fields"]:
            if field["name"] == feature_name:
                return json.dumps(field["type"])

    @property
    def id(self):
        """Feature group id."""
        return self._id

    @property
    def name(self):
        """Name of the feature group."""
        return self._name

    @property
    def version(self):
        """Version number of the feature group."""
        return self._version

    @property
    def description(self):
        """Description of the feature group contents."""
        return self._description

    @property
    def features(self):
        """Schema information."""
        return self._features

    @property
    def online_enabled(self):
        """Setting if the feature group is available in online storage."""
        return self._online_enabled

    @property
    def time_travel_format(self):
        """Setting of the feature group time travel format."""
        return self._time_travel_format

    @property
    def partition_key(self):
        """List of features building the partition key."""
        return self._partition_key

    @property
    def hudi_precombine_key(self):
        """Feature name that is the hudi precombine key."""
        return self._hudi_precombine_key

    @property
    def feature_store_id(self):
        return self._feature_store_id

    @property
    def feature_store_name(self):
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name

    @property
    def creator(self):
        """Username of the creator."""
        return self._creator

    @property
    def created(self):
        """Timestamp when the feature group was created."""
        return self._created

    @property
    def subject(self):
        """Subject of the feature group."""
        if self._subject is None:
            # cache the schema
            self._subject = self._feature_group_engine.get_subject(self)
        return self._subject

    @property
    def avro_schema(self):
        """Avro schema representation of the feature group."""
        return self.subject["schema"]

    @property
    def stream(self):
        """Whether to enable real time stream writing capabilities."""
        return self._stream

    @property
    def parents(self):
        """Parent feature groups as origin of the data in the current feature group.
        This is part of explicit provenance"""
        return self._parents

    @version.setter
    def version(self, version):
        self._version = version

    @description.setter
    def description(self, new_description):
        self._description = new_description

    @features.setter
    def features(self, new_features):
        self._features = new_features

    @time_travel_format.setter
    def time_travel_format(self, new_time_travel_format):
        self._time_travel_format = new_time_travel_format

    @partition_key.setter
    def partition_key(self, new_partition_key):
        self._partition_key = [pk.lower() for pk in new_partition_key]

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key):
        self._hudi_precombine_key = hudi_precombine_key.lower()

    @online_enabled.setter
    def online_enabled(self, new_online_enabled):
        self._online_enabled = new_online_enabled

    @stream.setter
    def stream(self, stream):
        self._stream = stream

    @parents.setter
    def parents(self, new_parents):
        self._parents = new_parents


class ExternalFeatureGroup(FeatureGroupBase):
    EXTERNAL_FEATURE_GROUP = "ON_DEMAND_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        storage_connector,
        query=None,
        data_format=None,
        path=None,
        options={},
        name=None,
        version=None,
        description=None,
        primary_key=None,
        featurestore_id=None,
        featurestore_name=None,
        created=None,
        creator=None,
        id=None,
        features=None,
        location=None,
        statistics_config=None,
        event_time=None,
        expectation_suite=None,
        href=None,
    ):
        super().__init__(featurestore_id, location, event_time=event_time)
        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = user.User.from_response_json(creator)
        self._version = version
        self._name = name
        self._query = query
        self._data_format = data_format.upper() if data_format else None
        self._path = path
        self._id = id
        self._expectation_suite = expectation_suite

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._feature_group_engine = (
            external_feature_group_engine.ExternalFeatureGroupEngine(featurestore_id)
        )

        if self._id:
            # Got from Hopsworks, deserialize features and storage connector
            self._features = (
                [feature.Feature.from_response_json(feat) for feat in features]
                if features
                else None
            )
            self.primary_key = (
                [feat.name for feat in self._features if feat.primary is True]
                if self._features
                else []
            )
            self.statistics_config = statistics_config
            self.expectation_suite = expectation_suite

            self._options = (
                {option["name"]: option["value"] for option in options}
                if options
                else None
            )
        else:
            self.primary_key = primary_key
            self.statistics_config = statistics_config
            self.expectation_suite = expectation_suite
            self._features = features
            self._options = options

        if storage_connector is not None and isinstance(storage_connector, dict):
            self._storage_connector = sc.StorageConnector.from_response_json(
                storage_connector
            )
        else:
            self._storage_connector = storage_connector

        self.expectation_suite = expectation_suite
        self._href = href

    def save(self):
        """Persist the metadata for this external feature group.

        Without calling this method, your feature group will only exist
        in your Python Kernel, but not in Hopsworks.

        ```python
        query = "SELECT * FROM sales"

        fg = feature_store.create_external_feature_group(name="sales",
            version=1,
            description="Physical shop sales features",
            query=query,
            storage_connector=connector,
            primary_key=['ss_store_sk'],
            event_time='sale_date'
        )

        fg.save()
        """
        self._feature_group_engine.save(self)
        self._code_engine.save_code(self)

        if self.statistics_config.enabled:
            self._statistics_engine.compute_statistics(self, self.read())

    def read(self, dataframe_type="default"):
        """Get the feature group as a DataFrame.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            df = fg.read()
            ```

        !!! warning "Engine Support"
            **Spark only**

            Reading an External Feature Group directly into a Pandas Dataframe using
            Python/Pandas as Engine is not supported, however, you can use the
            Query API to create Feature Views/Training Data containing External
            Feature Groups.

        # Arguments
            dataframe_type: str, optional. Possible values are `"default"`, `"spark"`,
                `"pandas"`, `"numpy"` or `"python"`, defaults to `"default"`.

        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`. A Spark DataFrame.
            `pandas.DataFrame`. A Pandas DataFrame.
            `numpy.ndarray`. A two-dimensional Numpy array.
            `list`. A two-dimensional Python list.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().read(dataframe_type=dataframe_type)

    def show(self, n):
        """Show the first n rows of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.show(5)
            ```
        """
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().show(n)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            _ = json_decamelized.pop("online_topic_name", None)
            _ = json_decamelized.pop("type", None)
            return cls(**json_decamelized)
        for fg in json_decamelized:
            _ = fg.pop("online_topic_name", None)
            _ = fg.pop("type", None)
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "query": self._query,
            "dataFormat": self._data_format,
            "path": self._path,
            "options": [{"name": k, "value": v} for k, v in self._options.items()]
            if self._options
            else None,
            "storageConnector": self._storage_connector.to_dict(),
            "type": "onDemandFeaturegroupDTO",
            "statisticsConfig": self._statistics_config,
            "eventTime": self._event_time,
            "expectationSuite": self._expectation_suite,
        }

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def description(self):
        return self._description

    @property
    def features(self):
        return self._features

    @property
    def query(self):
        return self._query

    @property
    def data_format(self):
        return self._data_format

    @property
    def path(self):
        return self._path

    @property
    def options(self):
        return self._options

    @property
    def storage_connector(self):
        return self._storage_connector

    @property
    def creator(self):
        return self._creator

    @property
    def created(self):
        return self._created

    @version.setter
    def version(self, version):
        self._version = version

    @description.setter
    def description(self, new_description):
        self._description = new_description

    @features.setter
    def features(self, new_features):
        self._features = new_features

    @property
    def feature_store_name(self):
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name
