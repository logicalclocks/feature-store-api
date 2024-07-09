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
from __future__ import annotations

import copy
import json
import logging
import time
import warnings
from datetime import date, datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)


if TYPE_CHECKING:
    import great_expectations

import avro.schema
import confluent_kafka
import humps
import numpy as np
import pandas as pd
import polars as pl
from hsfs import (
    engine,
    feature,
    feature_group_writer,
    tag,
    user,
    util,
)
from hsfs import (
    feature_store as feature_store_mod,
)
from hsfs import (
    storage_connector as sc,
)
from hsfs.client.exceptions import FeatureStoreException, RestAPIError
from hsfs.constructor import filter, query
from hsfs.constructor.filter import Filter, Logic
from hsfs.core import (
    code_engine,
    deltastreamer_jobconf,
    expectation_suite_engine,
    explicit_provenance,
    external_feature_group_engine,
    feature_group_engine,
    feature_monitoring_config_engine,
    feature_monitoring_result_engine,
    feature_store_api,
    great_expectation_engine,
    job_api,
    spine_group_engine,
    statistics_engine,
    validation_report_engine,
    validation_result_engine,
)
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_result as fmr
from hsfs.core.constants import (
    HAS_GREAT_EXPECTATIONS,
)
from hsfs.core.job import Job
from hsfs.core.variable_api import VariableApi
from hsfs.core.vector_db_client import VectorDbClient

# if great_expectations is not installed, we will default to using native Hopsworks class as return values
from hsfs.decorators import typechecked, uses_great_expectations
from hsfs.embedding import EmbeddingIndex
from hsfs.expectation_suite import ExpectationSuite
from hsfs.ge_validation_result import ValidationResult
from hsfs.hopsworks_udf import HopsworksUdf, UDFType
from hsfs.statistics import Statistics
from hsfs.statistics_config import StatisticsConfig
from hsfs.transformation_function import TransformationFunction
from hsfs.validation_report import ValidationReport


if HAS_GREAT_EXPECTATIONS:
    import great_expectations


_logger = logging.getLogger(__name__)


@typechecked
class FeatureGroupBase:
    def __init__(
        self,
        name: Optional[str],
        version: Optional[int],
        featurestore_id: Optional[int],
        location: Optional[str],
        event_time: Optional[Union[str, int, date, datetime]] = None,
        online_enabled: bool = False,
        id: Optional[int] = None,
        embedding_index: Optional[EmbeddingIndex] = None,
        expectation_suite: Optional[
            Union[
                ExpectationSuite,
                great_expectations.core.ExpectationSuite,
                Dict[str, Any],
            ]
        ] = None,
        online_topic_name: Optional[str] = None,
        topic_name: Optional[str] = None,
        notification_topic_name: Optional[str] = None,
        deprecated: bool = False,
        **kwargs,
    ) -> None:
        self._version = version
        self._name = name
        self.event_time = event_time
        self._online_enabled = online_enabled
        self._location = location
        self._id = id
        self._subject = None
        self._online_topic_name = online_topic_name
        self._topic_name = topic_name
        self._notification_topic_name = notification_topic_name
        self._deprecated = deprecated
        self._feature_store_id = featurestore_id
        self._feature_store = None
        self._variable_api: VariableApi = VariableApi()

        self._multi_part_insert: bool = False
        self._embedding_index = embedding_index
        # use setter for correct conversion
        self.expectation_suite = expectation_suite

        self._feature_group_engine: Optional[
            feature_group_engine.FeatureGroupEngine
        ] = None
        self._statistics_engine: statistics_engine.StatisticsEngine = (
            statistics_engine.StatisticsEngine(featurestore_id, self.ENTITY_TYPE)
        )
        self._code_engine: code_engine.CodeEngine = code_engine.CodeEngine(
            featurestore_id, self.ENTITY_TYPE
        )
        self._great_expectation_engine: great_expectation_engine.GreatExpectationEngine = great_expectation_engine.GreatExpectationEngine(
            featurestore_id
        )
        if self._id is not None:
            if expectation_suite:
                self._expectation_suite._init_expectation_engine(
                    feature_store_id=featurestore_id, feature_group_id=self._id
                )
            self._expectation_suite_engine: Optional[
                expectation_suite_engine.ExpectationSuiteEngine
            ] = expectation_suite_engine.ExpectationSuiteEngine(
                feature_store_id=featurestore_id, feature_group_id=self._id
            )
            self._validation_report_engine: Optional[
                validation_report_engine.ValidationReportEngine
            ] = validation_report_engine.ValidationReportEngine(
                featurestore_id, self._id
            )
            self._validation_result_engine: Optional[
                validation_result_engine.ValidationResultEngine
            ] = validation_result_engine.ValidationResultEngine(
                featurestore_id, self._id
            )
            self._feature_monitoring_config_engine: Optional[
                feature_monitoring_config_engine.FeatureMonitoringConfigEngine
            ] = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
                feature_store_id=featurestore_id,
                feature_group_id=self._id,
            )
            self._feature_monitoring_result_engine: feature_monitoring_result_engine.FeatureMonitoringResultEngine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
                feature_store_id=self._feature_store_id,
                feature_group_id=self._id,
            )

        self.check_deprecated()

    def check_deprecated(self) -> None:
        if self.deprecated:
            warnings.warn(
                f"Feature Group `{self._name}`, version `{self._version}` is deprecated",
                stacklevel=1,
            )

    def delete(self) -> None:
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
            stacklevel=1,
        )
        self._feature_group_engine.delete(self)

    def select_all(
        self,
        include_primary_key: Optional[bool] = True,
        include_event_time: Optional[bool] = True,
    ) -> query.Query:
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

    def select(self, features: List[Union[str, feature.Feature]]) -> query.Query:
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
                strings to be selected.

        # Returns
            `Query`: A query object with the selected features of the feature group.
        """
        return query.Query(
            left_feature_group=self,
            left_features=features,
            feature_store_name=self._feature_store_name,
            feature_store_id=self._feature_store_id,
        )

    def select_except(
        self, features: Optional[List[Union[str, feature.Feature]]] = None
    ) -> query.Query:
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

    def filter(self, f: Union[filter.Filter, filter.Logic]) -> query.Query:
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

        Composite filters require parenthesis and symbols for logical operands (e.g. `&`, `|`, ...):
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

    def add_tag(self, name: str, value: Any) -> None:
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

    def delete_tag(self, name: str) -> None:
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

    def get_tag(self, name: str) -> tag.Tag:
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

    def get_tags(self) -> Dict[str, tag.Tag]:
        """Retrieves all tags attached to a feature group.

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to retrieve the tags.
        """
        return self._feature_group_engine.get_tags(self)

    def get_parent_feature_groups(self) -> explicit_provenance.Links:
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

    def get_storage_connector_provenance(self):
        """Get the parents of this feature group, based on explicit provenance.
        Parents are storage connectors. These storage connector can be accessible,
        deleted or inaccessible.
        For deleted and inaccessible storage connector, only a minimal information is
        returned.

        # Returns
            `ExplicitProvenance.Links`: the storage connector used to generated this
            feature group

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        return self._feature_group_engine.get_storage_connector_provenance(self)

    def get_storage_connector(self):
        """Get the storage connector using this feature group, based on explicit
        provenance. Only the accessible storage connector is returned.
        For more items use the base method - get_storage_connector_provenance

        # Returns
            `StorageConnector: Storage connector.
        """
        storage_connector_provenance = self.get_storage_connector_provenance()

        if (
            storage_connector_provenance.inaccessible
            or storage_connector_provenance.deleted
        ):
            _logger.info(
                "The parent storage connector is deleted or inaccessible. For more details access `get_storage_connector_provenance`"
            )

        if storage_connector_provenance.accessible:
            return storage_connector_provenance.accessible[0]
        else:
            return None

    def get_generated_feature_views(self) -> explicit_provenance.Links:
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

    def get_generated_feature_groups(self) -> explicit_provenance.Links:
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

    def get_feature(self, name: str) -> feature.Feature:
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
        except KeyError as err:
            raise FeatureStoreException(
                f"'FeatureGroup' object has no feature called '{name}'."
            ) from err

    def update_statistics_config(
        self,
    ) -> Union[FeatureGroup, ExternalFeatureGroup, SpineGroup, FeatureGroupBase]:
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
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        self._feature_group_engine.update_statistics_config(self)
        return self

    def update_description(
        self, description: str
    ) -> Union[FeatureGroupBase, FeatureGroup, ExternalFeatureGroup, SpineGroup]:
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

    def update_notification_topic_name(
        self, notification_topic_name: str
    ) -> Union[FeatureGroupBase, ExternalFeatureGroup, SpineGroup, FeatureGroup]:
        """Update the notification topic name of the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_notification_topic_name(notification_topic_name="notification_topic_name")
            ```

        !!! info "Safe update"
            This method updates the feature group notification topic name safely. In case of failure
            your local metadata object will keep the old notification topic name.

        # Arguments
            notification_topic_name: Name of the topic used for sending notifications when entries
                are inserted or updated on the online feature store. If set to None no notifications are sent.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        self._feature_group_engine.update_notification_topic_name(
            self, notification_topic_name
        )
        return self

    def update_deprecated(
        self, deprecate: bool = True
    ) -> Union[FeatureGroupBase, FeatureGroup, ExternalFeatureGroup, SpineGroup]:
        """Deprecate the feature group.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_deprecated(deprecate=True)
            ```

        !!! info "Safe update"
            This method updates the feature group safely. In case of failure
            your local metadata object will be kept unchanged.

        # Arguments
            deprecate: Boolean value identifying if the feature group should be deprecated. Defaults to True.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        self._feature_group_engine.update_deprecated(self, deprecate)
        return self

    def update_features(
        self, features: Union[feature.Feature, List[feature.Feature]]
    ) -> Union[FeatureGroupBase, FeatureGroup, ExternalFeatureGroup, SpineGroup]:
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

    def update_feature_description(
        self, feature_name: str, description: str
    ) -> Union[FeatureGroupBase, FeatureGroup, ExternalFeatureGroup, SpineGroup]:
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

    def append_features(
        self, features: Union[feature.Feature, List[feature.Feature]]
    ) -> Union[FeatureGroupBase, FeatureGroup, ExternalFeatureGroup, SpineGroup]:
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
        features is considered a breaking change. Note that feature views built on
        top of this feature group will not read appended feature data. Create a new
        feature view based on an updated query via `fg.select` to include the new features.

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
        self, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> Union[ExpectationSuite, great_expectations.core.ExpectationSuite, None]:
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
                method on hopsworks type. Defaults to `True` if Great Expectations is installed,
                else `False`.

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
        expectation_suite: Union[
            ExpectationSuite, great_expectations.core.ExpectationSuite
        ],
        run_validation: bool = True,
        validation_ingestion_policy: Literal["always", "strict"] = "always",
        overwrite: bool = False,
    ) -> Union[ExpectationSuite, great_expectations.core.ExpectationSuite]:
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
        if HAS_GREAT_EXPECTATIONS and isinstance(
            expectation_suite, great_expectations.core.ExpectationSuite
        ):
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
        self, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> Union[
        ValidationReport, great_expectations.core.ExpectationSuiteValidationResult, None
    ]:
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
                method on hopsworks type. Defaults to `True` if Great Expectations is installed,
                else `False`.

        # Returns
            `ValidationReport`. The latest validation report attached to the Feature Group.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        return self._validation_report_engine.get_last(ge_type=ge_type)

    def get_all_validation_reports(
        self, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> List[
        Union[
            ValidationReport, great_expectations.core.ExpectationSuiteValidationResult
        ]
    ]:
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
                method on hopsworks type. Defaults to `True` if Great Expectations is installed,
                else `False`.

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
            Dict[str, Any],
            ValidationReport,
            great_expectations.core.expectation_validation_result.ExpectationSuiteValidationResult,
        ],
        ingestion_result: Literal["unknown", "experiment", "fg_data"] = "UNKNOWN",
        ge_type: bool = HAS_GREAT_EXPECTATIONS,
    ) -> Union[
        ValidationReport, great_expectations.core.ExpectationSuiteValidationResult
    ]:
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
                method on hopsworks type. Defaults to `True` if Great Expectations is installed,
                else `False`.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        if self._id:
            if HAS_GREAT_EXPECTATIONS and isinstance(
                validation_report,
                great_expectations.core.expectation_validation_result.ExpectationSuiteValidationResult,
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
        filter_by: List[
            Literal["ingested", "rejected", "unknown", "fg_data", "experiment"]
        ] = None,
        ge_type: bool = HAS_GREAT_EXPECTATIONS,
    ) -> Union[
        List[ValidationResult],
        List[great_expectations.core.ExpectationValidationResult],
    ]:
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
            ge_type: If `True` returns a native Great Expectation type, Hopsworks
                custom type otherwise. Conversion can be performed via the `to_ge_type()`
                method on hopsworks type. Defaults to `True` if Great Expectations is installed,
                else `False`.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.

        # Return
            Union[List[`ValidationResult`], List[`ExpectationValidationResult`]] A list of validation result connected to the expectation_id
        """
        if self._id:
            return self._validation_result_engine.get_validation_history(
                expectation_id=expectation_id,
                start_validation_time=start_validation_time,
                end_validation_time=end_validation_time,
                filter_by=filter_by or [],
                ge_type=ge_type,
            )
        else:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch validation history."
            )

    @uses_great_expectations
    def validate(
        self,
        dataframe: Optional[
            Union[pd.DataFrame, TypeVar("pyspark.sql.DataFrame")]  # noqa: F821
        ] = None,
        expectation_suite: Optional[ExpectationSuite] = None,
        save_report: Optional[bool] = False,
        validation_options: Optional[Dict[str, Any]] = None,
        ingestion_result: Literal[
            "unknown", "ingested", "rejected", "fg_data", "experiement"
        ] = "unknown",
        ge_type: bool = True,
    ) -> Union[
        great_expectations.core.ExpectationSuiteValidationResult, ValidationReport, None
    ]:
        """Run validation based on the attached expectations.

        Runs the expectation suite attached to the feature group against the provided dataframe.
        Raise an error if the great_expectations package is not installed.

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
            ge_type: Whether to return a Great Expectations object or Hopsworks own abstraction.
                Defaults to `True` if Great Expectations is installed, else `False`.

        # Returns
            A Validation Report produced by Great Expectations.
        """
        # Activity is logged only if a the validation concerns the feature group and not a specific dataframe
        if dataframe is None:
            dataframe = self.read()
            if ingestion_result.upper() == "UNKNOWN":
                ingestion_result = "FG_DATA"

        return self._great_expectation_engine.validate(
            self,
            dataframe=engine.get_instance().convert_to_default_dataframe(dataframe),
            expectation_suite=expectation_suite,
            save_report=save_report,
            validation_options=validation_options or {},
            ingestion_result=ingestion_result.upper(),
            ge_type=ge_type,
        )

    @classmethod
    def from_response_json(
        cls, feature_group_json: Dict[str, Any]
    ) -> Union[FeatureGroup, ExternalFeatureGroup, SpineGroup]:
        if (
            feature_group_json["type"] == "onDemandFeaturegroupDTO"
            and not feature_group_json["spine"]
        ):
            feature_group_obj = ExternalFeatureGroup.from_response_json(
                feature_group_json
            )
        elif (
            feature_group_json["type"] == "onDemandFeaturegroupDTO"
            and feature_group_json["spine"]
        ):
            feature_group_obj = SpineGroup.from_response_json(feature_group_json)
        else:
            feature_group_obj = FeatureGroup.from_response_json(feature_group_json)
        return feature_group_obj

    def get_feature_monitoring_configs(
        self,
        name: Optional[str] = None,
        feature_name: Optional[str] = None,
        config_id: Optional[int] = None,
    ) -> Union[fmc.FeatureMonitoringConfig, List[fmc.FeatureMonitoringConfig], None]:
        """Fetch all feature monitoring configs attached to the feature group, or fetch by name or feature name only.
        If no arguments is provided the method will return all feature monitoring configs
        attached to the feature group, meaning all feature monitoring configs that are attach
        to a feature in the feature group. If you wish to fetch a single config, provide the
        its name. If you wish to fetch all configs attached to a particular feature, provide
        the feature name.
        !!! example
            ```python3
            # fetch your feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # fetch all feature monitoring configs attached to the feature group
            fm_configs = fg.get_feature_monitoring_configs()

            # fetch a single feature monitoring config by name
            fm_config = fg.get_feature_monitoring_configs(name="my_config")

            # fetch all feature monitoring configs attached to a particular feature
            fm_configs = fg.get_feature_monitoring_configs(feature_name="my_feature")

            # fetch a single feature monitoring config with a given id
            fm_config = fg.get_feature_monitoring_configs(config_id=1)
            ```

        # Arguments
            name: If provided fetch only the feature monitoring config with the given name.
                Defaults to None.
            feature_name: If provided, fetch only configs attached to a particular feature.
                Defaults to None.
            config_id: If provided, fetch only the feature monitoring config with the given id.
                Defaults to None.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
            `hsfs.client.exceptions.FeatureStoreException`.
            ValueError: if both name and feature_name are provided.
            TypeError: if name or feature_name are not string or None.

        # Return
            Union[`FeatureMonitoringConfig`, List[`FeatureMonitoringConfig`], None]
                A list of feature monitoring configs. If name provided,
                returns either a single config or None if not found.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch feature monitoring configurations."
            )

        return self._feature_monitoring_config_engine.get_feature_monitoring_configs(
            name=name,
            feature_name=feature_name,
            config_id=config_id,
        )

    def get_feature_monitoring_history(
        self,
        config_name: Optional[str] = None,
        config_id: Optional[int] = None,
        start_time: Optional[Union[int, str, datetime, date]] = None,
        end_time: Optional[Union[int, str, datetime, date]] = None,
        with_statistics: Optional[bool] = True,
    ) -> List[fmr.FeatureMonitoringResult]:
        """Fetch feature monitoring history for a given feature monitoring config.

        !!! example
            ```python3
            # fetch your feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # fetch feature monitoring history for a given feature monitoring config
            fm_history = fg.get_feature_monitoring_history(
                config_name="my_config",
                start_time="2020-01-01",
            )

            # fetch feature monitoring history for a given feature monitoring config id
            fm_history = fg.get_feature_monitoring_history(
                config_id=1,
                start_time=datetime.now() - timedelta(weeks=2),
                end_time=datetime.now() - timedelta(weeks=1),
                with_statistics=False,
            )
            ```

        # Arguments
            config_name: The name of the feature monitoring config to fetch history for.
                Defaults to None.
            config_id: The id of the feature monitoring config to fetch history for.
                Defaults to None.
            start_time: The start date of the feature monitoring history to fetch.
                Defaults to None.
            end_time: The end date of the feature monitoring history to fetch.
                Defaults to None.
            with_statistics: Whether to include statistics in the feature monitoring history.
                Defaults to True. If False, only metadata about the monitoring will be fetched.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
            `hsfs.client.exceptions.FeatureStoreException`.
            ValueError: if both config_name and config_id are provided.
            TypeError: if config_name or config_id are not respectively string, int or None.

        # Return
            List[`FeatureMonitoringResult`]
                A list of feature monitoring results containing the monitoring metadata
                as well as the computed statistics for the detection and reference window
                if requested.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch feature monitoring history."
            )

        return self._feature_monitoring_result_engine.get_feature_monitoring_results(
            config_name=config_name,
            config_id=config_id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    def create_statistics_monitoring(
        self,
        name: str,
        feature_name: Optional[str] = None,
        description: Optional[str] = None,
        start_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        end_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        cron_expression: Optional[str] = "0 0 12 ? * * *",
    ) -> fmc.FeatureMonitoringConfig:
        """Run a job to compute statistics on snapshot of feature data on a schedule.

        !!! experimental
            Public API is subject to change, this feature is not suitable for production use-cases.

        !!! example
            ```python3
            # fetch feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # enable statistics monitoring
            my_config = fg.create_statistics_monitoring(
                name="my_config",
                start_date_time="2021-01-01 00:00:00",
                description="my description",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                # Statistics computed on 10% of the last week of data
                time_offset="1w",
                row_percentage=0.1,
            ).save()
            ```

        # Arguments
            name: Name of the feature monitoring configuration.
                name must be unique for all configurations attached to the feature group.
            feature_name: Name of the feature to monitor. If not specified, statistics
                will be computed for all features.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression to use to schedule the job. The cron expression
                must be in UTC and follow the Quartz specification. Default is '0 0 12 ? * * *',
                every day at 12pm UTC.

        # Raises
            `hsfs.client.exceptions.FeatureStoreException`.

        # Return
            `FeatureMonitoringConfig` Configuration with minimal information about the feature monitoring.
                Additional information are required before feature monitoring is enabled.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can enable scheduled statistics monitoring."
            )

        return self._feature_monitoring_config_engine._build_default_statistics_monitoring_config(
            name=name,
            feature_name=feature_name,
            description=description,
            start_date_time=start_date_time,
            valid_feature_names=[feat.name for feat in self._features],
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    def create_feature_monitoring(
        self,
        name: str,
        feature_name: str,
        description: Optional[str] = None,
        start_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        end_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        cron_expression: Optional[str] = "0 0 12 ? * * *",
    ) -> fmc.FeatureMonitoringConfig:
        """Enable feature monitoring to compare statistics on snapshots of feature data over time.

        !!! experimental
            Public API is subject to change, this feature is not suitable for production use-cases.

        !!! example
            ```python3
            # fetch feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # enable feature monitoring
            my_config = fg.create_feature_monitoring(
                name="my_monitoring_config",
                feature_name="my_feature",
                description="my monitoring config description",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                # Data inserted in the last day
                time_offset="1d",
                window_length="1d",
            ).with_reference_window(
                # Data inserted last week on the same day
                time_offset="1w1d",
                window_length="1d",
            ).compare_on(
                metric="mean",
                threshold=0.5,
            ).save()
            ```

        # Arguments
            name: Name of the feature monitoring configuration.
                name must be unique for all configurations attached to the feature group.
            feature_name: Name of the feature to monitor.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression to use to schedule the job. The cron expression
                must be in UTC and follow the Quartz specification. Default is '0 0 12 ? * * *',
                every day at 12pm UTC.

        # Raises
            `hsfs.client.exceptions.FeatureStoreException`.

        # Return
            `FeatureMonitoringConfig` Configuration with minimal information about the feature monitoring.
                Additional information are required before feature monitoring is enabled.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can enable feature monitoring."
            )

        return self._feature_monitoring_config_engine._build_default_feature_monitoring_config(
            name=name,
            feature_name=feature_name,
            description=description,
            start_date_time=start_date_time,
            valid_feature_names=[feat.name for feat in self._features],
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__getitem__(name)
        except KeyError as err:
            raise AttributeError(
                f"'FeatureGroup' object has no attribute '{name}'. "
                "If you are trying to access a feature, fall back on "
                "using the `get_feature` method."
            ) from err

    def __getitem__(self, name: str) -> feature.Feature:
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
    def statistics_config(self) -> StatisticsConfig:
        """Statistics configuration object defining the settings for statistics
        computation of the feature group.

        # Raises
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_config

    @statistics_config.setter
    def statistics_config(
        self,
        statistics_config: Optional[Union[StatisticsConfig, Dict[str, Any], bool]],
    ) -> None:
        self._check_statistics_support()  # raises an error if stats not supported
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
    def feature_store_id(self) -> Optional[int]:
        return self._feature_store_id

    @property
    def feature_store(self) -> feature_store_mod.FeatureStore:
        if self._feature_store is None:
            self._feature_store = feature_store_api.FeatureStoreApi().get(
                self._feature_store_id
            )
        return self._feature_store

    @feature_store.setter
    def feature_store(self, feature_store: feature_store_mod.FeatureStore) -> None:
        self._feature_store = feature_store

    @property
    def name(self) -> Optional[str]:
        """Name of the feature group."""
        return self._name

    @property
    def version(self) -> Optional[int]:
        """Version number of the feature group."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    def get_fg_name(self) -> str:
        return f"{self.name}_{self.version}"

    @property
    def statistics(self) -> Optional[Statistics]:
        """Get the latest computed statistics for the whole feature group.

        # Raises
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_engine.get(self)

    @property
    def primary_key(self) -> List[str]:
        """List of features building the primary key."""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, new_primary_key: List[str]) -> None:
        self._primary_key = [util.autofix_feature_name(pk) for pk in new_primary_key]

    def get_statistics(
        self,
        computation_time: Optional[Union[str, int, float, datetime, date]] = None,
        feature_names: Optional[List[str]] = None,
    ) -> Optional[Statistics]:
        """Returns the statistics computed at a specific time for the current feature group.

        If `computation_time` is `None`, the most recent statistics are returned.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_statistics = fg.get_statistics(computation_time=None)
            ```

        # Arguments
            computation_time: Date and time when statistics were computed. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.
            feature_names: List of feature names of which statistics are retrieved.
        # Returns
            `Statistics`. Statistics object.

        # Raises
            `hsfs.client.exceptions.RestAPIError`
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_engine.get(
            self, computation_time=computation_time, feature_names=feature_names
        )

    def get_all_statistics(
        self,
        computation_time: Optional[Union[str, int, float, datetime, date]] = None,
        feature_names: Optional[List[str]] = None,
    ) -> Optional[List[Statistics]]:
        """Returns all the statistics metadata computed before a specific time for the current feature group.

        If `computation_time` is `None`, all the statistics metadata are returned.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_statistics = fg.get_statistics(computation_time=None)
            ```

        # Arguments
            computation_time: Date and time when statistics were computed. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.
            feature_names: List of feature names of which statistics are retrieved.

        # Returns
            `Statistics`. Statistics object.

        # Raises
            `hsfs.client.exceptions.RestAPIError`
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_engine.get_all(
            self, computation_time=computation_time, feature_names=feature_names
        )

    def compute_statistics(self) -> None:
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
            `hsfs.client.exceptions.FeatureStoreException`.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        if self.statistics_config.enabled:
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Python engine. The Python engine is going to setup a Spark Job
            # to update the statistics.
            self._statistics_engine.compute_and_save_statistics(self)
        else:
            warnings.warn(
                (
                    "The statistics are not enabled of feature group `{}`, with version"
                    " `{}`. No statistics computed."
                ).format(self._name, self._version),
                util.StorageWarning,
                stacklevel=1,
            )

    @property
    def embedding_index(self) -> Optional["EmbeddingIndex"]:
        if self._embedding_index:
            self._embedding_index.feature_group = self
        return self._embedding_index

    @embedding_index.setter
    def embedding_index(self, embedding_index: Optional["EmbeddingIndex"]) -> None:
        self._embedding_index = embedding_index

    @property
    def event_time(self) -> Optional[str]:
        """Event time feature in the feature group."""
        return self._event_time

    @event_time.setter
    def event_time(self, feature_name: Optional[str]) -> None:
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
    def location(self) -> Optional[str]:
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
            ExpectationSuite,
            great_expectations.core.ExpectationSuite,
            Dict[str, Any],
            None,
        ],
    ) -> None:
        if isinstance(expectation_suite, ExpectationSuite):
            tmp_expectation_suite = expectation_suite.to_json_dict(decamelize=True)
            tmp_expectation_suite["feature_group_id"] = self._id
            tmp_expectation_suite["feature_store_id"] = self._feature_store_id
            self._expectation_suite = ExpectationSuite(**tmp_expectation_suite)
        elif HAS_GREAT_EXPECTATIONS and isinstance(
            expectation_suite,
            great_expectations.core.expectation_suite.ExpectationSuite,
        ):
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

    @property
    def online_enabled(self) -> bool:
        """Setting if the feature group is available in online storage."""
        return self._online_enabled

    @online_enabled.setter
    def online_enabled(self, online_enabled: bool) -> None:
        self._online_enabled = online_enabled

    @property
    def topic_name(self) -> Optional[str]:
        """The topic used for feature group data ingestion."""
        return self._topic_name

    @topic_name.setter
    def topic_name(self, topic_name: Optional[str]) -> None:
        self._topic_name = topic_name

    @property
    def notification_topic_name(self) -> Optional[str]:
        """The topic used for feature group notifications."""
        return self._notification_topic_name

    @notification_topic_name.setter
    def notification_topic_name(self, notification_topic_name: Optional[str]) -> None:
        self._notification_topic_name = notification_topic_name

    @property
    def deprecated(self) -> bool:
        """Setting if the feature group is deprecated."""
        return self._deprecated

    @deprecated.setter
    def deprecated(self, deprecated: bool) -> None:
        self._deprecated = deprecated

    @property
    def subject(self) -> Dict[str, Any]:
        """Subject of the feature group."""
        if self._subject is None:
            # cache the schema
            self._subject = self._feature_group_engine.get_subject(self)
        return self._subject

    @property
    def avro_schema(self) -> str:
        """Avro schema representation of the feature group."""
        return self.subject["schema"]

    def get_complex_features(self) -> List[str]:
        """Returns the names of all features with a complex data type in this
        feature group.

        !!! example
            ```python
            complex_dtype_features = fg.get_complex_features()
            ```
        """
        return [f.name for f in self.features if f.is_complex()]

    def _get_encoded_avro_schema(self) -> str:
        complex_features = self.get_complex_features()
        schema = json.loads(self.avro_schema)

        for field in schema["fields"]:
            if field["name"] in complex_features:
                field["type"] = ["null", "bytes"]

        schema_s = json.dumps(schema)
        try:
            avro.schema.parse(schema_s)
        except avro.schema.SchemaParseException as e:
            raise FeatureStoreException(
                "Failed to construct Avro Schema: {}".format(e)
            ) from e
        return schema_s

    def _get_feature_avro_schema(self, feature_name: str) -> str:
        for field in json.loads(self.avro_schema)["fields"]:
            if field["name"] == feature_name:
                return json.dumps(field["type"])

    @property
    def features(self) -> List["feature.Feature"]:
        """Feature Group schema (alias)"""
        return self._features

    @property
    def schema(self) -> List["feature.Feature"]:
        """Feature Group schema"""
        return self._features

    def _are_statistics_missing(self, statistics: Statistics) -> bool:
        if not self.statistics_config.enabled:
            return False
        elif statistics is None:
            return True
        if (
            self.statistics_config.histograms
            or self.statistics_config.correlations
            or self.statistics_config.exact_uniqueness
        ):
            # if statistics are missing, recompute and update statistics.
            # We need to check for missing statistics because the statistics config can have been modified
            for fds in statistics.feature_descriptive_statistics:
                if fds.feature_type in ["Integral", "Fractional"]:
                    if self.statistics_config.histograms and (
                        fds.extended_statistics is None
                        or "histogram" not in fds.extended_statistics
                    ):
                        return True

                    if self.statistics_config.correlations and (
                        fds.extended_statistics is None
                        or "correlations" not in fds.extended_statistics
                    ):
                        return True

                if self.statistics_config.exact_uniqueness and fds.uniqueness is None:
                    return True

        return False

    def _are_statistics_supported(self) -> bool:
        """Whether statistics are supported or not for the current Feature Group type"""
        return not isinstance(self, SpineGroup)

    def _check_statistics_support(self) -> None:
        """Check for statistics support on the current Feature Group type"""
        if not self._are_statistics_supported():
            raise FeatureStoreException(
                "Statistics not supported for this Feature Group type"
            )

    @features.setter
    def features(self, new_features: List["feature.Feature"]) -> None:
        self._features = new_features

    def _get_project_name(self) -> str:
        return util.strip_feature_store_suffix(self.feature_store_name)


@typechecked
class FeatureGroup(FeatureGroupBase):
    CACHED_FEATURE_GROUP = "CACHED_FEATURE_GROUP"
    STREAM_FEATURE_GROUP = "STREAM_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        name: str,
        version: Optional[int],
        featurestore_id: int,
        description: Optional[str] = "",
        partition_key: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        hudi_precombine_key: Optional[str] = None,
        featurestore_name: Optional[str] = None,
        embedding_index: Optional["EmbeddingIndex"] = None,
        created: Optional[str] = None,
        creator: Optional[Dict[str, Any]] = None,
        id: Optional[int] = None,
        features: Optional[List[Union["feature.Feature", Dict[str, Any]]]] = None,
        location: Optional[str] = None,
        online_enabled: bool = False,
        time_travel_format: Optional[str] = None,
        statistics_config: Optional[Union["StatisticsConfig", Dict[str, Any]]] = None,
        online_topic_name: Optional[str] = None,
        topic_name: Optional[str] = None,
        notification_topic_name: Optional[str] = None,
        event_time: Optional[str] = None,
        stream: bool = False,
        expectation_suite: Optional[
            Union[
                great_expectations.core.ExpectationSuite,
                ExpectationSuite,
                Dict[str, Any],
            ]
        ] = None,
        parents: Optional[List[explicit_provenance.Links]] = None,
        href: Optional[str] = None,
        delta_streamer_job_conf: Optional[
            Union[Dict[str, Any], deltastreamer_jobconf.DeltaStreamerJobConf]
        ] = None,
        deprecated: bool = False,
        transformation_functions: Optional[
            List[Union[TransformationFunction, HopsworksUdf]]
        ] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            version,
            featurestore_id,
            location,
            event_time=event_time,
            online_enabled=online_enabled,
            embedding_index=embedding_index,
            id=id,
            expectation_suite=expectation_suite,
            online_topic_name=online_topic_name,
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            deprecated=deprecated,
        )
        self._feature_store_name: Optional[str] = featurestore_name
        self._description: Optional[str] = description
        self._created = created
        self._creator = user.User.from_response_json(creator)

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._time_travel_format = (
            time_travel_format.upper() if time_travel_format is not None else None
        )

        self._stream = stream
        self._parents = parents
        self._deltastreamer_jobconf = delta_streamer_job_conf

        self._materialization_job: "Job" = None

        if self._id:
            # initialized by backend
            self.primary_key: List[str] = [
                feat.name for feat in self._features if feat.primary is True
            ]
            self._partition_key: List[str] = [
                feat.name for feat in self._features if feat.partition is True
            ]
            if (
                time_travel_format is not None
                and time_travel_format.upper() == "HUDI"
                and self._features
            ):
                # hudi precombine key is always a single feature
                self._hudi_precombine_key: Optional[str] = [
                    feat.name
                    for feat in self._features
                    if feat.hudi_precombine_key is True
                ][0]
            else:
                self._hudi_precombine_key: Optional[str] = None

            self.statistics_config = statistics_config

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
                        stacklevel=1,
                    )
                    self._time_travel_format = expected_format

            self.primary_key = primary_key
            self.partition_key = partition_key
            self._hudi_precombine_key = (
                util.autofix_feature_name(hudi_precombine_key)
                if hudi_precombine_key is not None
                and self._time_travel_format is not None
                and self._time_travel_format == "HUDI"
                else None
            )
            self.statistics_config = statistics_config

        self._feature_group_engine: "feature_group_engine.FeatureGroupEngine" = (
            feature_group_engine.FeatureGroupEngine(featurestore_id)
        )
        self._vector_db_client: Optional["VectorDbClient"] = None
        self._href: Optional[str] = href

        # cache for optimized writes
        self._kafka_producer: Optional["confluent_kafka.Producer"] = None
        self._feature_writers: Optional[Dict[str, callable]] = None
        self._writer: Optional[callable] = None
        self._kafka_headers: Optional[Dict[str, bytes]] = None
        # On-Demand Transformation Functions
        self._transformation_functions: List[TransformationFunction] = []

        if transformation_functions:
            for transformation_function in transformation_functions:
                if not isinstance(transformation_function, TransformationFunction):
                    self._transformation_functions.append(
                        TransformationFunction(
                            featurestore_id,
                            hopsworks_udf=transformation_function,
                            version=1,
                            transformation_type=UDFType.ON_DEMAND,
                        )
                    )
                else:
                    if not transformation_function.hopsworks_udf.udf_type:
                        transformation_function.hopsworks_udf.udf_type = (
                            UDFType.ON_DEMAND
                        )
                    self._transformation_functions.append(transformation_function)

        if self._transformation_functions:
            self._transformation_functions = (
                FeatureGroup._sort_transformation_functions(
                    self._transformation_functions
                )
            )

    @staticmethod
    def _sort_transformation_functions(
        transformation_functions: List[TransformationFunction],
    ) -> List[TransformationFunction]:
        """
        Function that sorts transformation functions in the order of the output column names.
        The list of transformation functions are sorted based on the output columns names to maintain consistent ordering.
        # Arguments
            transformation_functions:  `List[TransformationFunction]`. List of transformation functions to be sorted
        # Returns
            `List[TransformationFunction]`: List of transformation functions to be sorted
        """
        return sorted(transformation_functions, key=lambda x: x.output_column_names[0])

    def read(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        online: bool = False,
        dataframe_type: str = "default",
        read_options: Optional[dict] = None,
    ) -> Union[
        pd.DataFrame,
        np.ndarray,
        List[List[Any]],
        TypeVar("pyspark.sql.DataFrame"),
        TypeVar("pyspark.RDD"),
        pl.DataFrame,
    ]:
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
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                 Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                For python engine:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
                * key `"pandas_types"` and value `True` to retrieve columns as
                  [Pandas nullable types](https://pandas.pydata.org/docs/user_guide/integer_na.html)
                  rather than numpy/object(string) types (experimental).
                Defaults to `{}`.

        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`. A Spark DataFrame.
            `pandas.DataFrame`. A Pandas DataFrame.
            `polars.DataFrame`. A Polars DataFrame.
            `numpy.ndarray`. A two-dimensional Numpy array.
            `list`. A two-dimensional Python list.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. No data is available for feature group with this commit date, If time travel enabled.
        """
        if wallclock_time and self._time_travel_format is None:
            raise FeatureStoreException(
                "Time travel format is not set for the feature group, cannot read as of specific point in time."
            )
        elif wallclock_time and engine.get_type() == "python":
            raise FeatureStoreException(
                "Python environments does not support incremental queries. "
                + "Read feature group without timestamp to retrieve latest snapshot or switch to "
                + "environment with Spark Engine."
            )

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
                    read_options or {},
                )
            )
        else:
            return self.select_all().read(
                online,
                dataframe_type,
                read_options or {},
            )

    def read_changes(
        self,
        start_wallclock_time: Union[str, int, datetime, date],
        end_wallclock_time: Union[str, int, datetime, date],
        read_options: Optional[dict] = None,
    ) -> Union[
        pd.DataFrame,
        np.ndarray,
        List[List[Any]],
        TypeVar("pyspark.sql.DataFrame"),
        TypeVar("pyspark.RDD"),
        pl.DataFrame,
    ]:
        """Reads updates of this feature that occurred between specified points in time.

        !!! warning "Deprecated"
                    `read_changes` method is deprecated. Use
                    `as_of(end_wallclock_time, exclude_until=start_wallclock_time).read(read_options=read_options)`
                    instead.

        !!! warning "Pyspark/Spark Only"
            Apache HUDI exclusively supports Time Travel and Incremental Query via Spark Context

        !!! warning
            This function only works for feature groups with time_travel_format='HUDI'.

        # Arguments
            start_wallclock_time: Start time of the time travel query. Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            end_wallclock_time: End time of the time travel query. Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
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
            .read(False, "default", read_options or {})
        )

    def find_neighbors(
        self,
        embedding: List[Union[int, float]],
        col: Optional[str] = None,
        k: Optional[int] = 10,
        filter: Optional[Union[Filter, Logic]] = None,
        options: Optional[dict] = None,
    ) -> List[Tuple[float, List[Any]]]:
        """
        Finds the nearest neighbors for a given embedding in the vector database.

        If `filter` is specified, or if embedding feature is stored in default project index,
        the number of results returned may be less than k. Try using a large value of k and extract the top k
        items from the results if needed.

        # Arguments
            embedding: The target embedding for which neighbors are to be found.
            col: The column name used to compute similarity score. Required only if there
            are multiple embeddings (optional).
            k: The number of nearest neighbors to retrieve (default is 10).
            filter: A filter expression to restrict the search space (optional).
            options: The options used for the request to the vector database.
                The keys are attribute values of the `hsfs.core.opensearch.OpensearchRequestOption` class.

        # Returns
            A list of tuples representing the nearest neighbors.
            Each tuple contains: `(The similarity score, A list of feature values)`

        !!! Example
            ```
            embedding_index = EmbeddingIndex()
            embedding_index.add_embedding(name="user_vector", dimension=3)
            fg = fs.create_feature_group(
                        name='air_quality',
                        embedding_index = embedding_index,
                        version=1,
                        primary_key=['id1'],
                        online_enabled=True,
                    )
            fg.insert(data)
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
            )

            # apply filter
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
                filter=(fg.id1 > 10) & (fg.id1 < 30)
            )
            ```
        """
        if self._vector_db_client is None and self._embedding_index:
            self._vector_db_client = VectorDbClient(self.select_all())
        results = self._vector_db_client.find_neighbors(
            embedding,
            feature=(self.__getattr__(col) if col else None),
            k=k,
            filter=filter,
            options=options,
        )
        return [
            (result[0], [result[1][f.name] for f in self.features])
            for result in results
        ]

    def show(self, n: int, online: Optional[bool] = False) -> List[List[Any]]:
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
            pl.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[feature.Feature],
        ] = None,
        write_options: Optional[Dict[str, Any]] = None,
        validation_options: Optional[Dict[str, Any]] = None,
        wait: bool = False,
    ) -> Tuple[
        Optional["Job"],
        Optional[great_expectations.core.ExpectationSuiteValidationResult],
    ]:
        """Persist the metadata and materialize the feature group to the feature store.

        !!! warning "Changed in 3.3.0"
            `insert` and `save` methods are now async by default in non-spark clients.
            To achieve the old behaviour, set `wait` argument to `True`.

        Calling `save` creates the metadata for the feature group in the feature store.
        If a Pandas DataFrame, Polars DatFrame, RDD or Ndarray is provided, the data is written to the
        online/offline feature store as specified.
        By default, this writes the feature group to the offline storage, and if
        `online_enabled` for the feature group, also to the online feature store.
        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame,
        or a two-dimensional Numpy array or a two-dimensional Python nested list.
        # Arguments
            features: Pandas DataFrame, Polars DataFrame, RDD, Ndarray or a list of features. Features to be saved.
                This argument is optional if the feature list is provided in the create_feature_group or
                in the get_or_create_feature_group method invokation.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to write data into the
                  feature group.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it does not wait.
                * key `start_offline_backfill` and value `True` or `False` to configure
                  whether or not to start the materialization job to write data to the offline
                  storage. `start_offline_backfill` is deprecated. Use `start_offline_materialization` instead.
                * key `start_offline_materialization` and value `True` or `False` to configure
                  whether or not to start the materialization job to write data to the offline
                  storage. By default the materialization job gets started immediately.
                * key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln)
                  used to configure the Kafka client. To optimize for throughput in high latency connection, consider
                  changing the [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                * key `internal_kafka` and value `True` or `False` in case you established
                  connectivity from you Python environment to the internal advertised
                  listeners of the Hopsworks Kafka Cluster. Defaults to `False` and
                  will use external listeners when connecting from outside of Hopsworks.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
            wait: Wait for job to finish before returning, defaults to `False`.
                Shortcut for read_options `{"wait_for_job": False}`.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to ingest the feature group data.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. Unable to create feature group.
        """
        if (features is None and len(self._features) > 0) or (
            isinstance(features, List)
            and len(features) > 0
            and all([isinstance(f, feature.Feature) for f in features])
        ):
            # This is done for compatibility. Users can specify the feature list in the
            # (get_or_)create_feature_group. Users can also provide the feature list in the save().
            # Though it's an optional parameter.
            # For consistency reasons if the user specify both the feature list in the (get_or_)create_feature_group
            # and in the `save()` call, then the (get_or_)create_feature_group wins.
            # This is consistent with the behavior of the insert method where the feature list wins over the
            # dataframe structure
            self._features = self._features if len(self._features) > 0 else features
            self._feature_group_engine.save_feature_group_metadata(
                self, None, write_options or {}
            )

            return None, None

        if features is None:
            raise FeatureStoreException(
                "Feature list not provided in the create_feature_group or get_or_create_feature_group invokations."
                + " Please provide a list of features or a Dataframe"
            )

        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        user_version = self._version

        if write_options is None:
            write_options = {}
        if "wait_for_job" not in write_options:
            write_options["wait_for_job"] = wait

        # fg_job is used only if the python engine is used
        fg_job, ge_report = self._feature_group_engine.save(
            self, feature_dataframe, write_options, validation_options or {}
        )
        if ge_report is None or ge_report.ingestion_result == "INGESTED":
            self._code_engine.save_code(self)

        if self.statistics_config.enabled and engine.get_type().startswith("spark"):
            # Only compute statistics if the engine is Spark.
            # For Python engine, the computation happens in the Hopsworks application
            self._statistics_engine.compute_and_save_statistics(self, feature_dataframe)
        if user_version is None:
            warnings.warn(
                "No version provided for creating feature group `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
                stacklevel=1,
            )
        return (
            fg_job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    def insert(
        self,
        features: Union[
            pd.DataFrame,
            pl.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        overwrite: bool = False,
        operation: Optional[str] = "upsert",
        storage: Optional[str] = None,
        write_options: Optional[Dict[str, Any]] = None,
        validation_options: Optional[Dict[str, Any]] = None,
        save_code: Optional[bool] = True,
        wait: bool = False,
    ) -> Tuple[Optional[Job], Optional[ValidationReport]]:
        """Persist the metadata and materialize the feature group to the feature store
        or insert data from a dataframe into the existing feature group.

        Incrementally insert data to a feature group or overwrite all data contained in the feature group. By
        default, the data is inserted into the offline storage as well as the online storage if the feature group is
        `online_enabled=True`.

        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame, a Polars DataFrame
        or a two-dimensional Numpy array or a two-dimensional Python nested list.
        If statistics are enabled, statistics are recomputed for the entire feature
        group.
        If feature group's time travel format is `HUDI` then `operation` argument can be
        either `insert` or `upsert`.

        If feature group doesn't exist the insert method will create the necessary metadata the first time it is
        invoked and writes the specified `features` dataframe as feature group to the online/offline feature store.

        !!! warning "Changed in 3.3.0"
            `insert` and `save` methods are now async by default in non-spark clients.
            To achieve the old behaviour, set `wait` argument to `True`.

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
            features: Pandas DataFrame, Polars DataFrame, RDD, Ndarray, list. Features to be saved.
            overwrite: Drop all data in the feature group before
                inserting new data. This does not affect metadata, defaults to False.
            operation: Apache Hudi operation type `"insert"` or `"upsert"`.
                Defaults to `"upsert"`.
            storage: Overwrite default behaviour, write to offline
                storage only with `"offline"` or online only with `"online"`, defaults
                to `None` (If the streaming APIs are enabled, specifying the storage option is not supported).
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
                  whether or not to start the materialization job to write data to the offline
                  storage. `start_offline_backfill` is deprecated. Use `start_offline_materialization` instead.
                * key `start_offline_materialization` and value `True` or `False` to configure
                  whether or not to start the materialization job to write data to the offline
                  storage. By default the materialization job gets started immediately.
                * key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln)
                  used to configure the Kafka client. To optimize for throughput in high latency connection consider
                  changing [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                * key `internal_kafka` and value `True` or `False` in case you established
                  connectivity from you Python environment to the internal advertised
                  listeners of the Hopsworks Kafka Cluster. Defaults to `False` and
                  will use external listeners when connecting from outside of Hopsworks.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                * key `fetch_expectation_suite` a boolean value, by default `True`, to control whether the expectation
                   suite of the feature group should be fetched before every insert.
            save_code: When running HSFS on Hopsworks or Databricks, HSFS can save the code/notebook used to create
                the feature group or used to insert data to it. When calling the `insert` method repeatedly
                with small batches of data, this can slow down the writes. Use this option to turn off saving
                code. Defaults to `True`.
            wait: Wait for job to finish before returning, defaults to `False`.
                Shortcut for read_options `{"wait_for_job": False}`.

        # Returns
            (`Job`, `ValidationReport`) A tuple with job information if python engine is used and the validation report if validation is enabled.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. e.g fail to create feature group, dataframe schema does not match
                existing feature group schema, etc.
            `hsfs.client.exceptions.DataValidationException`. If data validation fails and the expectation
                suite `validation_ingestion_policy` is set to `STRICT`. Data is NOT ingested.
        """
        if storage and self.stream:
            warnings.warn(
                "Specifying the storage option is not supported if the streaming APIs are enabled",
                stacklevel=1,
            )

        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        if validation_options is None:
            validation_options = {}
        if write_options is None:
            write_options = {}
        if "wait_for_job" not in write_options:
            write_options["wait_for_job"] = wait

        job, ge_report = self._feature_group_engine.insert(
            self,
            feature_dataframe=feature_dataframe,
            overwrite=overwrite,
            operation=operation,
            storage=storage.lower() if storage is not None else None,
            write_options=write_options,
            validation_options={"save_report": True, **validation_options},
        )
        if save_code and (
            ge_report is None or ge_report.ingestion_result == "INGESTED"
        ):
            self._code_engine.save_code(self)

        if engine.get_type().startswith("spark") and not self.stream:
            # Also, only compute statistics if stream is False.
            # if True, the backfill job has not been triggered and the data has not been inserted (it's in Kafka)
            self.compute_statistics()

        return (
            job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    def multi_part_insert(
        self,
        features: Optional[
            Union[
                pd.DataFrame,
                pl.DataFrame,
                TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
                TypeVar("pyspark.RDD"),  # noqa: F821
                np.ndarray,
                List[list],
            ]
        ] = None,
        overwrite: bool = False,
        operation: Optional[str] = "upsert",
        storage: Optional[str] = None,
        write_options: Optional[Dict[str, Any]] = None,
        validation_options: Optional[Dict[str, Any]] = None,
    ) -> Union[
        Tuple[Optional[Job], Optional[ValidationReport]],
        feature_group_writer.FeatureGroupWriter,
    ]:
        """Get FeatureGroupWriter for optimized multi part inserts or call this method
        to start manual multi part optimized inserts.

        In use cases where very small batches (1 to 1000) rows per Dataframe need
        to be written to the feature store repeatedly, it might be inefficient to use
        the standard `feature_group.insert()` method as it performs some background
        actions to update the metadata of the feature group object first.

        For these cases, the feature group provides the `multi_part_insert` API,
        which is optimized for writing many small Dataframes after another.

        There are two ways to use this API:
        !!! example "Python Context Manager"
            Using the Python `with` syntax you can acquire a FeatureGroupWriter
            object that implements the same `multi_part_insert` API.
            ```python
            feature_group = fs.get_or_create_feature_group("fg_name", version=1)

            with feature_group.multi_part_insert() as writer:
                # run inserts in a loop:
                while loop:
                    small_batch_df = ...
                    writer.insert(small_batch_df)
            ```
            The writer batches the small Dataframes and transmits them to Hopsworks
            efficiently.
            When exiting the context, the feature group writer is sure to exit
            only once all the rows have been transmitted.

        !!! example "Multi part insert with manual context management"
            Instead of letting Python handle the entering and exiting of the
            multi part insert context, you can start and finalize the context
            manually.
            ```python
            feature_group = fs.get_or_create_feature_group("fg_name", version=1)

            while loop:
                small_batch_df = ...
                feature_group.multi_part_insert(small_batch_df)

            # IMPORTANT: finalize the multi part insert to make sure all rows
            # have been transmitted
            feature_group.finalize_multi_part_insert()
            ```
            Note that the first call to `multi_part_insert` initiates the context
            and be sure to finalize it. The `finalize_multi_part_insert` is a
            blocking call that returns once all rows have been transmitted.

            Once you are done with the multi part insert, it is good practice to
            start the materialization job in order to write the data to the offline
            storage:
            ```python
            feature_group.materialization_job.run(await_termination=True)
            ```

        # Arguments
            features: Pandas DataFrame, Polars DataFrame, RDD, Ndarray, list. Features to be saved.
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
                  whether or not to start the materialization job to write data to the offline
                  storage. `start_offline_backfill` is deprecated. Use `start_offline_materialization` instead.
                * key `start_offline_materialization` and value `True` or `False` to configure
                  whether or not to start the materialization job to write data to the offline
                  storage. By default the materialization job does not get started automatically
                  for multi part inserts.
                * key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln)
                  used to configure the Kafka client. To optimize for throughput in high latency connection consider
                  changing [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                * key `internal_kafka` and value `True` or `False` in case you established
                  connectivity from you Python environment to the internal advertised
                  listeners of the Hopsworks Kafka Cluster. Defaults to `False` and
                  will use external listeners when connecting from outside of Hopsworks.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                * key `fetch_expectation_suite` a boolean value, by default `False` for multi part inserts,
                   to control whether the expectation suite of the feature group should be fetched before every insert.

        # Returns
            (`Job`, `ValidationReport`) A tuple with job information if python engine is used and the validation report if validation is enabled.
            `FeatureGroupWriter` When used as a context manager with Python `with` statement.
        """
        self._multi_part_insert = True
        multi_part_writer = feature_group_writer.FeatureGroupWriter(self)
        if features is None:
            return multi_part_writer
        else:
            # go through writer to avoid setting multi insert defaults again
            return multi_part_writer.insert(
                features,
                overwrite,
                operation,
                storage,
                write_options or {},
                validation_options or {},
            )

    def finalize_multi_part_insert(self) -> None:
        """Finalizes and exits the multi part insert context opened by `multi_part_insert`
        in a blocking fashion once all rows have been transmitted.

        !!! example "Multi part insert with manual context management"
            Instead of letting Python handle the entering and exiting of the
            multi part insert context, you can start and finalize the context
            manually.
            ```python
            feature_group = fs.get_or_create_feature_group("fg_name", version=1)

            while loop:
                small_batch_df = ...
                feature_group.multi_part_insert(small_batch_df)

            # IMPORTANT: finalize the multi part insert to make sure all rows
            # have been transmitted
            feature_group.finalize_multi_part_insert()
            ```
            Note that the first call to `multi_part_insert` initiates the context
            and be sure to finalize it. The `finalize_multi_part_insert` is a
            blocking call that returns once all rows have been transmitted.
        """
        if self._kafka_producer is not None:
            self._kafka_producer.flush()
            self._kafka_producer = None
        self._feature_writers = None
        self._writer = None
        self._kafka_headers = None
        self._multi_part_insert = False

    def insert_stream(
        self,
        features: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        query_name: Optional[str] = None,
        output_mode: Optional[str] = "append",
        await_termination: bool = False,
        timeout: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        write_options: Optional[Dict[str, Any]] = None,
    ) -> TypeVar("StreamingQuery"):
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
                stacklevel=1,
            )

            return self._feature_group_engine.insert_stream(
                self,
                feature_dataframe,
                query_name,
                output_mode,
                await_termination,
                timeout,
                checkpoint_dir,
                write_options or {},
            )

    def commit_details(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Dict[str, str]]:
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
        write_options: Optional[Dict[Any, Any]] = None,
    ) -> None:
        """Drops records present in the provided DataFrame and commits it as update to this
        Feature group. This method can only be used on feature groups stored as HUDI or DELTA.

        # Arguments
            delete_df: dataFrame containing records to be deleted.
            write_options: User provided write options. Defaults to `{}`.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        self._feature_group_engine.commit_delete(self, delete_df, write_options or {})

    def as_of(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        exclude_until: Optional[Union[str, int, datetime, date]] = None,
    ) -> "query.Query":
        """Get Query object to retrieve all features of the group at a point in the past.

        !!! warning "Pyspark/Spark Only"
            Apache HUDI exclusively supports Time Travel and Incremental Query via Spark Context

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
        return self.select_all().as_of(
            wallclock_time=wallclock_time, exclude_until=exclude_until
        )

    def get_statistics_by_commit_window(
        self,
        from_commit_time: Optional[Union[str, int, datetime, date]] = None,
        to_commit_time: Optional[Union[str, int, datetime, date]] = None,
        feature_names: Optional[List[str]] = None,
    ) -> Optional[Union["Statistics", List["Statistics"]]]:
        """Returns the statistics computed on a specific commit window for this feature group. If time travel is not enabled, it raises an exception.

        If `from_commit_time` is `None`, the commit window starts from the first commit.
        If `to_commit_time` is `None`, the commit window ends at the last commit.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...
            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)
            fg_statistics = fg.get_statistics_by_commit_window(from_commit_time=None, to_commit_time=None)
            ```
        # Arguments
            to_commit_time: Date and time of the last commit of the window. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.
            from_commit_time: Date and time of the first commit of the window. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.
            feature_names: List of feature names of which statistics are retrieved.
        # Returns
            `Statistics`. Statistics object.
        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        if not self._is_time_travel_enabled():
            raise ValueError("Time travel is not enabled for this feature group")
        return self._statistics_engine.get_by_time_window(
            self,
            start_commit_time=from_commit_time,
            end_commit_time=to_commit_time,
            feature_names=feature_names,
        )

    def compute_statistics(
        self, wallclock_time: Optional[Union[str, int, datetime, date]] = None
    ) -> None:
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
            if self._is_time_travel_enabled() or wallclock_time is not None:
                wallclock_time = wallclock_time or datetime.now()
                # Retrieve fg commit id related to this wall clock time and recompute statistics. It will throw
                # exception if its not time travel enabled feature group.
                fg_commit_id = [
                    commit_id
                    for commit_id in self._feature_group_engine.commit_details(
                        self, wallclock_time, 1
                    ).keys()
                ][0]
                registered_stats = self.get_statistics_by_commit_window(
                    to_commit_time=fg_commit_id
                )
                if registered_stats is not None and self._are_statistics_missing(
                    registered_stats
                ):
                    registered_stats = None
                # Don't read the dataframe here, to avoid triggering a read operation
                # for the Python engine. The Python engine is going to setup a Spark Job
                # to update the statistics.
                return (
                    registered_stats
                    or self._statistics_engine.compute_and_save_statistics(
                        self,
                        feature_group_commit_id=fg_commit_id,
                    )
                )
        return super().compute_statistics()

    @classmethod
    def from_response_json(
        cls, json_dict: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Union["FeatureGroup", List["FeatureGroup"]]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            if "type" in json_decamelized:
                json_decamelized["stream"] = (
                    json_decamelized["type"] == "streamFeatureGroupDTO"
                )
            _ = json_decamelized.pop("type", None)
            json_decamelized.pop("validation_type", None)
            if "embedding_index" in json_decamelized:
                json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                    json_decamelized["embedding_index"]
                )
            if "transformation_functions" in json_decamelized:
                transformation_functions = json_decamelized["transformation_functions"]
                json_decamelized["transformation_functions"] = [
                    TransformationFunction.from_response_json(
                        {
                            **transformation_function,
                            "transformation_type": UDFType.ON_DEMAND,
                        }
                    )
                    for transformation_function in transformation_functions
                ]
            return cls(**json_decamelized)
        for fg in json_decamelized:
            if "type" in fg:
                fg["stream"] = fg["type"] == "streamFeatureGroupDTO"
            _ = fg.pop("type", None)
            fg.pop("validation_type", None)
            if "embedding_index" in fg:
                fg["embedding_index"] = EmbeddingIndex.from_response_json(
                    fg["embedding_index"]
                )
            if "transformation_functions" in fg:
                transformation_functions = fg["transformation_functions"]
                fg["transformation_functions"] = [
                    TransformationFunction.from_response_json(
                        {
                            **transformation_function,
                            "transformation_type": UDFType.ON_DEMAND,
                        }
                    )
                    for transformation_function in transformation_functions
                ]
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict: Dict[str, Any]) -> "FeatureGroup":
        json_decamelized = humps.decamelize(json_dict)
        json_decamelized["stream"] = json_decamelized["type"] == "streamFeatureGroupDTO"
        _ = json_decamelized.pop("type")
        if "embedding_index" in json_decamelized:
            json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                json_decamelized["embedding_index"]
            )
        if "transformation_functions" in json_decamelized:
            transformation_functions = json_decamelized["transformation_functions"]
            json_decamelized["transformation_functions"] = [
                TransformationFunction.from_response_json(
                    {
                        **transformation_function,
                        "transformation_type": UDFType.ON_DEMAND,
                    }
                )
                for transformation_function in transformation_functions
            ]
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        """Get specific Feature Group metadata in json format.

        !!! example
            ```python
            fg.json()
            ```
        """
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self) -> Dict[str, Any]:
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
            "topicName": self.topic_name,
            "notificationTopicName": self.notification_topic_name,
            "deprecated": self.deprecated,
            "transformationFunctions": self._transformation_functions,
        }
        if self.embedding_index:
            fg_meta_dict["embeddingIndex"] = self.embedding_index.to_dict()
        if self._stream:
            fg_meta_dict["deltaStreamerJobConf"] = self._deltastreamer_jobconf
        return fg_meta_dict

    def _get_table_name(self) -> str:
        return self.feature_store_name + "." + self.get_fg_name()

    def _is_time_travel_enabled(self) -> bool:
        """Whether time-travel is enabled or not"""
        return (
            self._time_travel_format is not None
            and self._time_travel_format.upper() == "HUDI"
        )

    @property
    def id(self) -> Optional[int]:
        """Feature group id."""
        return self._id

    @property
    def description(self) -> Optional[str]:
        """Description of the feature group contents."""
        return self._description

    @property
    def time_travel_format(self) -> Optional[str]:
        """Setting of the feature group time travel format."""
        return self._time_travel_format

    @property
    def partition_key(self) -> List[str]:
        """List of features building the partition key."""
        return self._partition_key

    @property
    def hudi_precombine_key(self) -> Optional[str]:
        """Feature name that is the hudi precombine key."""
        return self._hudi_precombine_key

    @property
    def feature_store_name(self) -> Optional[str]:
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name

    @property
    def creator(self) -> Optional["user.User"]:
        """Username of the creator."""
        return self._creator

    @property
    def created(self) -> Optional[str]:
        """Timestamp when the feature group was created."""
        return self._created

    @property
    def stream(self) -> bool:
        """Whether to enable real time stream writing capabilities."""
        return self._stream

    @property
    def parents(self) -> List["explicit_provenance.Links"]:
        """Parent feature groups as origin of the data in the current feature group.
        This is part of explicit provenance"""
        return self._parents

    @property
    def materialization_job(self) -> Optional["Job"]:
        """Get the Job object reference for the materialization job for this
        Feature Group."""
        if self._materialization_job is not None:
            return self._materialization_job
        else:
            feature_group_name = util.feature_group_name(self)
            job_suffix_list = ["materialization", "backfill"]
            for job_suffix in job_suffix_list:
                job_name = "{}_offline_fg_{}".format(feature_group_name, job_suffix)
                for _ in range(3):  # retry starting job
                    try:
                        self._materialization_job = job_api.JobApi().get(job_name)
                        return self._materialization_job
                    except RestAPIError as e:
                        if e.response.status_code == 404:
                            if e.response.json().get("errorCode", "") == 130009:
                                break  # no need to retry, since no such job exists
                            else:
                                time.sleep(1)  # backoff and then retry
                                continue
                        raise e
            raise FeatureStoreException("No materialization job was found")

    @property
    def statistics(self) -> "Statistics":
        """Get the latest computed statistics for the whole feature group."""
        if self._is_time_travel_enabled():
            # retrieve the latests statistics computed on the whole Feature Group, including all the commits.
            now = util.convert_event_time_to_timestamp(datetime.now())
            return self._statistics_engine.get_by_time_window(
                self,
                start_commit_time=None,
                end_commit_time=now,
            )
        return super().statistics

    @property
    def transformation_functions(
        self,
    ) -> List[TransformationFunction]:
        """Get transformation functions."""
        return self._transformation_functions

    @description.setter
    def description(self, new_description: Optional[str]) -> None:
        self._description = new_description

    @time_travel_format.setter
    def time_travel_format(self, new_time_travel_format: Optional[str]) -> None:
        self._time_travel_format = new_time_travel_format

    @partition_key.setter
    def partition_key(self, new_partition_key: List[str]) -> None:
        self._partition_key = [
            util.autofix_feature_name(pk) for pk in new_partition_key
        ]

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key: str) -> None:
        self._hudi_precombine_key = util.autofix_feature_name(hudi_precombine_key)

    @stream.setter
    def stream(self, stream: bool) -> None:
        self._stream = stream

    @parents.setter
    def parents(self, new_parents: "explicit_provenance.Links") -> None:
        self._parents = new_parents

    @transformation_functions.setter
    def transformation_functions(
        self,
        transformation_functions: List[TransformationFunction],
    ) -> None:
        self._transformation_functions = transformation_functions


@typechecked
class ExternalFeatureGroup(FeatureGroupBase):
    EXTERNAL_FEATURE_GROUP = "ON_DEMAND_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        storage_connector: Union[sc.StorageConnector, Dict[str, Any]],
        query: Optional[str] = None,
        data_format: Optional[str] = None,
        path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        version: Optional[int] = None,
        description: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        featurestore_id: Optional[int] = None,
        featurestore_name: Optional[str] = None,
        created: Optional[str] = None,
        creator: Optional[Dict[str, Any]] = None,
        id: Optional[int] = None,
        features: Optional[Union[List[Dict[str, Any]], List[feature.Feature]]] = None,
        location: Optional[str] = None,
        statistics_config: Optional[Union[StatisticsConfig, Dict[str, Any]]] = None,
        event_time: Optional[str] = None,
        expectation_suite: Optional[
            Union[
                ExpectationSuite,
                great_expectations.core.ExpectationSuite,
                Dict[str, Any],
            ]
        ] = None,
        online_enabled: bool = False,
        href: Optional[str] = None,
        online_topic_name: Optional[str] = None,
        topic_name: Optional[str] = None,
        notification_topic_name: Optional[str] = None,
        spine: bool = False,
        deprecated: bool = False,
        embedding_index: Optional[EmbeddingIndex] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            version,
            featurestore_id,
            location,
            event_time=event_time,
            online_enabled=online_enabled,
            id=id,
            embedding_index=embedding_index,
            expectation_suite=expectation_suite,
            online_topic_name=online_topic_name,
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            deprecated=deprecated,
        )

        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = user.User.from_response_json(creator)
        self._query = query
        self._data_format = data_format.upper() if data_format else None
        self._path = path

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._feature_group_engine: "external_feature_group_engine.ExternalFeatureGroupEngine" = external_feature_group_engine.ExternalFeatureGroupEngine(
            featurestore_id
        )

        if self._id:
            # Got from Hopsworks, deserialize features and storage connector
            self._features = (
                [
                    feature.Feature.from_response_json(feat)
                    if isinstance(feat, dict)
                    else feat
                    for feat in features
                ]
                if features
                else None
            )
            self.primary_key = (
                [feat.name for feat in self._features if feat.primary is True]
                if self._features
                else []
            )
            self.statistics_config = statistics_config

            self._options = (
                {option["name"]: option["value"] for option in options}
                if options
                else None
            )
        else:
            self.primary_key = primary_key
            self.statistics_config = statistics_config
            self._features = features
            self._options = options or {}

        if storage_connector is not None and isinstance(storage_connector, dict):
            self._storage_connector = sc.StorageConnector.from_response_json(
                storage_connector
            )
        else:
            self._storage_connector: "sc.StorageConnector" = storage_connector
        self._vector_db_client: Optional["VectorDbClient"] = None
        self._href: Optional[str] = href

    def save(self) -> None:
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
            self._statistics_engine.compute_and_save_statistics(self)

    def insert(
        self,
        features: Union[
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        write_options: Optional[Dict[str, Any]] = None,
        validation_options: Optional[Dict[str, Any]] = None,
        save_code: Optional[bool] = True,
        wait: bool = False,
    ) -> Tuple[
        None, Optional[great_expectations.core.ExpectationSuiteValidationResult]
    ]:
        """Insert the dataframe feature values ONLY in the online feature store.

        External Feature Groups contains metadata about feature data in an external storage system.
        External storage system are usually offline, meaning feature values cannot be retrieved in real-time.
        In order to use the feature values for real-time use-cases, you can insert them
        in Hopsoworks Online Feature Store via this method.

        The Online Feature Store has a single-entry per primary key value, meaining that providing a new value with
        for a given primary key will overwrite the existing value. No record of the previous value is kept.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the External Feature Group instance
            fg = fs.get_feature_group(name="external_sales_records", version=1)

            # get the feature values, e.g reading from csv files in a S3 bucket
            feature_values = ...

            # insert the feature values in the online feature store
            fg.insert(feature_values)
            ```

        !!! Note
            Data Validation via Great Expectation is supported if you have attached an expectation suite to
            your External Feature Group. However, as opposed to regular Feature Groups, this can lead to
            discrepancies between the data in the external storage system and the online feature store.

        # Arguments
            features: DataFrame, RDD, Ndarray, list. Features to be saved.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln)
                  used to configure the Kafka client. To optimize for throughput in high latency connection consider
                  changing [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                * key `internal_kafka` and value `True` or `False` in case you established
                  connectivity from you Python environment to the internal advertised
                  listeners of the Hopsworks Kafka Cluster. Defaults to `False` and
                  will use external listeners when connecting from outside of Hopsworks.
            validation_options: Additional validation options as key-value pairs, defaults to `{}`.
                * key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                * key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                * key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                * key `fetch_expectation_suite` a boolean value, by default `True`, to control whether the expectation
                   suite of the feature group should be fetched before every insert.
            save_code: When running HSFS on Hopsworks or Databricks, HSFS can save the code/notebook used to create
                the feature group or used to insert data to it. When calling the `insert` method repeatedly
                with small batches of data, this can slow down the writes. Use this option to turn off saving
                code. Defaults to `True`.

        # Returns
            Tuple(None, `ge.core.ExpectationSuiteValidationResult`) The validation report if validation is enabled.

        # Raises
            `hsfs.client.exceptions.RestAPIError`. e.g fail to create feature group, dataframe schema does not match
                existing feature group schema, etc.
            `hsfs.client.exceptions.DataValidationException`. If data validation fails and the expectation
                suite `validation_ingestion_policy` is set to `STRICT`. Data is NOT ingested.

        """
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        if validation_options is None:
            validation_options = {}
        if write_options is None:
            write_options = {}
        if "wait_for_job" not in write_options:
            write_options["wait_for_job"] = wait

        job, ge_report = self._feature_group_engine.insert(
            self,
            feature_dataframe=feature_dataframe,
            write_options=write_options,
            validation_options={"save_report": True, **validation_options},
        )

        if save_code and (
            ge_report is None or ge_report.ingestion_result == "INGESTED"
        ):
            self._code_engine.save_code(self)

        if self.statistics_config.enabled:
            warnings.warn(
                (
                    "Statistics are not computed for insertion to online enabled external feature group `{}`, with version"
                    " `{}`. Call `compute_statistics` explicitly to compute statistics over the data in the external storage system."
                ).format(self._name, self._version),
                util.StorageWarning,
                stacklevel=1,
            )

        return (
            job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    def read(
        self,
        dataframe_type: str = "default",
        online: bool = False,
        read_options: Optional[Dict[str, Any]] = None,
    ) -> Union[
        TypeVar("pyspark.sql.DataFrame"),
        TypeVar("pyspark.RDD"),
        pd.DataFrame,
        pl.DataFrame,
        np.ndarray,
    ]:
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
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            online: bool, optional. If `True` read from online feature store, defaults
                to `False`.
            read_options: Additional options as key/value pairs to pass to the spark engine.
                Defaults to `None`.
        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`. A Spark DataFrame.
            `pandas.DataFrame`. A Pandas DataFrame.
            `numpy.ndarray`. A two-dimensional Numpy array.
            `list`. A two-dimensional Python list.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """

        if (
            engine.get_type() == "python"
            and not online
            and not engine.get_instance().is_flyingduck_query_supported(
                self.select_all()
            )
        ):
            raise FeatureStoreException(
                "Reading an External Feature Group directly into a Pandas Dataframe using "
                + "Python/Pandas as Engine from the external storage system "
                + "is not supported, however, if the feature group is online enabled, you can read "
                + "from online storage or you can use the "
                + "Query API to create Feature Views/Training Data containing External "
                + "Feature Groups."
            )
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().read(
            dataframe_type=dataframe_type,
            online=online,
            read_options=read_options or {},
        )

    def show(self, n: int, online: bool = False) -> List[List[Any]]:
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

    def find_neighbors(
        self,
        embedding: List[Union[int, float]],
        col: Optional[str] = None,
        k: Optional[int] = 10,
        filter: Optional[Union[Filter, Logic]] = None,
        options: Optional[dict] = None,
    ) -> List[Tuple[float, List[Any]]]:
        """
        Finds the nearest neighbors for a given embedding in the vector database.

        If `filter` is specified, or if embedding feature is stored in default project index,
        the number of results returned may be less than k. Try using a large value of k and extract the top k
        items from the results if needed.

        # Arguments
            embedding: The target embedding for which neighbors are to be found.
            col: The column name used to compute similarity score. Required only if there
            are multiple embeddings (optional).
            k: The number of nearest neighbors to retrieve (default is 10).
            filter: A filter expression to restrict the search space (optional).
            options: The options used for the request to the vector database.
                The keys are attribute values of the `hsfs.core.opensearch.OpensearchRequestOption` class.
        # Returns
            A list of tuples representing the nearest neighbors.
            Each tuple contains: `(The similarity score, A list of feature values)`

        !!! Example
            ```
            embedding_index = EmbeddingIndex()
            embedding_index.add_embedding(name="user_vector", dimension=3)
            fg = fs.create_feature_group(
                        name='air_quality',
                        embedding_index = embedding_index,
                        version=1,
                        primary_key=['id1'],
                        online_enabled=True,
                    )
            fg.insert(data)
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
            )

            # apply filter
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
                filter=(fg.id1 > 10) & (fg.id1 < 30)
            )
            ```
        """
        if self._vector_db_client is None and self._embedding_index:
            self._vector_db_client = VectorDbClient(self.select_all())
        results = self._vector_db_client.find_neighbors(
            embedding,
            feature=(self.__getattr__(col) if col else None),
            k=k,
            filter=filter,
            options=options,
        )
        return [
            (result[0], [result[1][f.name] for f in self.features])
            for result in results
        ]

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> Union["ExternalFeatureGroup", List["ExternalFeatureGroup"]]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            _ = json_decamelized.pop("type", None)
            if "embedding_index" in json_decamelized:
                json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                    json_decamelized["embedding_index"]
                )
            return cls(**json_decamelized)
        for fg in json_decamelized:
            _ = fg.pop("type", None)
            if "embedding_index" in fg:
                fg["embedding_index"] = EmbeddingIndex.from_response_json(
                    fg["embedding_index"]
                )
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(
        self, json_dict: Dict[str, Any]
    ) -> "ExternalFeatureGroup":
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        if "embedding_index" in json_decamelized:
            json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                json_decamelized["embedding_index"]
            )
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self) -> Dict[str, Any]:
        fg_meta_dict = {
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
            "onlineEnabled": self._online_enabled,
            "spine": False,
            "topicName": self.topic_name,
            "notificationTopicName": self.notification_topic_name,
            "deprecated": self.deprecated,
        }
        if self.embedding_index:
            fg_meta_dict["embeddingIndex"] = self.embedding_index
        return fg_meta_dict

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def query(self) -> Optional[str]:
        return self._query

    @property
    def data_format(self) -> Optional[str]:
        return self._data_format

    @property
    def path(self) -> Optional[str]:
        return self._path

    @property
    def options(self) -> Optional[Dict[str, Any]]:
        return self._options

    @property
    def storage_connector(self) -> "sc.StorageConnector":
        return self._storage_connector

    @property
    def creator(self) -> Optional["user.User"]:
        return self._creator

    @property
    def created(self) -> Optional[str]:
        return self._created

    @description.setter
    def description(self, new_description: Optional[str]) -> None:
        self._description = new_description

    @property
    def feature_store_name(self) -> Optional[str]:
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name


@typechecked
class SpineGroup(FeatureGroupBase):
    SPINE_GROUP = "ON_DEMAND_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        storage_connector: Optional[
            Union["sc.StorageConnector", Dict[str, Any]]
        ] = None,
        query: Optional[str] = None,
        data_format: Optional[str] = None,
        path: Optional[str] = None,
        options: Dict[str, Any] = None,
        name: Optional[str] = None,
        version: Optional[int] = None,
        description: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        featurestore_id: Optional[int] = None,
        featurestore_name: Optional[str] = None,
        created: Optional[str] = None,
        creator: Optional[Dict[str, Any]] = None,
        id: Optional[int] = None,
        features: Optional[List[Union[feature.Feature, Dict[str, Any]]]] = None,
        location: Optional[str] = None,
        statistics_config: Optional[StatisticsConfig] = None,
        event_time: Optional[str] = None,
        expectation_suite: Optional[
            Union[ExpectationSuite, great_expectations.core.ExpectationSuite]
        ] = None,
        online_enabled: bool = False,
        href: Optional[str] = None,
        online_topic_name: Optional[str] = None,
        topic_name: Optional[str] = None,
        spine: bool = True,
        dataframe: Optional[str] = None,
        deprecated: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            version,
            featurestore_id,
            location,
            event_time=event_time,
            online_enabled=online_enabled,
            id=id,
            expectation_suite=expectation_suite,
            online_topic_name=online_topic_name,
            topic_name=topic_name,
            deprecated=deprecated,
        )

        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = user.User.from_response_json(creator)

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._feature_group_engine: "spine_group_engine.SpineGroupEngine" = (
            spine_group_engine.SpineGroupEngine(featurestore_id)
        )
        self._statistics_config = None

        if self._id:
            # Got from Hopsworks, deserialize features and storage connector
            self._features = (
                [
                    feature.Feature.from_response_json(feat)
                    if isinstance(feat, dict)
                    else feat
                    for feat in features
                ]
                if features
                else None
            )
            self.primary_key = (
                [feat.name for feat in self._features if feat.primary is True]
                if self._features
                else []
            )
        else:
            self.primary_key = primary_key
            self._features = features

        self._href = href

        # has to happen last -> features and id are needed for schema verification
        # use setter to convert to default dataframe type for engine
        self.dataframe = dataframe

    def _save(self) -> "SpineGroup":
        """Persist the metadata for this spine group.

        Without calling this method, your feature group will only exist
        in your Python Kernel, but not in Hopsworks.

        ```python
        query = "SELECT * FROM sales"

        fg = feature_store.create_spine_group(name="sales",
            version=1,
            description="Physical shop sales features",
            primary_key=['ss_store_sk'],
            event_time='sale_date',
            dataframe=df,
        )

        fg._save()
        """
        self._feature_group_engine.save(self)
        return self

    @property
    def dataframe(
        self,
    ) -> Optional[Union[pd.DataFrame, TypeVar("pyspark.sql.DataFrame")]]:
        """Spine dataframe with primary key, event time and
        label column to use for point in time join when fetching features.
        """
        return self._dataframe

    @dataframe.setter
    def dataframe(
        self,
        dataframe: Optional[
            Union[
                pd.DataFrame,
                pl.DataFrame,
                np.ndarray,
                TypeVar("pyspark.sql.DataFrame"),
                TypeVar("pyspark.RDD"),
            ]
        ],
    ) -> None:
        """Update the spine dataframe contained in the spine group."""
        if dataframe is None:
            warnings.warn(
                "Spine group dataframe is not set, use `spine_fg.dataframe = df` to set it"
                " before attempting to use it as a basis to join features. The dataframe will not"
                " be saved to Hopsworks when registering the spine group with `save` method.",
                UserWarning,
                stacklevel=1,
            )
            self._dataframe = (
                None  # if metadata fetched from backend the dataframe is not set
            )
        else:
            self._dataframe = engine.get_instance().convert_to_default_dataframe(
                dataframe
            )

        # in fs query the features are not sent, so then don't do validation
        if (
            self._id is not None
            and self._dataframe is not None
            and self._features is not None
        ):
            dataframe_features = engine.get_instance().parse_schema_feature_group(
                self._dataframe
            )
            self._feature_group_engine._verify_schema_compatibility(
                self._features, dataframe_features
            )

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> Union["SpineGroup", List["SpineGroup"]]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            _ = json_decamelized.pop("type", None)
            return cls(**json_decamelized)
        for fg in json_decamelized:
            _ = fg.pop("type", None)
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict: Dict[str, Any]) -> "SpineGroup":
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": "onDemandFeaturegroupDTO",
            "statisticsConfig": self._statistics_config,
            "eventTime": self._event_time,
            "spine": True,
            "topicName": self.topic_name,
            "deprecated": self.deprecated,
        }
