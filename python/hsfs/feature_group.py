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
import humps
import json
import warnings
import pandas as pd
import numpy as np
import avro.schema
from typing import Optional, Union, Any, Dict, List, TypeVar

from hsfs import util, engine, feature, user, storage_connector as sc
from hsfs.core import (
    feature_group_engine,
    statistics_engine,
    code_engine,
    data_validation_engine,
    on_demand_feature_group_engine,
    expectations_api,
)
from hsfs.statistics_config import StatisticsConfig
from hsfs.constructor import query, filter
from hsfs.client.exceptions import FeatureStoreException


class FeatureGroupBase:
    def __init__(self, featurestore_id, validation_type, location):
        self._validation_type = validation_type.upper()
        self._location = location
        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )
        self._code_engine = code_engine.CodeEngine(featurestore_id, self.ENTITY_TYPE)
        self._expectations_api = expectations_api.ExpectationsApi(
            featurestore_id, "featuregroups"
        )
        self._data_validation_engine = data_validation_engine.DataValidationEngine(
            featurestore_id, self.ENTITY_TYPE
        )

    def delete(self):
        """Drop the entire feature group along with its feature data.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature group **and** all the feature data in offline and online storage
            associated with it.

        # Raises
            `RestAPIError`.
        """
        self._feature_group_engine.delete(self)

    def select_all(self):
        """Select all features in the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a
        training dataset immediately.

        # Returns
            `Query`. A query object with all features of the feature group.
        """
        return query.Query(
            left_feature_group=self,
            left_features=self._features,
            feature_store_name=self._feature_store_name,
            feature_store_id=self._feature_store_id,
        )

    def select(self, features: List[Union[str, feature.Feature]] = []):
        """Select a subset of features of the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a training
        dataset with a subset of features of the feature group.

        # Arguments
            features: list, optional. A list of `Feature` objects or feature names as
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

    def select_except(self, features: List[Union[str, feature.Feature]] = []):
        """Select all features of the feature group except a few and return a query
        object.

        The query can be used to construct joins of feature groups or create a training
        dataset with a subset of features of the feature group.

        # Arguments
            features: list, optional. A list of `Feature` objects or feature names as
                strings to be selected, defaults to [], selecting all features.

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

        ```python
        from hsfs.feature import Feature

        fg.filter(Feature("weekly_sales") > 1000)
        ```

        If you are planning to join the filtered feature group later on with another
        feature group, make sure to select the filtered feature explicitly from the
        respective feature group:
        ```python
        fg.filter(fg.feature1 == 1).show(10)
        ```

        Composite filters require parenthesis:
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

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `RestAPIError` in case the backend fails to add the tag.
        """

        self._feature_group_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag attached to a feature group.

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `RestAPIError` in case the backend fails to delete the tag.
        """
        self._feature_group_engine.delete_tag(self, name)

    def get_tag(self, name: str):
        """Get the tags of a feature group.

        # Arguments
            name: Name of the tag to get.

        # Returns
            tag value

        # Raises
            `RestAPIError` in case the backend fails to retrieve the tag.
        """
        return self._feature_group_engine.get_tag(self, name)

    def get_tags(self):
        """Retrieves all tags attached to a feature group.

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `RestAPIError` in case the backend fails to retrieve the tags.
        """
        return self._feature_group_engine.get_tags(self)

    def get_feature(self, name: str):
        """Retrieve a `Feature` object from the schema of the feature group.

        There are several ways to access features of a feature group:

        ```python
        fg.feature1
        fg["feature1"]
        fg.get_feature("feature1")
        ```

        !!! note
            Attribute access to features works only for non-reserved names. For example
            features named `id` or `name` will not be accessible via `fg.name`, instead
            this will return the name of the feature group itself. Fall back on using
            the `get_feature` method.

        Args:
            name (str): [description]

        Returns:
            [type]: [description]
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

        # Returns
            `FeatureGroup`. The updated metadata object of the feature group.

        # Raises
            `RestAPIError`.
        """
        self._feature_group_engine.update_statistics_config(self)
        return self

    def update_description(self, description: str):
        """Update the description of the feature group.

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
        """Update a single feature in this feature group.

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

    def attach_expectation(self, expectation):
        """Attach a feature group expectation. If feature group validation is not already enabled, it will be enabled
        and set to the stricter setting.

        # Arguments
            name: The expectation name.

        # Returns
            `Expectation`. The expectation metadata object.

        """
        # Turn on validation for this FG and set stricter setting
        if self._validation_type == "NONE":
            self._validation_type = "STRICT"

        return self._expectations_api.attach(self, expectation.name)

    def detach_expectation(self, expectation):
        """Remove an expectation from a feature group.

        # Arguments
            name: The expectation name.

        # Returns
            `Expectation`. The expectation metadata object.

        """
        return self._expectations_api.detach(self, expectation.name)

    def get_expectations(self):
        """Get all feature group expectations.

        # Arguments
            name: The expectation name.

        # Returns
            `Expectation`. A list of expectation metadata objects.

        """
        return self._expectations_api.get(feature_group=self)

    def get_expectation(self, name: str):
        """Get attached expectation by name for this feature group. Name is unique across a feature store.

        # Arguments
            name: The expectation name.

        # Returns
            `Expectation`. The expectation metadata object.

        """
        return self._expectations_api.get(name, self)

    def get_validations(self, validation_time=None, commit_time=None):
        """Get feature group data validation results based on the attached expectations.

        # Arguments
           validation_time: The data validation time, when the data validation started.
           commit_time: The commit time of a time travel enabled feature group.

        # Returns
           `FeatureGroupValidation`. The feature group validation metadata object.

        """
        return self._data_validation_engine.get_validations(
            self, validation_time, commit_time
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

    def get_statistics(self, commit_time: str = None):
        """Returns the statistics for this feature group at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        # Arguments
            commit_time: Commit time in the format `YYYYMMDDhhmmss`, defaults to `None`.

        # Returns
            `Statistics`. Statistics object.

        # Raises
            `RestAPIError`.
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
        # Returns
            `Statistics`. The statistics metadata object.
        # Raises
            `RestAPIError`. Unable to persist the statistics.
        """
        if self.statistics_config.enabled:
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Hive engine. The Hive engine is going to setup a Spark Job
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
    def event_time(self, feature_name):
        self._event_time = feature_name

    @property
    def location(self):
        return self._location


class FeatureGroup(FeatureGroupBase):
    CACHED_FEATURE_GROUP = "CACHED_FEATURE_GROUP"
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
        validation_type="NONE",
        expectations=None,
        online_topic_name=None,
        event_time=None,
    ):
        super().__init__(featurestore_id, validation_type, location)

        self._feature_store_id = featurestore_id
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

        self._avro_schema = None
        self._online_topic_name = online_topic_name
        self._event_time = event_time

        if self._id:
            # initialized by backend
            self.primary_key = [
                feat.name for feat in self._features if feat.primary is True
            ]
            self._partition_key = [
                feat.name for feat in self._features if feat.partition is True
            ]
            if time_travel_format is not None and time_travel_format.upper() == "HUDI":
                # hudi precombine key is always a single feature
                self._hudi_precombine_key = [
                    feat.name
                    for feat in self._features
                    if feat.hudi_precombine_key is True
                ][0]
            else:
                self._hudi_precombine_key = None

            self.statistics_config = statistics_config

        else:
            # initialized by user
            self.primary_key = primary_key
            self.partition_key = partition_key
            self._hudi_precombine_key = (
                hudi_precombine_key.lower()
                if hudi_precombine_key is not None
                and time_travel_format is not None
                and time_travel_format.upper() == "HUDI"
                else None
            )
            self.statistics_config = statistics_config

        if expectations is not None:
            self._expectations_names = [
                expectation.name for expectation in expectations
            ]
        else:
            self._expectations_names = []

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(
            featurestore_id
        )

    def read(
        self,
        wallclock_time: Optional[str] = None,
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
            fs = connection.get_feature_store();
            fg = fs.get_feature_group("example_feature_group", 1)
            fg.read()
            ```
        !!! example "Read feature group as of specific point in time:"
            ```python
            fs = connection.get_feature_store();
            fg = fs.get_feature_group("example_feature_group", 1)
            fg.read("2020-10-20 07:34:11")
            ```
        # Arguments
            wallclock_time: Date string in the format of "YYYYMMDD" or "YYYYMMDDhhmmss".
                If Specified will retrieve feature group as of specific point in time.
                If not specified will return as of most recent time. Defaults to `None`.
            online: bool, optional. If `True` read from online feature store, defaults
                to `False`.
            dataframe_type: str, optional. Possible values are `"default"`, `"spark"`,
                `"pandas"`, `"numpy"` or `"python"`, defaults to `"default"`.
            read_options: Additional read options as key/value pairs, defaults to `{}`.

        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`. A Spark DataFrame.
            `pandas.DataFrame`. A Pandas DataFrame.
            `numpy.ndarray`. A two-dimensional Numpy array.
            `list`. A two-dimensional Python list.

        # Raises
            `RestAPIError`. No data is available for feature group with this commit date, If time travel enabled.
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
        start_wallclock_time: str,
        end_wallclock_time: str,
        read_options: Optional[dict] = {},
    ):
        """Reads updates of this feature that occurred between specified points in time.

        This function only works on feature groups with `HUDI` time travel format.

        !!! example "Reading commits incrementally between specified points in time:"
            ```python
            fs = connection.get_feature_store();
            fg = fs.get_feature_group("example_feature_group", 1)
            fg.read_changes("2020-10-20 07:31:38", "2020-10-20 07:34:11").show()
            ```

        # Arguments
            start_wallclock_time: Date string in the format of "YYYYMMDD" or
                "YYYYMMDDhhmmss".
            end_wallclock_time: Date string in the format of "YYYYMMDD" or
                "YYYYMMDDhhmmss".
            read_options: User provided read options. Defaults to `{}`.

        # Returns
            `DataFrame`. The spark dataframe containing the incremental changes of
            feature data.

        # Raises
            `RestAPIError`.  No data is available for feature group with this commit date.
            `FeatureStoreException`. If the feature group does not have `HUDI` time travel format
        """
        if (
            self._time_travel_format is None
            or self._time_travel_format.upper() != "HUDI"
        ):
            raise FeatureStoreException(
                "read_changes can only be used on time travel enabled feature groups"
            )

        return (
            self.select_all()
            .pull_changes(start_wallclock_time, end_wallclock_time)
            .read(False, "default", read_options)
        )

    def show(self, n: int, online: Optional[bool] = False):
        """Show the first `n` rows of the feature group.

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
    ):
        """Persist the metadata and materialize the feature group to the feature store.

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
                When using the `hive` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to write data into the
                  feature group.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `hive` engine, it returns the Hopsworks Job
                that was launched to ingest the feature group data.

        # Raises
            `RestAPIError`. Unable to create feature group.
        """
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        user_version = self._version

        # fg_job is used only if the hive engine is used
        fg_job = self._feature_group_engine.save(self, feature_dataframe, write_options)
        self._code_engine.save_code(self)
        if self.statistics_config.enabled and engine.get_type() == "spark":
            # Only compute statistics if the engine is Spark.
            # For Hive engine, the computation happens in the Hopsworks application
            self._statistics_engine.compute_statistics(self, feature_dataframe)
        if user_version is None:
            warnings.warn(
                "No version provided for creating feature group `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )
        return fg_job

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
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Insert data from a dataframe into the feature group.

        Incrementally insert data to a feature group or overwrite all data contained
        in the feature group. By default, the data is inserted into the offline storage
        as well as the online storage if the feature group is `online_enabled=True`. To
        insert only into the online storage, set `storage="online"`, or oppositely
        `storage="offline"`.

        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame,
        or a two-dimensional Numpy array or a two-dimensional Python nested list.

        If statistics are enabled, statistics are recomputed for the entire feature
        group.

        If feature group's time travel format is `HUDI` then `operation` argument can be
        either `insert` or `upsert`.

        !!! example "Upsert new feature data with time travel format `HUDI`:"
            ```python
            fs = conn.get_feature_store();
            fg = fs.get_feature_group("example_feature_group", 1)
            upsert_df = ...
            fg.insert(upsert_df)
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
                When using the `hive` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to write data into the
                  feature group.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the insert call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `FeatureGroup`. Updated feature group metadata object.
        """
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        self._feature_group_engine.insert(
            self,
            feature_dataframe,
            overwrite,
            operation,
            storage.lower() if storage is not None else None,
            write_options,
        )

        self._code_engine.save_code(self)
        if engine.get_type() == "spark":
            # Only compute statistics if the engine is Spark,
            # if Hive, the statistics are computed by the application doing the insert
            self.compute_statistics()

    def insert_stream(
        self,
        features: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        query_name: Optional[str] = None,
        output_mode: Optional[str] = "append",
        await_termination: Optional[bool] = False,
        timeout: Optional[int] = None,
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
                seconds. Defaults to `Fals`e.
            timeout: Only relevant in combination with `await_termination=True`.
                Defaults to `None`.
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
                write_options,
            )

    def commit_details(
        self, wallclock_time: Optional[str] = None, limit: Optional[int] = None
    ):
        """Retrieves commit timeline for this feature group. This method can only be used
        on time travel enabled feature groups

        # Arguments
            wallclock_time: Commit details as of specific point in time. Defaults to `None`.
            limit: Number of commits to retrieve. Defaults to `None`.

        # Returns
            `Dict[str, Dict[str, str]]`. Dictionary object of commit metadata timeline, where Key is commit id and value
            is `Dict[str, str]` with key value pairs of date committed on, number of rows updated, inserted and deleted.

        # Raises
            `RestAPIError`.
            `FeatureStoreException`. If the feature group does not have `HUDI` time travel format
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
            `RestAPIError`.
        """
        if (
            self._time_travel_format is None
            or self._time_travel_format.upper() != "HUDI"
        ):
            raise FeatureStoreException(
                "commit_delete_record can only be used on time travel enabled feature groups"
            )
        self._feature_group_engine.commit_delete(self, delete_df, write_options)

    def as_of(self, wallclock_time):
        """Get Query object to retrieve all features of the group at a point in the past.

        This method selects all features in the feature group and returns a Query object
        at the specified point in time. This can then either be read into a Dataframe
        or used further to perform joins or construct a training dataset.

        # Arguments
            wallclock_time: Datetime string. The String should be formatted in one of the
                following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.

        # Returns
            `Query`. The query object with the applied time travel condition.
        """
        return self.select_all().as_of(wallclock_time)

    def validate(
        self,
        dataframe: TypeVar("pyspark.sql.DataFrame") = None,  # noqa: F821
        log_activity=False,  # noqa: F821
    ):
        """Run validation based on the attached expectations

        # Arguments
            dataframe: The PySpark dataframe to run the data validation expectations against.
            log_activity: Whether to log the validation as a feature group activity.
                If a dataframe is not provided (None),
                the validation will be logged as a feature store activity.

        # Returns
            `FeatureGroupValidation`. The feature group validation metadata object.

        """
        # Activity is logged only if a the validation concerts the feature group and not a specific dataframe
        if dataframe is None:
            dataframe = self.read()
            log_activity = True
        return self._data_validation_engine.validate(self, dataframe, log_activity)

    def compute_statistics(self, wallclock_time: Optional[str] = None):
        """Recompute the statistics for the feature group and save them to the
        feature store.

        Statistics are only computed for data in the offline storage of the feature
        group.

        # Arguments
            wallclock_time: Date string in the format of "YYYYMMDD" or "YYYYMMDDhhmmss".
                Only valid if feature group is time travel enabled. If specified will recompute statistics on
                feature group as of specific point in time. If not specified then will compute statistics
                as of most recent time of this feature group. Defaults to `None`.

        # Returns
            `Statistics`. The statistics metadata object.

        # Raises
            `RestAPIError`. Unable to persist the statistics.
        """
        if self.statistics_config.enabled:
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Hive engine. The Hive engine is going to setup a Spark Job
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
            _ = json_decamelized.pop("type", None)
            return cls(**json_decamelized)
        for fg in json_decamelized:
            _ = fg.pop("type", None)
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "onlineEnabled": self._online_enabled,
            "timeTravelFormat": self._time_travel_format,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": "cachedFeaturegroupDTO",
            "statisticsConfig": self._statistics_config,
            "validationType": self._validation_type,
            "expectationsNames": self._expectations_names,
            "eventTime": self._event_time,
        }

    def _get_table_name(self):
        return self.feature_store_name + "." + self.name + "_" + str(self.version)

    def _get_online_table_name(self):
        return self.name + "_" + str(self.version)

    def get_complex_features(self):
        """Returns the names of all features with a complex data type in this
        feature group.
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
    def avro_schema(self):
        """Avro schema representation of the feature group."""
        if self._avro_schema is None:
            # cache the schema
            self._avro_schema = self._feature_group_engine.get_avro_schema(self)
        return self._avro_schema

    @property
    def validation_type(self):
        """Validation type, one of "STRICT", "WARNING", "ALL", "NONE"."""
        return self._validation_type

    @property
    def expectations_names(self):
        """The names of expectations attached to this feature group."""
        return self._expectations_names

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

    @validation_type.setter
    def validation_type(self, new_validation_type):
        if new_validation_type is None:
            self._validation_type = "NONE"
        else:
            self._validation_type = new_validation_type.upper()
        self._feature_group_engine.update_validation_type(self)

    @expectations_names.setter
    def expectations_names(self, new_expectations_names):
        self._expectations_names = new_expectations_names


class OnDemandFeatureGroup(FeatureGroupBase):
    ON_DEMAND_FEATURE_GROUP = "ON_DEMAND_FEATURE_GROUP"
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
        validation_type="NONE",
        expectations=None,
    ):
        super().__init__(featurestore_id, validation_type, location)

        self._feature_store_id = featurestore_id
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
        self._event_time = event_time

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._feature_group_engine = (
            on_demand_feature_group_engine.OnDemandFeatureGroupEngine(featurestore_id)
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

            self._options = (
                {option["name"]: option["value"] for option in options}
                if options
                else None
            )
        else:
            self.primary_key = primary_key
            self.statistics_config = statistics_config
            self._features = features
            self._options = options

        if storage_connector is not None and isinstance(storage_connector, dict):
            self._storage_connector = sc.StorageConnector.from_response_json(
                storage_connector
            )
        else:
            self._storage_connector = storage_connector

        if expectations is not None:
            self._expectations_names = [
                expectation.name for expectation in expectations
            ]
        else:
            self._expectations_names = []

    def save(self):
        self._feature_group_engine.save(self)
        self._code_engine.save_code(self)
        if self._validation_type != "NONE":
            self.validate()

        if self.statistics_config.enabled:
            self._statistics_engine.compute_statistics(self, self.read())

    def read(self, dataframe_type="default"):
        """Get the feature group as a DataFrame."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().read(dataframe_type=dataframe_type)

    def show(self, n):
        """Show the first n rows of the feature group."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().show(n)

    def validate(self):  # noqa: F821
        """Run validation based on the attached expectations

        # Returns
            `FeatureGroupValidation`. The feature group validation metadata object.

        """
        return self._data_validation_engine.validate(self, self.read(), True)

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
            "validationType": self._validation_type,
            "expectationsNames": self._expectations_names,
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

    @property
    def validation_type(self):
        """Validation type, one of "STRICT", "WARNING", "ALL", "NONE"."""
        return self._validation_type

    @property
    def expectations_names(self):
        """The names of expectations attached to this feature group."""
        return self._expectations_names

    @version.setter
    def version(self, version):
        self._version = version

    @description.setter
    def description(self, new_description):
        self._description = new_description

    @features.setter
    def features(self, new_features):
        self._features = new_features

    @validation_type.setter
    def validation_type(self, new_validation_type):
        if new_validation_type is None:
            self._validation_type = "NONE"
        else:
            self._validation_type = new_validation_type.upper()
        self._feature_group_engine.update_validation_type(self)

    @expectations_names.setter
    def expectations_names(self, new_expectations_names):
        self._expectations_names = new_expectations_names
