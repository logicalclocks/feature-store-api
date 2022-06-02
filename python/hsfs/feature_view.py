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
import warnings
from datetime import datetime
from typing import Optional, Union, List, Dict, Any

import humps

from hsfs import (
    util,
    training_dataset_feature,
    storage_connector,
    training_dataset,
)
from hsfs.constructor import query
from hsfs.core import (
    feature_view_engine,
    transformation_function_engine,
    vector_server,
    training_dataset_bundle,
)
from hsfs.transformation_function import TransformationFunction
from hsfs.statistics_config import StatisticsConfig


class FeatureView:
    ENTITY_TYPE = "featureview"

    def __init__(
        self,
        name: str,
        query,
        featurestore_id,
        id=None,
        version: Optional[int] = None,
        description: Optional[str] = "",
        labels: Optional[List[str]] = [],
        transformation_functions: Optional[Dict[str, TransformationFunction]] = {},
    ):
        self._name = name
        self._id = id
        self._query = query
        self._featurestore_id = featurestore_id
        self._version = version
        self._description = description
        self._labels = labels
        self._transformation_functions = transformation_functions
        self._features = []
        self._feature_view_engine = feature_view_engine.FeatureViewEngine(
            featurestore_id
        )
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(featurestore_id)
        )
        self._vector_server = None

    def delete(self):
        """Delete current feature view and all associated metadata.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature view **and** related training dataset **and** materialized data in HopsFS.

        # Raises
            `RestAPIError`.
        """
        self._feature_view_engine.delete(self.name, self.version)

    def update(self):
        # TODO feature view: wait for RestAPI
        return self

    def init_serving(
        self,
        training_dataset_version: Optional[int] = None,
        batch: Optional[bool] = False,
        external: Optional[bool] = False,
    ):
        """Initialise and cache parametrized prepared statement to
           retrieve feature vector from online feature store.

        # Arguments
            training_dataset_version: int, optional. Default to be 1. Transformation statistics
                are fetched from training dataset and apply in serving vector.
            batch: boolean, optional. If set to True, prepared statements will be
                initialised for retrieving serving vectors as a batch.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        """
        self._vector_server = vector_server.VectorServer(
            self._featurestore_id, training_dataset_version
        )
        self._vector_server.init_serving(self, batch, external)

    def get_batch_query(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ):
        """Get a query string of batch query.

        # Arguments
            start_time: Optional. Start time of the batch query.
            end_time: Optional. End time of the batch query.

        # Returns
            `str`: batch query
        """
        return self._feature_view_engine.get_batch_query_string(
            self, start_time, end_time
        )

    def get_feature_vector(
        self, entry: Dict[str, Any], external: Optional[bool] = False
    ):
        """Returns assembled serving vector from online feature store.

        # Arguments
            entry: dictionary of training dataset feature group primary key names as keys and values provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `list` List of feature values related to provided primary keys, ordered according to positions of this
            features in training dataset query.
        """
        if self._vector_server is None:
            self.init_serving(external=external)
        return self._vector_server.get_feature_vector(self, entry, external)

    def get_feature_vectors(
        self, entry: Dict[str, List[Any]], external: Optional[bool] = False
    ):
        """Returns assembled serving vectors in batches from online feature store.

        # Arguments
            entry: dict of feature group primary key names as keys and value as list of primary keys provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `List[list]` List of lists of feature values related to provided primary keys, ordered according to
            positions of this features in training dataset query.
        """
        if self._vector_server is None:
            self.init_serving(batch=True, external=external)
        return self._vector_server.get_feature_vectors(self, entry, external)

    def preview_feature_vector(self, external: Optional[bool] = False):
        """Returns a sample of assembled serving vector from online feature store.

        # Arguments
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `list` List of feature values, ordered according to positions of this
            features in training dataset query.
        """
        if self._vector_server is None:
            self.init_serving()
        return self._vector_server.get_preview_vectors(self, external, 1)

    def preview_feature_vectors(self, n: int, external: Optional[bool] = False):
        """Returns n samples of assembled serving vectors in batches from online feature store.

        # Arguments
            n: int. Number of feature vectors to return.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `List[list]` List of lists of feature values , ordered according to
            positions of this features in training dataset query.
        """
        if self._vector_server is None:
            self.init_serving()
        return self._vector_server.get_preview_vectors(self, external, n)

    def get_batch_data(self, start_time=None, end_time=None, read_options=None):
        """
        start_time: timestamp in second or wallclock_time: Datetime string. The String should be formatted in one of the
                following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, `%Y%m%d%H%M%S`, or `%Y%m%d%H%M%S%f`.
        end_time: timestamp in second or wallclock_time: Datetime string. The String should be formatted in one of the
                following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, `%Y%m%d%H%M%S`,  or `%Y%m%d%H%M%S%f`.
        read_options: User provided read options. Defaults to `{}`.

        """
        return self._feature_view_engine.get_batch_data(
            self, start_time, end_time, read_options
        )

    def add_tag(self, name: str, value):
        return self._feature_view_engine.add_tag(self, name, value)

    def get_tag(self, name: str):
        return self._feature_view_engine.get_tag(self, name)

    def get_tags(self):
        return self._feature_view_engine.get_tags(self)

    def delete_tag(self, name: str):
        return self._feature_view_engine.delete_tag(self, name)

    def get_training_dataset(
        self,
        version: Optional[int] = None,
        start_time: Optional = None,
        end_time: Optional = None,
        description: Optional[str] = "",
        splits: Optional[Dict[str, float]] = None,
        train_split: Optional[str] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
    ):
        """
        Get training data from storage or feature groups.

        If version is not provided or provided version has not already existed, it creates
        a new version of training data according to given arguments and returns a dataframe.

        If version is provided and has already existed, it reads training data from storage
        or feature groups and returns a dataframe. If split is provided, it reads the specific split.

        !!! info
        If a materialised training data has deleted. Use `recreate_training_dataset()` to
        recreate the training data.

        # Arguments
            start_time: timestamp in second or wallclock_time: Datetime string. The String should be formatted in one of the
                    following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, `%Y%m%d%H%M%S`, or `%Y%m%d%H%M%S%f`.
            end_time: timestamp in second or wallclock_time: Datetime string. The String should be formatted in one of the
                    following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, `%Y%m%d%H%M%S`,  or `%Y%m%d%H%M%S%f`.
            version: Version of the training dataset to retrieve, defaults to `None` and
                will create the training dataset with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            splits: A dictionary defining training dataset splits to be created. Keys in
                the dictionary define the name of the split as `str`, values represent
                percentage of samples in the split as `float`. Currently, only random
                splits are supported. Defaults to empty dict`{}`, creating only a single
                training dataset without splits.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            train_split: If `splits` is set, provide the name of the split that is going
                to be used for training. The statistics of this split will be used for
                transformation functions if necessary. Defaults to `None`.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to compute the training dataset.

        # Returns
            Training dataset tuple: (`int`, `dict(str, Dataframe)`)
                Training dataset version and Dictionary of dataframes where split is the key.

        """
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=version,
            event_start_time=start_time,
            event_end_time=end_time,
            description=description,
            storage_connector=None,
            featurestore_id=self._featurestore_id,
            data_format="csv",
            location="",
            splits=splits if splits else {},
            statistics_config=statistics_config,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            train_split=train_split,
        )
        # td_job is used only if the python engine is used
        td, df = self._feature_view_engine.get_training_data(
            self, td, read_options if read_options else {}, splits=splits
        )
        if version is None:
            warnings.warn(
                "No version provided for creating training dataset, incremented version to `{}`.".format(
                    td.version
                ),
                util.VersionWarning,
            )
        if splits:
            return training_dataset_bundle.TrainingDatasetBundle(
                td.version, training_dataset_splits=df, train_split=td.train_split
            )
        else:
            return training_dataset_bundle.TrainingDatasetBundle(
                td.version, training_dataset=df
            )

    def create_training_dataset(
        self,
        start_time: Optional = None,
        end_time: Optional = None,
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        version: Optional[int] = None,
        description: Optional[str] = "",
        data_format: Optional[str] = "csv",
        coalesce: Optional[bool] = False,
        splits: Optional[Dict[str, float]] = {},
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        train_split: str = None,
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Create a training dataset and save data into `location`.

                !!! info "Data Formats"
                    The feature store currently supports the following data formats for
                    training datasets:

                    1. tfrecord
                    2. csv
                    3. tsv
                    4. parquet
                    5. avro
                    6. orc

                    Currently not supported petastorm, hdf5 and npy file formats.


        # Arguments
            start_time: timestamp in second or wallclock_time: Datetime string. The String should be formatted in one of the
                    following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, `%Y%m%d%H%M%S`, or `%Y%m%d%H%M%S%f`.
            end_time: timestamp in second or wallclock_time: Datetime string. The String should be formatted in one of the
                    following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, `%Y%m%d%H%M%S`,  or `%Y%m%d%H%M%S%f`.
            storage_connector: Storage connector defining the sink location for the
                training dataset, defaults to `None`, and materializes training dataset
                on HopsFS.
            location: Path to complement the sink storage connector with, e.g if the
                storage connector points to an S3 bucket, this path can be used to
                define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the
                storage connector.
            version: Version of the training dataset to retrieve, defaults to `None` and
                will create the training dataset with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            data_format: The data format used to save the training dataset,
                defaults to `"csv"`-format.
            coalesce: If true the training dataset data will be coalesced into
                a single partition before writing. The resulting training dataset
                will be a single file per split. Default False.
            splits: A dictionary defining training dataset splits to be created. Keys in
                the dictionary define the name of the split as `str`, values represent
                percentage of samples in the split as `float`. Currently, only random
                splits are supported. Defaults to empty dict`{}`, creating only a single
                training dataset without splits.
            seed: Optionally, define a seed to create the random splits with, in order
                to guarantee reproducability, defaults to `None`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            train_split: If `splits` is set, provide the name of the split that is going
                to be used for training. The statistics of this split will be used for
                transformation functions if necessary. Defaults to `None`.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.
        """
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=version,
            event_start_time=start_time,
            event_end_time=end_time,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._featurestore_id,
            splits=splits,
            seed=seed,
            statistics_config=statistics_config,
            coalesce=coalesce,
            train_split=train_split,
        )
        # td_job is used only if the python engine is used
        td, td_job = self._feature_view_engine.create_training_dataset(
            self, td, write_options
        )
        if version is None:
            warnings.warn(
                "No version provided for creating training dataset, incremented version to `{}`.".format(
                    td.version
                ),
                util.VersionWarning,
            )

        return training_dataset_bundle.TrainingDatasetBundle(td.version, job=td_job)

    def recreate_training_dataset(
        self, version: int, write_options: Optional[Dict[Any, Any]] = None
    ):
        td, td_job = self._feature_view_engine.recreate_training_dataset(
            self, version, write_options
        )
        return training_dataset_bundle.TrainingDatasetBundle(td.version, job=td_job)

    def add_training_dataset_tag(self, training_dataset_version: int, name: str, value):
        return self._feature_view_engine.add_tag(
            self, name, value, training_dataset_version=training_dataset_version
        )

    def get_training_dataset_tag(self, training_dataset_version: int, name: str):
        return self._feature_view_engine.get_tag(
            self, name, training_dataset_version=training_dataset_version
        )

    def get_training_dataset_tags(self, training_dataset_version: int):
        return self._feature_view_engine.get_tags(
            self, training_dataset_version=training_dataset_version
        )

    def delete_training_dataset_tag(self, training_dataset_version: int, name: str):
        return self._feature_view_engine.delete_tag(
            self, name, training_dataset_version=training_dataset_version
        )

    def purge_training_data(self, version: int):
        self._feature_view_engine.delete_training_dataset_only(
            self, training_data_version=version
        )

    def purge_all_training_data(self):
        self._feature_view_engine.delete_training_dataset_only(self)

    def delete_training_dataset(self, version: int):
        self._feature_view_engine.delete_training_data(
            self, training_data_version=version
        )

    def delete_all_training_datasets(self):
        self._feature_view_engine.delete_training_data(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        fv = cls(
            id=json_decamelized.get("id", None),
            name=json_decamelized["name"],
            query=query.Query.from_response_json(json_decamelized["query"]),
            featurestore_id=json_decamelized["featurestore_id"],
            version=json_decamelized.get("version", None),
            description=json_decamelized.get("description", None),
        )
        features = json_decamelized.get("features", None)
        if features:
            features = [
                training_dataset_feature.TrainingDatasetFeature.from_response_json(
                    feature
                )
                for feature in features
            ]
        fv.schema = features
        fv.labels = [feature.name for feature in features if feature.label]
        return fv

    def update_from_response_json(self, json_dict):
        other = self.from_response_json(json_dict)
        for key in [
            "name",
            "description",
            "id",
            "query",
            "featurestore_id",
            "version",
            "labels",
            "schema",
        ]:
            self._update_attribute_if_present(self, other, key)
        return self

    @staticmethod
    def _update_attribute_if_present(this, new, key):
        if getattr(new, key):
            setattr(this, key, getattr(new, key))

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "query": self._query,
            "features": self._features,
        }

    @property
    def id(self):
        """Feature view id."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def featurestore_id(self):
        """Feature store id."""
        return self._featurestore_id

    @featurestore_id.setter
    def featurestore_id(self, id):
        self._featurestore_id = id

    @property
    def name(self):
        """Name of the feature view."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        """Version number of the feature view."""
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def labels(self):
        """The labels/prediction feature of the feature view.

        Can be a composite of multiple features.
        """
        return self._labels

    @labels.setter
    def labels(self, labels):
        self._labels = [lb.lower() for lb in labels]

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        """Description of the feature view."""
        self._description = description

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query_obj):
        """Query of the feature view."""
        self._query = query_obj

    @property
    def transformation_functions(self):
        """Set transformation functions."""
        if self._id is not None and self._transformation_functions is None:
            self._transformation_functions = (
                self._transformation_function_engine.get_td_transformation_fn(self)
            )
        return self._transformation_functions

    @transformation_functions.setter
    def transformation_functions(self, transformation_functions):
        self._transformation_functions = transformation_functions

    @property
    def schema(self):
        """Feature view schema."""
        return self._features

    @schema.setter
    def schema(self, features):
        self._features = features
