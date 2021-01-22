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
import warnings
from typing import Optional, Union, Any, Dict, List, TypeVar

import humps
import pandas as pd
import numpy as np

from hsfs import util, engine, training_dataset_feature
from hsfs.statistics_config import StatisticsConfig
from hsfs.storage_connector import StorageConnector
from hsfs.core import (
    training_dataset_api,
    training_dataset_engine,
    tfdata_engine,
    statistics_engine,
)
from hsfs.constructor import query


class TrainingDataset:
    HOPSFS = "HOPSFS_TRAINING_DATASET"
    EXTERNAL = "EXTERNAL_TRAINING_DATASET"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(
        self,
        name,
        version,
        data_format,
        location,
        featurestore_id,
        description=None,
        storage_connector=None,
        splits=None,
        seed=None,
        created=None,
        creator=None,
        features=None,
        statistics_config=None,
        featurestore_name=None,
        id=None,
        jobs=None,
        inode_id=None,
        training_dataset_type=None,
        from_query=None,
        querydto=None,
        label=None,
    ):
        self._id = id
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._seed = seed
        self._location = location
        self._from_query = from_query
        self._querydto = querydto
        self._feature_store_id = featurestore_id

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            featurestore_id
        )

        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            featurestore_id
        )

        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )

        # set up depending on user initialized or coming from backend response
        if training_dataset_type is None:
            # no type -> user init
            self._features = features
            self.storage_connector = storage_connector
            self.splits = splits
            self.statistics_config = statistics_config
            self._label = label
        else:
            # type available -> init from backend response
            # make rest call to get all connector information, description etc.
            self._storage_connector = StorageConnector.from_response_json(
                storage_connector
            )

            self._features = [
                training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
                for feat in features
            ]
            self._splits = splits
            self._training_dataset_type = training_dataset_type
            self._statistics_config = StatisticsConfig.from_response_json(
                statistics_config
            )
            self._label = [feat.name for feat in self._features if feat.label]

    def save(
        self,
        features: Union[
            query.Query,
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Materialize the training dataset to storage.

        This method materializes the training dataset either from a Feature Store
        `Query`, a Spark or Pandas `DataFrame`, a Spark RDD, two-dimensional Python
        lists or Numpy ndarrays.

        # Arguments
            features: Feature data to be materialized.
            write_options: Additional write options as key/value pairs.
                Defaults to `{}`.

        # Returns
            `TrainingDataset`: The updated training dataset metadata object, the
                previous `TrainingDataset` object on which you call `save` is also
                updated.

        # Raises
            `RestAPIError`: Unable to create training dataset metadata.
        """
        user_version = self._version
        user_stats_config = self._statistics_config
        self._training_dataset_engine.save(self, features, write_options)
        # currently we do not save the training dataset statistics config for training datasets
        self.statistics_config = user_stats_config
        if self.statistics_config.enabled and engine.get_type() == "spark":
            self._statistics_engine.compute_statistics(self, self.read())
        if user_version is None:
            warnings.warn(
                "No version provided for creating training dataset `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )
        return self

    def insert(
        self,
        features: Union[
            query.Query,
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        overwrite: bool,
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Insert additional feature data into the training dataset.

        This method appends data to the training dataset either from a Feature Store
        `Query`, a Spark or Pandas `DataFrame`, a Spark RDD, two-dimensional Python
        lists or Numpy ndarrays. The schemas must match for this operation.

        This can also be used to overwrite all data in an existing training dataset.

        # Arguments
            features: Feature data to be materialized.
            overwrite: Whether to overwrite the entire data in the training dataset.
            write_options: Additional write options as key/value pairs.
                Defaults to `{}`.

        # Returns
            `TrainingDataset`: The updated training dataset metadata object, the
                previous `TrainingDataset` object on which you call `save` is also
                updated.

        # Raises
            `RestAPIError`: Unable to create training dataset metadata.
        """
        self._training_dataset_engine.insert(self, features, write_options, overwrite)

        self.compute_statistics()

    def read(self, split=None, read_options={}):
        """Read the training dataset into a dataframe.

        It is also possible to read only a specific split.

        # Arguments
            split: Name of the split to read, defaults to `None`, reading the entire
                training dataset.
            read_options: Additional read options as key/value pairs, defaults to `{}`.
        # Returns
            `DataFrame`: The spark dataframe containing the feature data of the
                training dataset.
        """
        return self._training_dataset_engine.read(self, split, read_options)

    def compute_statistics(self):
        """Recompute the statistics for the training dataset and save them to the
        feature store.
        """
        if self.statistics_config.enabled and engine.get_type() == "spark":
            return self._statistics_engine.compute_statistics(self, self.read())

    def tf_data(
        self,
        target_name: str,
        split: Optional[str] = None,
        feature_names: Optional[list] = None,
        var_len_features: Optional[list] = [],
        is_training: Optional[bool] = True,
        cycle_length: Optional[int] = 2,
    ):
        """
        Returns an object with utility methods to read training dataset as `tf.data.Dataset` object and handle it for further processing.

        # Arguments
            target_name: Name of the target variable.
            split: Name of training dataset split. For example, `"train"`, `"test"` or `"val"`, defaults to `None`,
                returning the full training dataset.
            feature_names: Names of training variables, defaults to `None`.
            var_len_features: Feature names that have variable length and need to be returned as `tf.io.VarLenFeature`, defaults to `[]`.
            is_training: Whether it is for training, testing or validation. Defaults to `True`.
            cycle_length: Number of files to be read and deserialized in parallel, defaults to `2`.

        # Returns
            `TFDataEngine`. An object with utility methods to generate and handle `tf.data.Dataset` object.
        """
        return tfdata_engine.TFDataEngine(
            self,
            split=split,
            target_name=target_name,
            feature_names=feature_names,
            var_len_features=var_len_features,
            is_training=is_training,
            cycle_length=cycle_length,
        )

    def show(self, n: int, split: str = None):
        """Show the first `n` rows of the training dataset.

        You can specify a split from which to retrieve the rows.

        # Arguments
            n: Number of rows to show.
            split: Name of the split to show, defaults to `None`, showing the first rows
                when taking all splits together.
        """
        self.read(split).show(n)

    def add_tag(self, name: str, value: str = None):
        """Attach a name/value tag to a training dataset.

        A tag can consist of a name only or a name/value pair. Tag names are
        unique identifiers.

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added, defaults to `None`.
        """
        self._training_dataset_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag from a training dataset.

        Tag names are unique identifiers.

        # Arguments
            name: Name of the tag to be removed.
        """
        self._training_dataset_engine.delete_tag(self, name)

    def get_tag(self, name=None):
        """Get the tags of a training dataset.

        Tag names are unique identifiers. Returns all tags if no tag name is
        specified.

        # Arguments
            name: Name of the tag to get, defaults to `None`.
        # Returns
            `List[Tag]`. List of tags as name/value pairs.
        """
        return self._training_dataset_engine.get_tags(self, name)

    def update_statistics_config(self):
        """Update the statistics configuration of the training dataset.

        Change the `statistics_config` object and persist the changes by calling
        this method.

        # Returns
            `TrainingDataset`. The updated metadata object of the training dataset.

        # Raises
            `RestAPIError`.
        """
        self._training_dataset_engine.update_statistics_config(self)
        return self

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        # here we lose the information that the user set, e.g. write_options
        self.__init__(**json_decamelized)
        return self

    def _infer_training_dataset_type(self, connector_type):
        if connector_type == StorageConnector.HOPSFS or connector_type is None:
            return self.HOPSFS
        elif (
            connector_type == StorageConnector.S3
            or connector_type == StorageConnector.ADLS
        ):
            return self.EXTERNAL
        else:
            raise TypeError(
                "Storage connectors of type {} are currently not supported for training datasets.".format(
                    connector_type
                )
            )

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "dataFormat": self._data_format,
            "storageConnector": self._storage_connector,
            "location": self._location,
            "trainingDatasetType": self._training_dataset_type,
            "features": self._features,
            "splits": self._splits,
            "seed": self._seed,
            "queryDTO": self._querydto.to_dict() if self._querydto else None,
            "statisticsConfig": self._statistics_config,
        }

    @property
    def id(self):
        """Training dataset id."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        """Name of the training dataset."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        """Version number of the training dataset."""
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        """Description of the training dataset contents."""
        self._description = description

    @property
    def data_format(self):
        """File format of the training dataset."""
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._data_format = data_format

    @property
    def write_options(self):
        """User provided options to write training dataset."""
        return self._write_options

    @write_options.setter
    def write_options(self, write_options):
        self._write_options = write_options

    @property
    def storage_connector(self):
        """Storage connector."""
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector):
        if isinstance(storage_connector, StorageConnector):
            self._storage_connector = storage_connector
        elif storage_connector is None:
            # init empty connector, otherwise will have to handle it at serialization time
            self._storage_connector = StorageConnector(
                None, None, None, None, None, None, None, None
            )
        else:
            raise TypeError(
                "The argument `storage_connector` has to be `None` or of type `StorageConnector`, is of type: {}".format(
                    type(storage_connector)
                )
            )
        self._training_dataset_type = self._infer_training_dataset_type(
            self._storage_connector.connector_type
        )

    @property
    def splits(self):
        """Training dataset splits. `train`, `test` or `eval` and corresponding percentages."""
        return {split["name"]: split["percentage"] for split in self._splits}

    @splits.setter
    def splits(self, splits):
        # user api differs from how the backend expects the splits to be represented
        splits_list = [{"name": k, "percentage": v} for k, v in splits.items()]
        self._splits = splits_list

    @property
    def location(self):
        """Path to the training dataset location."""
        return self._location

    @location.setter
    def location(self, location):
        self._location = location

    @property
    def schema(self):
        """Training dataset schema."""
        return self._features

    @property
    def seed(self):
        """Seed."""
        return self._seed

    @seed.setter
    def seed(self, seed):
        self._seed = seed

    @property
    def statistics_config(self):
        """Statistics configuration object defining the settings for statistics
        computation of the training dataset."""
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
        """Get the latest computed statistics for the training dataset."""
        return self._statistics_engine.get_last(self)

    def get_statistics(self, commit_time: str = None):
        """Returns the statistics for this training dataset at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        # Arguments
            commit_time: Commit time in the format `YYYYMMDDhhmmss`, defaults to `None`.

        # Returns
            `Statistics`. Object with statistics information.
        """
        if commit_time is None:
            return self.statistics
        else:
            return self._statistics_engine.get(self, commit_time)

    @property
    def query(self):
        """Query to generate this training dataset from online feature store."""
        return self._training_dataset_engine.query(self, True, True)

    def get_query(self, online: bool = True, with_label: bool = False):
        """Returns the query used to generate this training dataset

        # Arguments
            online: boolean, optional. Return the query for the online storage, else
                for offline storage, defaults to `True` - for online storage.
            with_label: Indicator whether the query should contain features which were
                marked as prediction label/feature when the training dataset was
                created, defaults to `False`.

        # Returns
            `str`. Query string for the chosen storage used to generate this training
                dataset.
        """
        return self._training_dataset_engine.query(self, online, with_label)

    @property
    def label(self):
        """The label/prediction feature of the training dataset.

        Can be a composite of multiple features.
        """
        return self._label

    @label.setter
    def label(self, label):
        self._label = label

    @property
    def feature_store_id(self):
        return self._feature_store_id
