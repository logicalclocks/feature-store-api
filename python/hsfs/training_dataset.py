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
from hsfs.storage_connector import StorageConnector, HopsFSConnector
from hsfs.core import (
    training_dataset_api,
    training_dataset_engine,
    tfdata_engine,
    statistics_engine,
    code_engine,
    transformation_function_engine,
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
        coalesce=False,
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
        inode_id=None,
        training_dataset_type=None,
        from_query=None,
        querydto=None,
        label=None,
        transformation_functions=None,
    ):
        self._id = id
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._coalesce = coalesce
        self._seed = seed
        self._location = location
        self._from_query = from_query
        self._querydto = querydto
        self._feature_store_id = featurestore_id
        self._prepared_statement_engine = None
        self._prepared_statements = None
        self._serving_keys = None
        self._transformation_functions = transformation_functions

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            featurestore_id
        )

        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            featurestore_id
        )

        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )

        self._code_engine = code_engine.CodeEngine(featurestore_id, self.ENTITY_TYPE)

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(featurestore_id)
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
            self._label = [feat.name.lower() for feat in self._features if feat.label]

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
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `hive` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `hive` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `RestAPIError`: Unable to create training dataset metadata.
        """
        user_version = self._version
        user_stats_config = self._statistics_config
        # td_job is used only if the hive engine is used
        training_dataset, td_job = self._training_dataset_engine.save(
            self, features, write_options
        )
        self.storage_connector = training_dataset.storage_connector
        # currently we do not save the training dataset statistics config for training datasets
        self.statistics_config = user_stats_config
        self._code_engine.save_code(self)
        if self.statistics_config.enabled and engine.get_type() == "spark":
            self.compute_statistics()
        if user_version is None:
            warnings.warn(
                "No version provided for creating training dataset `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )

        return td_job

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
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `hive` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the insert call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `hive` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `RestAPIError`: Unable to create training dataset metadata.
        """
        # td_job is used only if the hive engine is used
        td_job = self._training_dataset_engine.insert(
            self, features, write_options, overwrite
        )

        self._code_engine.save_code(self)
        self.compute_statistics()

        return td_job

    def read(self, split=None, read_options={}):
        """Read the training dataset into a dataframe.

        It is also possible to read only a specific split.

        # Arguments
            split: Name of the split to read, defaults to `None`, reading the entire
                training dataset. If the training dataset has split, the `split` parameter
                is mandatory.
            read_options: Additional read options as key/value pairs, defaults to `{}`.
        # Returns
            `DataFrame`: The spark dataframe containing the feature data of the
                training dataset.
        """
        if self.splits and split is None:
            raise ValueError(
                "The training dataset has splits, please specify the split you want to read"
            )

        return self._training_dataset_engine.read(self, split, read_options)

    def compute_statistics(self):
        """Recompute the statistics for the training dataset and save them to the
        feature store.
        """
        if self.statistics_config.enabled and engine.get_type() == "spark":
            if self.splits:
                return self._statistics_engine.register_split_statistics(self)
            else:
                return self._statistics_engine.compute_statistics(self, self.read())

    def tf_data(
        self,
        target_name: str,
        split: Optional[str] = None,
        feature_names: Optional[list] = None,
        var_len_features: Optional[list] = [],
        is_training: Optional[bool] = True,
        cycle_length: Optional[int] = 2,
        deterministic: Optional[bool] = False,
        file_pattern: Optional[str] = "*.tfrecord*",
    ):
        """
        Returns an object with utility methods to read training dataset as `tf.data.Dataset` object and handle it for further processing.

        # Arguments
            target_name: Name of the target variable.
            split: Name of training dataset split. For example, `"train"`, `"test"` or `"val"`, defaults to `None`,
                returning the full training dataset.
            feature_names: Names of training variables, defaults to `None`.
            var_len_features: Feature names that have variable length and need to be returned as `tf.io.VarLenFeature`,
            defaults to `[]`. is_training: Whether it is for training, testing or validation. Defaults to `True`.
            cycle_length: Number of files to be read and deserialized in parallel, defaults to `2`.
            deterministic: Controls the order in which the transformation produces elements. If set to False, the
            transformation is allowed to yield elements out of order to trade determinism for performance.
            Defaults to `False`.
            file_pattern: Returns a list of files that match the given pattern Defaults to `*.tfrecord*`.

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
            deterministic=deterministic,
            file_pattern=file_pattern,
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

    def add_tag(self, name: str, value):
        """Attach a tag to a training dataset.

        A tag consists of a <name,value> pair. Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `RestAPIError` in case the backend fails to add the tag.
        """
        self._training_dataset_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag attached to a training dataset.

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `RestAPIError` in case the backend fails to delete the tag.
        """
        self._training_dataset_engine.delete_tag(self, name)

    def get_tag(self, name):
        """Get the tags of a training dataset.

        # Arguments
            name: Name of the tag to get.

        # Returns
            tag value

        # Raises
            `RestAPIError` in case the backend fails to retrieve the tag.
        """
        return self._training_dataset_engine.get_tag(self, name)

    def get_tags(self):
        """Returns all tags attached to a training dataset.

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `RestAPIError` in case the backend fails to retrieve the tags.
        """
        return self._training_dataset_engine.get_tags(self)

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

    def delete(self):
        """Delete training dataset and all associated metadata.

        !!! note "Drops only HopsFS data"
            Note that this operation drops only files which were materialized in
            HopsFS. If you used a Storage Connector for a cloud storage such as S3,
            the data will not be deleted, but you will not be able to track it anymore
            from the Feature Store.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            training dataset **and** and the materialized data in HopsFS.

        # Raises
            `RestAPIError`.
        """
        self._training_dataset_api.delete(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        for td in json_decamelized:
            _ = td.pop("type")
        return [cls(**td) for td in json_decamelized]

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
            "coalesce": self._coalesce,
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
    def coalesce(self):
        """If true the training dataset data will be coalesced into
        a single partition before writing. The resulting training dataset
        will be a single file per split"""
        return self._coalesce

    @coalesce.setter
    def coalesce(self, coalesce):
        self._coalesce = coalesce

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
            self._storage_connector = HopsFSConnector(
                None, None, None, None, None, None
            )
        else:
            raise TypeError(
                "The argument `storage_connector` has to be `None` or of type `StorageConnector`, is of type: {}".format(
                    type(storage_connector)
                )
            )
        self._training_dataset_type = self._infer_training_dataset_type(
            self._storage_connector.type
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

    def init_prepared_statement(self, external: Optional[bool] = False):
        """Initialise and cache parametrized prepared statement to
           retrieve feature vector from online feature store.

        # Arguments
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        """
        if self.prepared_statements is None:
            self._training_dataset_engine.init_prepared_statement(self, external)

    def get_serving_vector(
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
        return self._training_dataset_engine.get_serving_vector(self, entry, external)

    @property
    def label(self):
        """The label/prediction feature of the training dataset.

        Can be a composite of multiple features.
        """
        return self._label

    @label.setter
    def label(self, label):
        self._label = [lb.lower() for lb in label]

    @property
    def feature_store_id(self):
        return self._feature_store_id

    @property
    def prepared_statement_engine(self):
        """JDBC connection engine to retrieve connections to online features store from."""
        return self._prepared_statement_engine

    @prepared_statement_engine.setter
    def prepared_statement_engine(self, prepared_statement_engine):
        self._prepared_statement_engine = prepared_statement_engine

    @property
    def prepared_statements(self):
        """The dict object of prepared_statements as values and kes as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements):
        self._prepared_statements = prepared_statements

    @property
    def serving_keys(self):
        """Set of primary key names that is used as keys in input dict object for `get_serving_vector` method."""
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys):
        self._serving_keys = serving_vector_keys

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
