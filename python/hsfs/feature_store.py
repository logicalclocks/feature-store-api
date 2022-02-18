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

import warnings
import humps
import numpy
import datetime
from typing import Optional, Union, List, Dict, TypeVar

from hsfs.transformation_function import TransformationFunction
from hsfs.core import transformation_function_engine

from hsfs import (
    training_dataset,
    feature_group,
    feature,
    util,
    storage_connector,
    expectation,
    rule,
)
from hsfs.core import (
    feature_group_api,
    storage_connector_api,
    training_dataset_api,
    expectations_api,
    feature_group_engine,
)
from hsfs.statistics_config import StatisticsConfig


class FeatureStore:
    DEFAULT_VERSION = 1

    def __init__(
        self,
        featurestore_id,
        featurestore_name,
        created,
        hdfs_store_path,
        project_name,
        project_id,
        featurestore_description,
        inode_id,
        offline_featurestore_name,
        hive_endpoint,
        online_enabled,
        num_feature_groups=None,
        num_training_datasets=None,
        num_storage_connectors=None,
        online_featurestore_name=None,
        mysql_server_endpoint=None,
        online_featurestore_size=None,
    ):
        self._id = featurestore_id
        self._name = featurestore_name
        self._created = created
        self._hdfs_store_path = hdfs_store_path
        self._project_name = project_name
        self._project_id = project_id
        self._description = featurestore_description
        self._inode_id = inode_id
        self._online_feature_store_name = online_featurestore_name
        self._online_feature_store_size = online_featurestore_size
        self._offline_feature_store_name = offline_featurestore_name
        self._hive_endpoint = hive_endpoint
        self._mysql_server_endpoint = mysql_server_endpoint
        self._online_enabled = online_enabled
        self._num_feature_groups = num_feature_groups
        self._num_training_datasets = num_training_datasets
        self._num_storage_connectors = num_storage_connectors

        self._feature_group_api = feature_group_api.FeatureGroupApi(self._id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            self._id
        )
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(self._id)
        self._expectations_api = expectations_api.ExpectationsApi(self._id)

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(self._id)

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(self._id)
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def get_feature_group(self, name: str, version: int = None):
        """Get a feature group entity from the feature store.

        Getting a feature group from the Feature Store means getting its metadata handle
        so you can subsequently read the data into a Spark or Pandas DataFrame or use
        the `Query`-API to perform joins between feature groups.

        # Arguments
            name: Name of the feature group to get.
            version: Version of the feature group to retrieve, defaults to `None` and will
                return the `version=1`.

        # Returns
            `FeatureGroup`: The feature group metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.

        """
        if version is None:
            warnings.warn(
                "No version provided for getting feature group `{}`, defaulting to `{}`.".format(
                    name, self.DEFAULT_VERSION
                ),
                util.VersionWarning,
            )
            version = self.DEFAULT_VERSION
        return self._feature_group_api.get(
            name, version, feature_group_api.FeatureGroupApi.CACHED
        )

    def get_feature_groups(self, name: str):
        """Get a list of all versions of a feature group entity from the feature store.

        Getting a feature group from the Feature Store means getting its metadata handle
        so you can subsequently read the data into a Spark or Pandas DataFrame or use
        the `Query`-API to perform joins between feature groups.

        # Arguments
            name: Name of the feature group to get.

        # Returns
            `FeatureGroup`: List of feature group metadata objects.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.

        """
        return self._feature_group_api.get(
            name, None, feature_group_api.FeatureGroupApi.CACHED
        )

    def get_on_demand_feature_group(self, name: str, version: int = None):
        """Get a on-demand feature group entity from the feature store.

        Getting a on-demand feature group from the Feature Store means getting its
        metadata handle so you can subsequently read the data into a Spark or
        Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        # Arguments
            name: Name of the on-demand feature group to get.
            version: Version of the on-demand feature group to retrieve,
                defaults to `None` and will return the `version=1`.

        # Returns
            `OnDemandFeatureGroup`: The on-demand feature group metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.

        """

        if version is None:
            warnings.warn(
                "No version provided for getting feature group `{}`, defaulting to `{}`.".format(
                    name, self.DEFAULT_VERSION
                ),
                util.VersionWarning,
            )
            version = self.DEFAULT_VERSION
        return self._feature_group_api.get(
            name, version, feature_group_api.FeatureGroupApi.ONDEMAND
        )

    def get_on_demand_feature_groups(self, name: str):
        """Get a list of all versions of an on-demand feature group entity from the feature store.

        Getting a on-demand feature group from the Feature Store means getting its
        metadata handle so you can subsequently read the data into a Spark or
        Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        # Arguments
            name: Name of the on-demand feature group to get.

        # Returns
            `OnDemandFeatureGroup`: List of on-demand feature group metadata objects.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.
        """
        return self._feature_group_api.get(
            name, None, feature_group_api.FeatureGroupApi.ONDEMAND
        )

    def get_training_dataset(self, name: str, version: int = None):
        """Get a training dataset entity from the feature store.

        Getting a training dataset from the Feature Store means getting its metadata handle
        so you can subsequently read the data into a Spark or Pandas DataFrame.

        # Arguments
            name: Name of the training dataset to get.
            version: Version of the training dataset to retrieve, defaults to `None` and will
                return the `version=1`.

        # Returns
            `TrainingDataset`: The training dataset metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.
        """

        if version is None:
            warnings.warn(
                "No version provided for getting training dataset `{}`, defaulting to `{}`.".format(
                    name, self.DEFAULT_VERSION
                ),
                util.VersionWarning,
            )
            version = self.DEFAULT_VERSION
        return self._training_dataset_api.get(name, version)

    def get_training_datasets(self, name: str):
        """Get a list of all versions of a training dataset entity from the feature store.

        Getting a training dataset from the Feature Store means getting its metadata handle
        so you can subsequently read the data into a Spark or Pandas DataFrame.

        # Arguments
            name: Name of the training dataset to get.

        # Returns
            `TrainingDataset`: List of training dataset metadata objects.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.
        """
        return self._training_dataset_api.get(name, None)

    def get_storage_connector(self, name: str):
        """Get a previously created storage connector from the feature store.

        Storage connectors encapsulate all information needed for the execution engine
        to read and write to specific storage. This storage can be S3, a JDBC compliant
        database or the distributed filesystem HOPSFS.

        If you want to connect to the online feature store, see the
        `get_online_storage_connector` method to get the JDBC connector for the Online
        Feature Store.

        !!! example "Getting a Storage Connector"
            ```python

            sc = fs.get_storage_connector("demo_fs_meb10000_Training_Datasets")

            td = fs.create_training_dataset(..., storage_connector=sc, ...)
            ```

        # Arguments
            name: Name of the storage connector to retrieve.

        # Returns
            `StorageConnector`. Storage connector object.
        """
        return self._storage_connector_api.get(name)

    def sql(
        self,
        query: str,
        dataframe_type: Optional[str] = "default",
        online: Optional[bool] = False,
        read_options: Optional[dict] = {},
    ):
        """Execute SQL command on the offline or online feature store database

        # Arguments
            query: The SQL query to execute.
            dataframe_type: The type of the returned dataframe. Defaults to "default".
                which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Hive engine.
            online: Set to true to execute the query against the online feature store.
                Defaults to False.
            read_options: Additional options to pass to the execution engine. Defaults to {}.
                If running queries on the online feature store, users can provide an entry `{'external': True}`,
                this instructs the library to use the `host` parameter in the [`hsfs.connection()`](project.md#connection) to establish the connection to the online feature store.
                If not set, or set to False, the online feature store storage connector is used which relies on
                the private ip.

        # Returns
            `DataFrame`: DataFrame depending on the chosen type.
        """
        return self._feature_group_engine.sql(
            query, self._name, dataframe_type, online, read_options
        )

    def get_online_storage_connector(self):
        """Get the storage connector for the Online Feature Store of the respective
        project's feature store.

        The returned storage connector depends on the project that you are connected to.

        # Returns
            `StorageConnector`. JDBC storage connector to the Online Feature Store.
        """
        return self._storage_connector_api.get_online_connector()

    def get_expectation(self, name: str):
        """Get an expectation entity from the feature store.

        Getting an expectation from the Feature Store means getting its metadata handle
        so you can subsequently add features and/or rules and save it which will overwrite the previous instance.

        # Arguments
            name: Name of the training dataset to get.

        # Returns
            `Expectation`: The expectation metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve the expectation from the feature store.
        """

        return self._expectations_api.get(name)

    def get_expectations(self):
        """Get all expectation entities from the feature store.

        Getting expectations from the Feature Store means getting their metadata handles
        so you can subsequently add features and/or rules and save it which will overwrite the previous instance.

        # Returns
            `Expectation`: The expectation metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve the expectations from the feature store.
        """

        return self._expectations_api.get()

    def create_feature_group(
        self,
        name: str,
        version: Optional[int] = None,
        description: Optional[str] = "",
        online_enabled: Optional[bool] = False,
        time_travel_format: Optional[str] = "HUDI",
        partition_key: Optional[List[str]] = [],
        primary_key: Optional[List[str]] = [],
        hudi_precombine_key: Optional[str] = None,
        features: Optional[List[feature.Feature]] = [],
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        validation_type: Optional[str] = "NONE",
        expectations: Optional[List[expectation.Expectation]] = [],
        event_time: Optional[str] = None,
    ):
        """Create a feature group metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or feature data in the
            feature store on its own. To persist the feature group and save feature data
            along the metadata in the feature store, call the `save()` method with a
            DataFrame.

        # Arguments
            name: Name of the feature group to create.
            version: Version of the feature group to retrieve, defaults to `None` and
                will create the feature group with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the feature group to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            online_enabled: Define whether the feature group should be made available
                also in the online feature store for low latency access, defaults to
                `False`.
            time_travel_format: Format used for time travel, defaults to `"HUDI"`.
            partition_key: A list of feature names to be used as partition key when
                writing the feature data to the offline storage, defaults to empty list
                `[]`.
            primary_key: A list of feature names to be used as primary key for the
                feature group. This primary key can be a composite key of multiple
                features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the feature group won't have any primary key.
            hudi_precombine_key: A feature name to be used as a precombine key for the `"HUDI"`
                feature group. Defaults to `None`. If feature group has time travel format
                `"HUDI"` and hudi precombine key was not specified then the first primary key of
                the feature group will be used as hudi precombine key.
            features: Optionally, define the schema of the feature group manually as a
                list of `Feature` objects. Defaults to empty list `[]` and will use the
                schema information of the DataFrame provided in the `save` method.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation, `"histograms"` to compute feature value frequencies and
                `"exact_uniqueness"` to compute uniqueness, distinctness and entropy.
                The values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            validation_type: Optionally, set the validation type to one of "NONE", "STRICT",
                "WARNING", "ALL". Determines the mode in which data validation is applied on
                 ingested or already existing feature group data.
            expectations: Optionally, a list of expectations to be attached to the feature group.
                The expectations list contains Expectation metadata objects which can be retrieved with
                the `get_expectation()` and `get_expectations()` functions.
            event_time: Optionally, provide the name of the feature containing the event
                time for the features in this feature group. If event_time is set
                the feature group can be used for point-in-time joins. Defaults to `None`.

        # Returns
            `FeatureGroup`. The feature group metadata object.
        """
        return feature_group.FeatureGroup(
            name=name,
            version=version,
            description=description,
            online_enabled=online_enabled,
            time_travel_format=time_travel_format,
            partition_key=partition_key,
            primary_key=primary_key,
            hudi_precombine_key=hudi_precombine_key,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features,
            statistics_config=statistics_config,
            validation_type=validation_type,
            expectations=expectations,
            event_time=event_time,
        )

    def create_on_demand_feature_group(
        self,
        name: str,
        storage_connector: storage_connector.StorageConnector,
        query: Optional[str] = None,
        data_format: Optional[str] = None,
        path: Optional[str] = "",
        options: Optional[Dict[str, str]] = {},
        version: Optional[int] = None,
        description: Optional[str] = "",
        primary_key: Optional[List[str]] = [],
        features: Optional[List[feature.Feature]] = [],
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        event_time: Optional[str] = None,
        validation_type: Optional[str] = "NONE",
        expectations: Optional[List[expectation.Expectation]] = [],
    ):
        """Create a on-demand feature group metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist any metadata in the
            feature store on its own. To persist the feature group metadata in the feature store,
            call the `save()` method.

        # Arguments
            name: Name of the on-demand feature group to create.
            query: A string containing a SQL query valid for the target data source.
                the query will be used to pull data from the data sources when the
                feature group is used.
            data_format: If the on-demand feature groups refers to a directory with data,
                the data format to use when reading it
            path: The location within the scope of the storage connector, from where to read
                the data for the on-demand feature group
            storage_connector: the storage connector to use to establish connectivity
                with the data source.
            version: Version of the on-demand feature group to retrieve, defaults to `None` and
                will create the feature group with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the on-demand feature group to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            primary_key: A list of feature names to be used as primary key for the
                feature group. This primary key can be a composite key of multiple
                features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the feature group won't have any primary key.
            features: Optionally, define the schema of the on-demand feature group manually as a
                list of `Feature` objects. Defaults to empty list `[]` and will use the
                schema information of the DataFrame resulting by executing the provided query
                against the data source.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this on-demand feature group, `"correlations`" to turn on feature correlation
                computation, `"histograms"` to compute feature value frequencies and
                `"exact_uniqueness"` to compute uniqueness, distinctness and entropy.
                The values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            event_time: Optionally, provide the name of the feature containing the event
                time for the features in this feature group. If event_time is set
                the feature group can be used for point-in-time joins. Defaults to `None`.
            validation_type: Optionally, set the validation type to one of "NONE", "STRICT",
                "WARNING", "ALL". Determines the mode in which data validation is applied on
                 ingested or already existing feature group data.
            expectations: Optionally, a list of expectations to be attached to the feature group.
                The expectations list contains Expectation metadata objects which can be retrieved with
                the `get_expectation()` and `get_expectations()` functions.

        # Returns
            `OnDemandFeatureGroup`. The on-demand feature group metadata object.
        """
        return feature_group.OnDemandFeatureGroup(
            name=name,
            query=query,
            data_format=data_format,
            path=path,
            options=options,
            storage_connector=storage_connector,
            version=version,
            description=description,
            primary_key=primary_key,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features,
            statistics_config=statistics_config,
            event_time=event_time,
            validation_type=validation_type,
            expectations=expectations,
        )

    def create_training_dataset(
        self,
        name: str,
        version: Optional[int] = None,
        description: Optional[str] = "",
        data_format: Optional[str] = "tfrecords",
        coalesce: Optional[bool] = False,
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        splits: Optional[Dict[str, float]] = {},
        location: Optional[str] = "",
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        label: Optional[List[str]] = [],
        transformation_functions: Optional[Dict[str, TransformationFunction]] = {},
        train_split: str = None,
    ):
        """Create a training dataset metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or feature data in the
            feature store on its own. To materialize the training dataset and save
            feature data along the metadata in the feature store, call the `save()`
            method with a `DataFrame` or `Query`.

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
            name: Name of the training dataset to create.
            version: Version of the training dataset to retrieve, defaults to `None` and
                will create the training dataset with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            data_format: The data format used to save the training dataset,
                defaults to `"tfrecords"`-format.
            coalesce: If true the training dataset data will be coalesced into
                a single partition before writing. The resulting training dataset
                will be a single file per split. Default False.
            storage_connector: Storage connector defining the sink location for the
                training dataset, defaults to `None`, and materializes training dataset
                on HopsFS.
            splits: A dictionary defining training dataset splits to be created. Keys in
                the dictionary define the name of the split as `str`, values represent
                percentage of samples in the split as `float`. Currently, only random
                splits are supported. Defaults to empty dict`{}`, creating only a single
                training dataset without splits.
            location: Path to complement the sink storage connector with, e.g if the
                storage connector points to an S3 bucket, this path can be used to
                define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the
                storage connector.
            seed: Optionally, define a seed to create the random splits with, in order
                to guarantee reproducability, defaults to `None`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            label: A list of feature names constituting the prediction label/feature of
                the training dataset. When replaying a `Query` during model inference,
                the label features can be omitted from the feature vector retrieval.
                Defaults to `[]`, no label.
            transformation_functions: A dictionary mapping tansformation functions to
                to the features they should be applied to before writing out the
                training data and at inference time. Defaults to `{}`, no
                transformations.
            train_split: If `splits` is set, provide the name of the split that is going
                to be used for training. The statistics of this split will be used for
                transformation functions if necessary. Defaults to `None`.

        # Returns:
            `TrainingDataset`: The training dataset metadata object.
        """
        return training_dataset.TrainingDataset(
            name=name,
            version=version,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._id,
            splits=splits,
            seed=seed,
            statistics_config=statistics_config,
            label=label,
            coalesce=coalesce,
            transformation_functions=transformation_functions,
            train_split=train_split,
        )

    def create_expectation(
        self,
        name: str,
        description: Optional[str] = "",
        features: Optional[List[str]] = [],
        rules: Optional[List[rule.Rule]] = [],
    ):
        """Create an expectation metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist the expectation in the
            feature store on its own. To materialize the expectation and save
            call the `save()` method of the expectation metadata object.

        # Arguments
            name: Name of the training dataset to create.
            description: A string describing the expectation that can describe its business logic and applications
                within the feature store.
            features: The features this expectation is applied on.
            rules: The validation rules this expectation will apply to the its features.

        # Returns:
            `Expectation`: The expectation metadata object.
        """
        return expectation.Expectation(
            name=name,
            description=description,
            features=features,
            rules=rules,
            featurestore_id=self._id,
        )

    def delete_expectation(
        self,
        name: str,
    ):
        """Delete an expectation from the feature store.

        # Arguments
            name: Name of the training dataset to create.
        """
        return self._expectations_api.delete(name)

    def create_transformation_function(
        self,
        transformation_function: callable,
        output_type: Union[
            str,
            TypeVar("str"),  # noqa: F821
            TypeVar("string"),  # noqa: F821
            bytes,
            numpy.int8,
            TypeVar("int8"),  # noqa: F821
            TypeVar("byte"),  # noqa: F821
            numpy.int16,
            TypeVar("int16"),  # noqa: F821
            TypeVar("short"),  # noqa: F821
            int,
            TypeVar("int"),  # noqa: F821
            numpy.int,
            numpy.int32,
            numpy.int64,
            TypeVar("int64"),  # noqa: F821
            TypeVar("long"),  # noqa: F821
            TypeVar("bigint"),  # noqa: F821
            float,
            TypeVar("float"),  # noqa: F821
            numpy.float,
            numpy.float64,
            TypeVar("float64"),  # noqa: F821
            TypeVar("double"),  # noqa: F821
            datetime.datetime,
            numpy.datetime64,
            datetime.date,
            bool,
            TypeVar("boolean"),  # noqa: F821
            TypeVar("bool"),  # noqa: F821
            numpy.bool,
        ],
        version: Optional[int] = None,
    ):
        """Create a transformation function metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist the transformation function in the
            feature store on its own. To materialize the transformation function and save
            call the `save()` method of the transformation function metadata object.

        # Arguments
            transformation_function: callable object.
            output_type: python or numpy output type that will be inferred as pyspark.sql.types type.

        # Returns:
            `TransformationFunction`: The TransformationFunction metadata object.
        """
        return TransformationFunction(
            featurestore_id=self._id,
            transformation_fn=transformation_function,
            output_type=output_type,
            version=version,
        )

    def get_transformation_function(
        self,
        name: str,
        version: Optional[int] = None,
    ):
        """Get  transformation function metadata object.

        # Arguments
            name: name of transformation function.
            version: version of transformation function. Optional, if not provided all functions that match to provided
                name will be retrieved .
        # Returns:
            `TransformationFunction`: The TransformationFunction metadata object.
        """
        return self._transformation_function_engine.get_transformation_fn(name, version)

    def get_transformation_functions(self):
        """Get  all transformation functions metadata objects.

        # Returns:
             `List[TransformationFunction]`. List of transformation function instances.
        """
        return self._transformation_function_engine.get_transformation_fns()

    def register_builtin_transformation_functions(self):
        """
        ***Deprecated***

        Register hsfs built-in transformation functions.
        """
        self._transformation_function_engine.register_builtin_transformation_fns()

    @property
    def id(self):
        """Id of the feature store."""
        return self._id

    @property
    def name(self):
        """Name of the feature store."""
        return self._name

    @property
    def project_name(self):
        """Name of the project in which the feature store is located."""
        return self._project_name

    @property
    def project_id(self):
        """Id of the project in which the feature store is located."""
        return self._project_id

    @property
    def description(self):
        """Description of the feature store."""
        return self._description

    @property
    def online_featurestore_name(self):
        """Name of the online feature store database."""
        return self._online_feature_store_name

    @property
    def mysql_server_endpoint(self):
        """MySQL server endpoint for the online feature store."""
        return self._mysql_server_endpoint

    @property
    def online_enabled(self):
        """Indicator whether online feature store is enabled."""
        return self._online_enabled

    @property
    def hive_endpoint(self):
        """Hive endpoint for the offline feature store."""
        return self._hive_endpoint

    @property
    def offline_featurestore_name(self):
        """Name of the offline feature store database."""
        return self._offline_feature_store_name
