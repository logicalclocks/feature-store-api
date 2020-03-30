import humps
import json

from hopsworks import util, engine
from hopsworks.storage_connector import StorageConnector
from hopsworks.core import query, training_dataset_api, storage_connector_api


class TrainingDataset:
    HOPSFS = "HOPSFS_TRAINING_DATASET"
    EXTERNAL = "EXTERNAL_TRAINING_DATASET"

    def __init__(
        self,
        name,
        version,
        description,
        data_format,
        location,
        featurestore_id,
        storage_connector=None,
        splits=None,
        type=None,
        cluster_analysis=None,
        created=None,
        creator=None,
        descriptive_statistics=None,
        feature_correlation_matrix=None,
        features=None,
        features_histogram=None,
        featurestore_name=None,
        id=None,
        jobs=None,
        inode_id=None,
        storage_connector_name=None,
        storage_connector_id=None,
        storage_connector_type=None,
        training_dataset_type=None,
    ):
        self._id = id
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._feature_query = None
        self._feature_dataframe = None
        self._splits = splits
        self._location = location

        self._storage_connector_name = storage_connector_name
        self._storage_connector_id = storage_connector_id
        self._storage_connector_type = storage_connector_type

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            featurestore_id
        )

        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            featurestore_id
        )

        # set up depending on user initialized or coming from backend response
        if training_dataset_type is None:
            # no type -> user init
            self.storage_connector = storage_connector
        else:
            # type available -> init from backend response
            # make rest call to get all connector information, description etc.
            self._storage_connector = self._storage_connector_api.get_by_id(
                storage_connector_id, storage_connector_type
            )
            self._training_dataset_type = training_dataset_type

    def create(self, features, write_options={}):
        # TODO: Decide if we want to have potentially dangerous defaults like {}
        if isinstance(features, query.Query):
            feature_dataframe = features.read()
        else:
            feature_dataframe = engine.get_instance().convert_to_default_dataframe(
                features
            )

        self._features = engine.get_instance().parse_schema(feature_dataframe)
        self._training_dataset_api.post(self)

        # post updates reinstantiates the instance so set these after the REST call
        self._feature_dataframe = feature_dataframe
        if isinstance(features, query.Query):
            self._feature_query = features

        write_options = engine.get_instance().write_options(
            self.data_format, write_options
        )

        # TODO: currently not supported petastorm, hdf5 and npy file formats
        engine.get_instance().write(
            feature_dataframe,
            self._storage_connector,
            self._data_format,
            "overwrite",
            write_options,
            self._location + "/" + self._name,
        )

        return self

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # here we lose the information that the user set
        # need to find solution for that
        self.__init__(**json_decamelized)
        return self

    def _infer_training_dataset_type(self, connector_type):
        if connector_type == StorageConnector.HOPSFS:
            return self.HOPSFS
        elif connector_type == StorageConnector.S3:
            return self.EXTERNAL
        elif connector_type is None:
            return self.HOPSFS
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
            "storageConnectorId": self._storage_connector.id,
            "location": self._location,
            "trainingDatasetType": self._training_dataset_type,
            "features": self._features,
        }

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    @property
    def data_format(self):
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._data_format = data_format

    @property
    def write_options(self):
        return self._write_options

    @write_options.setter
    def write_options(self, write_options):
        self._write_options = write_options

    @property
    def storage_connector(self):
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector):
        if isinstance(storage_connector, StorageConnector):
            self._storage_connector = storage_connector
        elif storage_connector is None:
            # init empty connector, otherwise will have to handle it at serialization time
            # TODO: make prettier
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
        return self._splits

    @splits.setter
    def splits(self, splits):
        self._splits = splits

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        self._location = location
