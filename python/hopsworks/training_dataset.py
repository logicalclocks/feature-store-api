import humps
import json

from hopsworks import util, engine
from hopsworks.core import query, training_dataset_api


class TrainingDataset:
    def __init__(
        self,
        client,
        id,
        name,
        version,
        description,
        data_format,
        write_options,
        storage_connector,
        splits,
        location,
        features,
        feature_store_id,
    ):
        self._id = id
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._feature_query = None
        self._feature_dataframe = None
        self._write_options = write_options
        self._storage_connector = storage_connector
        self._storage_connector_id = storage_connector.id
        self._splits = splits
        self._location = location

        # TODO: fix
        self._training_dataset_type = "HOPSFS_TRAINING_DATASET"

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            client, feature_store_id
        )

    def create(self, features):
        if isinstance(features, query.Query):
            self._feature_dataframe = self._feature_query.read()
        else:
            self._feature_dataframe = engine.get_instance().convert_to_default_dataframe(
                features
            )

        self._features = engine.get_instance().parse_schema(self._feature_dataframe)

        return self

    @classmethod
    def from_json_response(cls, client, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(client, **json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "dataFormat": self._data_format,
            "storageConnectorId": self._storage_connector_id,
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
        self._storage_connector = storage_connector
        self._storage_connector_id = storage_connector.id

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
