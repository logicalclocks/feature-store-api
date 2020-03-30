import humps


class StorageConnector:
    HOPSFS = "HOPSFS"
    S3 = "S3"
    JDBC = "JDBC"
    HOPSFS_DTO = "featurestoreHopsfsConnectorDTO"
    S3_DTO = ""

    def __init__(
        self,
        id,
        name,
        description,
        featurestore_id,
        storage_connector_type,
        # members specific to type of connector
        # TODO: or should these be subclassed and then init with factory?
        hopsfs_path=None,
        dataset_name=None,
        access_key=None,
        secret_key=None,
        bucket=None,
    ):
        self._id = id
        self._name = name
        self._description = description
        self._feature_store_id = featurestore_id
        self._storage_connector_type = storage_connector_type
        self._hopsfs_path = hopsfs_path
        self._dataset_name = dataset_name
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    @property
    def id(self):
        return self._id

    @property
    def connector_type(self):
        return self._storage_connector_type

    @property
    def access_key(self):
        return self._access_key

    @property
    def secret_key(self):
        return self._secret_key
