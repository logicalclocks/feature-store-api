import humps


class StorageConnector:
    HOPSFS = "HOPSFS"
    S3 = "S3"
    JDBC = "JDBC"

    def __init__(
        self,
        type,
        id,
        name,
        description,
        featurestore_id,
        storage_connector_type,
        hopsfs_path,
        dataset_name,
    ):
        self._type = type
        self._id = id
        self._name = name
        self._description = description
        self._feature_store_id = featurestore_id
        self._storage_connector_type = storage_connector_type
        self._hopsfs_path = hopsfs_path
        self._dataset_name = dataset_name

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # TODO(Moritz): Later we can add a factory here to generate featuregroups depending on the type in the return json
        # i.e. offline, online, on-demand
        return cls(**json_decamelized)

    @property
    def id(self):
        return self._id
