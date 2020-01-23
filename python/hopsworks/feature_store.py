import humps


class FeatureStore:
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
        online_featurestore_type,
        online_featurestore_name,
        online_featurestore_size,
        offline_featurestore_type,
        offline_featurestore_name,
        hive_endpoint,
        mysql_server_endpoint,
        online_enabled,
    ):
        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name
        self._created = created
        self._hdfs_store_path = hdfs_store_path
        self._project_name = project_name
        self._project_id = project_id
        self._feature_store_description = featurestore_description
        self._inode_id = inode_id
        self._online_feature_store_type = online_featurestore_type
        self._online_feature_store_name = online_featurestore_name
        self._online_feature_store_size = online_featurestore_size
        self._offline_feature_store_type = offline_featurestore_type
        self._offline_feature_store_name = offline_featurestore_name
        self._hive_endpoint = hive_endpoint
        self._mysql_server_endpoint = mysql_server_endpoint
        self._online_enabled = online_enabled

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)
