import humps

from hopsworks.core import query, query_constructor_api
from hopsworks import engine


class FeatureGroup:
    def __init__(
        self,
        client,
        dataframe_type,
        type,
        featurestore_id,
        featurestore_name,
        description,
        created,
        creator,
        version,
        descriptive_statistics,
        feature_correlation_matrix,
        features_histogram,
        cluster_analysis,
        name,
        id,
        features,
        location,
        jobs,
        featuregroup_type,
        desc_stats_enabled,
        feat_corr_enabled,
        feat_hist_enabled,
        cluster_analysis_enabled,
        statistic_columns,
        num_bins,
        num_clusters,
        corr_method,
        hdfs_store_paths,
        hive_table_id,
        hive_table_type,
        inode_id,
        input_format,
        online_featuregroup_enabled,
    ):
        self._type = type
        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = creator
        self._version = version
        self._descriptive_statistics = descriptive_statistics
        self._feature_correlation_matrix = feature_correlation_matrix
        self._features_histogram = features_histogram
        self._cluster_analysis = cluster_analysis
        self._name = name
        self._id = id
        self._features = features
        self._location = location
        self._jobs = jobs
        self._feature_group_typ = featuregroup_type
        self._desc_stats_enabled = desc_stats_enabled
        self._feat_corr_enabled = feat_corr_enabled
        self._feat_hist_enabled = feat_hist_enabled
        self._cluster_analysis_enabled = cluster_analysis_enabled
        self._statistic_columns = statistic_columns
        self._num_bins = num_bins
        self._num_clusters = num_clusters
        self._corr_method = corr_method
        self._hdfs_store_paths = hdfs_store_paths
        self._hive_table_id = hive_table_id
        self._hive_table_type = hive_table_type
        self._inode_id = inode_id
        self._input_format = input_format
        self._online_feature_group_enabled = online_featuregroup_enabled

        self._query_constructor_api = query_constructor_api.QueryConstructorApi(client)
        self._dataframe_type = dataframe_type

    def read(self):
        """Get the feature group as a Spark DataFrame."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().read()

    def show(self, n):
        """Show the first n rows of the feature group."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().show(n)

    def select_all(self):
        """Select all features in the feature group and return a query object."""
        return query.Query(
            self._dataframe_type, self._query_constructor_api, self, self._features
        )

    def select(self, features=[]):
        return query.Query(
            self._dataframe_type, self._query_constructor_api, self, features
        )

    @classmethod
    def from_response_json(cls, client, dataframe_type, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # TODO(Moritz): Later we can add a factory here to generate featuregroups depending on the type in the return json
        # i.e. offline, online, on-demand
        return cls(client, dataframe_type, **json_decamelized)

    @classmethod
    def new_featuregroup(cls):
        pass
