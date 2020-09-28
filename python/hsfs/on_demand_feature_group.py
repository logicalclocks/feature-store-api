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

class OnDemandFeatureGroup:
    ON_DEMAND_FEATURE_GROUP = "ON_DEMAND_FEATURE_GROUP"

    def __init__(
        self,
        name,
        version,
        description,
        featurestore_id,
        partition_key=None,
        primary_key=None,
        featurestore_name=None,
        created=None,
        creator=None,
        descriptive_statistics=None,
        feature_correlation_matrix=None,
        features_histogram=None,
        cluster_analysis=None,
        id=None,
        features=None,
        jobs=None,
        desc_stats_enabled=None,
        feat_corr_enabled=None,
        feat_hist_enabled=None,
        cluster_analysis_enabled=None,
        statistic_columns=None,
        num_bins=None,
        num_clusters=None,
        corr_method=None,
    ):
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
        self._features = [feature.Feature.from_response_json(feat) for feat in features]
        self._jobs = jobs
        self._desc_stats_enabled = desc_stats_enabled
        self._feat_corr_enabled = feat_corr_enabled
        self._feat_hist_enabled = feat_hist_enabled
        self._cluster_analysis_enabled = cluster_analysis_enabled
        self._statistic_columns = statistic_columns
        self._num_bins = num_bins
        self._num_clusters = num_clusters
        self._corr_method = corr_method

        if id is None:
            # Initialized from the API
            self._primary_key = primary_key
            self._partition_key = partition_key
        else:
            # Initialized from the backend
            self._primary_key = [f.name for f in self._features if f.primary]
            self._partition_key = [f.name for f in self._features if f.partition]

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(
            featurestore_id
        )

