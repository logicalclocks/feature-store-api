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

import humps
import json
import warnings

from hsfs.core import query, feature_group_engine
from hsfs import util, engine, feature


class FeatureGroup:
    CACHED_FEATURE_GROUP = "CACHED_FEATURE_GROUP"
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
        location=None,
        jobs=None,
        desc_stats_enabled=None,
        feat_corr_enabled=None,
        feat_hist_enabled=None,
        cluster_analysis_enabled=None,
        statistic_columns=None,
        num_bins=None,
        num_clusters=None,
        corr_method=None,
        online_enabled=False,
        hudi_enabled=False,
        default_storage="offline",
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
        self._location = location
        self._jobs = jobs
        self._desc_stats_enabled = desc_stats_enabled
        self._feat_corr_enabled = feat_corr_enabled
        self._feat_hist_enabled = feat_hist_enabled
        self._cluster_analysis_enabled = cluster_analysis_enabled
        self._statistic_columns = statistic_columns
        self._num_bins = num_bins
        self._num_clusters = num_clusters
        self._corr_method = corr_method
        self._online_enabled = online_enabled
        self._default_storage = default_storage
        self._hudi_enabled = hudi_enabled

        self._primary_key = primary_key
        self._partition_key = partition_key

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(
            featurestore_id
        )

    def read(self, storage=None, dataframe_type="default"):
        """Get the feature group as a DataFrame."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().read(
            storage if storage else self._default_storage, dataframe_type
        )

    def show(self, n, storage=None):
        """Show the first n rows of the feature group."""
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            "Getting feature group: {} from the featurestore {}".format(
                self._name, self._feature_store_name
            ),
        )
        return self.select_all().show(n, storage if storage else self._default_storage)

    def select_all(self):
        """Select all features in the feature group and return a query object."""
        return query.Query(
            self._feature_store_name, self._feature_store_id, self, self._features
        )

    def select(self, features=[]):
        return query.Query(
            self._feature_store_name, self._feature_store_id, self, features
        )

    def save(self, features, storage=None, write_options={}):
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        user_version = self._version
        self._feature_group_engine.save(
            self,
            feature_dataframe,
            storage if storage else self._default_storage,
            write_options,
        )
        if user_version is None:
            warnings.warn(
                "No version provided for creating feature group `{}`, incremented version to `{}`".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )
        return self

    def insert(self, features, overwrite=False, storage=None, write_options={}):
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        self._feature_group_engine.insert(
            self,
            feature_dataframe,
            overwrite,
            storage if storage else self._default_storage,
            write_options,
        )

    def delete(self):
        self._feature_group_engine.delete(self)

    def add_tag(self, name, value=None):
        self._feature_group_engine.add_tag(self, name, value)

    def delete_tag(self, name):
        self._feature_group_engine.delete_tag(self, name)

    def get_tag(self, name=None):
        return self._feature_group_engine.get_tags(self, name)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "onlineEnabled": self._online_enabled,
            "defaultStorage": self._default_storage,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": "cachedFeaturegroupDTO",
        }

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def description(self):
        return self._description

    @property
    def features(self):
        return self._features

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def online_enabled(self):
        return self._online_enabled

    @property
    def partition_key(self):
        return self._partition_key

    @property
    def feature_store_name(self):
        return self._feature_store_name

    @property
    def creator(self):
        return self._creator

    @property
    def created(self):
        return self._created

    @description.setter
    def description(self, new_description):
        self._description = new_description

    @features.setter
    def features(self, new_features):
        self._features = new_features

    @primary_key.setter
    def primary_key(self, new_primary_key):
        self._primary_key = new_primary_key

    @partition_key.setter
    def partition_key(self, new_partition_key):
        self._partition_key = new_partition_key

    @online_enabled.setter
    def online_enabled(self, new_online_enabled):
        self._online_enabled = new_online_enabled
