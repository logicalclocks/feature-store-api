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

from hsfs.core import query, feature_group_engine, statistics_engine
from hsfs import util, engine, feature
from hsfs.statistics_config import StatisticsConfig


class FeatureGroup:
    CACHED_FEATURE_GROUP = "CACHED_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

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
        id=None,
        features=None,
        location=None,
        jobs=None,
        desc_stats_enabled=None,
        feat_corr_enabled=None,
        feat_hist_enabled=None,
        statistic_columns=None,
        online_enabled=False,
        hudi_enabled=False,
        default_storage="offline",
        statistics_config=None,
    ):
        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = creator
        self._version = version
        self._name = name
        self._id = id
        self._features = [feature.Feature.from_response_json(feat) for feat in features]
        self._location = location
        self._jobs = jobs
        self._online_enabled = online_enabled
        self._default_storage = default_storage
        self._hudi_enabled = hudi_enabled

        if id is not None:
            # initialized by backend
            self.statistics_config = StatisticsConfig(
                desc_stats_enabled,
                feat_corr_enabled,
                feat_hist_enabled,
                statistic_columns,
            )
            self._primary_key = [
                feat.name for feat in self._features if feat.primary is True
            ]
            self._partition_key = [
                feat.name for feat in self._features if feat.partition is True
            ]
        else:
            # initialized by user
            self.statistics_config = statistics_config
            self._primary_key = primary_key
            self._partition_key = partition_key

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(
            featurestore_id
        )

        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
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

    def save(self, features, write_options={}):
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        user_version = self._version
        self._feature_group_engine.save(
            self, feature_dataframe, self._default_storage, write_options,
        )
        if self.statistics_config.enabled:
            self._statistics_engine.compute_statistics(self, feature_dataframe)
        if user_version is None:
            warnings.warn(
                "No version provided for creating feature group `{}`, incremented version to `{}`.".format(
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

        self.compute_statistics()

    def delete(self):
        self._feature_group_engine.delete(self)

    def update_statistics_config(self):
        """Update the statistics configuration of the feature group.

        Change the `statistics_config` object and persist the changes by calling
        this method.

        :return: the updated metadata object of the feature group.
        :rtype: FeatureGroup
        """
        self._feature_group_engine.update_statistics_config(self)
        return self

    def compute_statistics(self):
        """Recompute the statistics for the feature group and save them to the
        feature store.

        :return: the statistics metadata object.
        :rtype: Statistics
        """
        if self.statistics_config.enabled:
            if self._default_storage.lower() in ["all", "offline"]:
                return self._statistics_engine.compute_statistics(
                    self, self.read("offline")
                )
            else:
                warnings.warn(
                    (
                        "The default storage of feature group `{}`, with version `{}`, is `{}`. "
                        "Statistics are only computed for default storage `offline` and `all`."
                    ).format(self._name, self._version, self._default_storage),
                    util.StorageWarning,
                )

    def add_tag(self, name, value=None):
        """Attach a name/value tag to a feature group.

        A tag can consist of a name only or a name/value pair. Tag names are
        unique identifiers.

        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added, defaults to None
        :type value: str, optional
        """
        self._feature_group_engine.add_tag(self, name, value)

    def delete_tag(self, name):
        """Delete a tag from a feature group.

        Tag names are unique identifiers.

        :param name: name of the tag to be removed
        :type name: str
        """
        self._feature_group_engine.delete_tag(self, name)

    def get_tag(self, name=None):
        """Get the tags of a feature group.

        Tag names are unique identifiers. Returns all tags if no tag name is
        specified.

        :param name: name of the tag to get, defaults to None
        :type name: str, optional
        :return: list of tags as name/value pairs
        :rtype: list of dict
        """
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
            "defaultStorage": self._default_storage.upper(),
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": "cachedFeaturegroupDTO",
            "descStatsEnabled": self._statistics_config.enabled,
            "featHistEnabled": self._statistics_config.histograms,
            "featCorrEnabled": self._statistics_config.correlations,
            "statisticColumns": self._statistics_config.columns,
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

    @version.setter
    def version(self, version):
        self._version = version

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

    @property
    def statistics_config(self):
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
        return self._statistics_engine.get_last(self)

    def get_statistics(self, commit_time=None):
        """Returns the statistics for this feature group at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        :param commit_time: Commit time in the format `YYYYMMDDhhmmss`, defaults to None
        :type commit_time: str, optional
        :return: Statistics information
        :rtype: Statistics
        """
        if commit_time is None:
            return self.statistics
        else:
            return self._statistics_engine.get(self, commit_time)
