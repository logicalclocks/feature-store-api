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

from hsfs import util
from hsfs.core import query, feature_group_base_engine, statistics_engine
from hsfs.statistics_config import StatisticsConfig


class FeatureGroupBase:
    def __init__(self, featurestore_id):
        self._feature_group_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            featurestore_id
        )
        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )

    def delete(self):
        """Drop the entire feature group along with its feature data.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature group **and** all the feature data in offline and online storage
            associated with it.

        # Raises
            `RestAPIError`.
        """
        self._feature_group_engine.delete(self)

    def select_all(self):
        """Select all features in the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a
        training dataset immediately.

        # Returns
            `Query`. A query object with all features of the feature group.
        """
        return query.Query(
            self._feature_store_name, self._feature_store_id, self, self._features
        )

    def select(self, features=[]):
        """Select a subset of features of the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a training
        dataset with a subset of features of the feature group.

        # Arguments
            features: list, optional. A list of `Feature` objects or feature names as
                strings to be selected, defaults to [].

        # Returns
            `Query`: A query object with the selected features of the feature group.
        """
        return query.Query(
            self._feature_store_name, self._feature_store_id, self, features
        )

    def add_tag(self, name: str, value: str = None):
        """Attach a name/value tag to a feature group.

        A tag can consist of a name only or a name/value pair. Tag names are
        unique identifiers.

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added, defaults to `None`.

        # Raises
            `RestAPIError`.
        """
        self._feature_group_base_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag from a feature group.

        Tag names are unique identifiers.

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `RestAPIError`.
        """
        self._feature_group_base_engine.delete_tag(self, name)

    def get_tag(self, name: str = None):
        """Get the tags of a feature group.

        Tag names are unique identifiers. Returns all tags if no tag name is
        specified.

        # Arguments
            name: Name of the tag to get, defaults to `None`.

        # Returns
            `list[Tag]`. List of tags as name/value pairs.

        # Raises
            `RestAPIError`.
        """
        return self._feature_group_base_engine.get_tags(self, name)

    def update_statistics_config(self):
        """Update the statistics configuration of the feature group.

        Change the `statistics_config` object and persist the changes by calling
        this method.

        # Returns
            `FeatureGroup`. The updated metadata object of the feature group.

        # Raises
            `RestAPIError`.
        """
        self._feature_group_engine.update_statistics_config(self)
        return self

    def update_description(self, description: str):
        """Update the description of the feature gorup.

        # Arguments
            description: str. New description string.

        # Returns
            `FeatureGroup`. The updated feature group object.
        """
        self._feature_group_engine.update_description(self, description)
        return self

    def compute_statistics(self):
        """Recompute the statistics for the feature group and save them to the
        feature store.

        Statistics are only computed for data in the offline storage of the feature
        group.

        # Returns
            `Statistics`. The statistics metadata object.

        # Raises
            `RestAPIError`. Unable to persist the statistics.
        """
        if self.statistics_config.enabled:
            return self._statistics_engine.compute_statistics(self, self.read())
        else:
            warnings.warn(
                (
                    "The statistics are not enabled of feature group `{}`, with version"
                    " `{}`. No statistics computed."
                ).format(self._name, self._version),
                util.StorageWarning,
            )

    @property
    def statistics_config(self):
        """Statistics configuration object defining the settings for statistics
        computation of the feature group."""
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
        """Get the latest computed statistics for the feature group."""
        return self._statistics_engine.get_last(self)

    def get_statistics(self, commit_time: str = None):
        """Returns the statistics for this feature group at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        # Arguments
            commit_time: Commit time in the format `YYYYMMDDhhmmss`, defaults to `None`.

        # Returns
            `Statistics`. Statistics object.

        # Raises
            `RestAPIError`.
        """
        if commit_time is None:
            return self.statistics
        else:
            return self._statistics_engine.get(self, commit_time)
