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

from hsfs.core import query, feature_group_base_engine


class FeatureGroupBase:
    def __init__(self, featurestore_id):
        self._feature_group_base_engine = feature_group_base_engine.FeatureGroupBaseEngine(
            featurestore_id
        )

    def delete(self):
        self._feature_group_base_engine.delete(self)

    def select_all(self):
        """Select all features in the feature group and return a query object."""
        return query.Query(
            self._feature_store_name, self._feature_store_id, self, self._features
        )

    def select(self, features=[]):
        return query.Query(
            self._feature_store_name, self._feature_store_id, self, features
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
        self._feature_group_base_engine.add_tag(self, name, value)

    def delete_tag(self, name):
        """Delete a tag from a feature group.

        Tag names are unique identifiers.

        :param name: name of the tag to be removed
        :type name: str
        """
        self._feature_group_base_engine.delete_tag(self, name)

    def get_tag(self, name=None):
        """Get the tags of a feature group.

        Tag names are unique identifiers. Returns all tags if no tag name is
        specified.

        :param name: name of the tag to get, defaults to None
        :type name: str, optional
        :return: list of tags as name/value pairs
        :rtype: list of dict
        """
        return self._feature_group_base_engine.get_tags(self, name)
