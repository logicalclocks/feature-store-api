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

from hsfs import client
from hsfs import feature_group, feature_group_commit
from hsfs.core import ingestion_job


class FeatureGroupApi:
    CACHED = "cached"
    ONDEMAND = "ondemand"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def save(self, feature_group_instance):
        """Save feature group metadata to the feature store.

        :param feature_group_instance: metadata object of feature group to be
            saved
        :type feature_group_instance: FeatureGroup
        :return: updated metadata object of the feature group
        :rtype: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
        ]
        headers = {"content-type": "application/json"}
        return feature_group_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=feature_group_instance.json(),
            ),
        )

    def get(self, name, version, fg_type):
        """Get the metadata of a feature group with a certain name and version.

        :param name: name of the feature group
        :type name: str
        :param version: version of the feature group
        :type version: int
        :param fg_type: type of the feature group to return
        :type version: string
        :return: feature group metadata object
        :rtype: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            name,
        ]
        query_params = {"version": version}
        fg_json = _client._send_request("GET", path_params, query_params)[0]

        if fg_type == self.CACHED:
            return feature_group.FeatureGroup.from_response_json(fg_json)
        else:
            return feature_group.OnDemandFeatureGroup.from_response_json(fg_json)

    def delete_content(self, feature_group_instance):
        """Delete the content of a feature group.

        This endpoint serves to simulate the overwrite/insert mode.

        :param feature_group_instance: metadata object of feature group to clear
            the content for
        :type feature_group_instance: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "clear",
        ]
        _client._send_request("POST", path_params)

    def delete(self, feature_group_instance):
        """Drop a feature group from the feature store.

        Drops the metadata and data of a version of a feature group.

        :param feature_group_instance: metadata object of feature group
        :type feature_group_instance: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
        ]
        _client._send_request("DELETE", path_params)

    def update_metadata(
        self,
        feature_group_instance,
        feature_group_copy,
        query_parameter,
        query_parameter_value=True,
    ):
        """Update the metadata of a feature group.

        This only updates description and schema/features. The
        `feature_group_copy` is the metadata object sent to the backend, while
        `feature_group_instance` is the user object, which is only updated
        after a successful REST call.

        # Arguments
            feature_group_instance: FeatureGroup. User metadata object of the
                feature group.
            feature_group_copy: FeatureGroup. Metadata object of the feature
                group with the information to be updated.
            query_parameter: str. Query parameter that controls which information is updated. E.g. "updateMetadata",
                or "validationType".
            query_parameter_value: Str. Value of the query_parameter.

        # Returns
            FeatureGroup. The updated feature group metadata object.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
        ]
        headers = {"content-type": "application/json"}
        query_params = {query_parameter: query_parameter_value}
        return feature_group_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                query_params,
                headers=headers,
                data=feature_group_copy.json(),
            ),
        )

    def commit(self, feature_group_instance, feature_group_commit_instance):
        """
        Save feature group commit metadata.
        # Arguments
        feature_group_instance: FeatureGroup, required
            metadata object of feature group.
        feature_group_commit_instance: FeatureGroupCommit, required
            metadata object of feature group commit.
        # Returns
            `FeatureGroupCommit`.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "commits",
        ]
        headers = {"content-type": "application/json"}
        return feature_group_commit_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=feature_group_commit_instance.json(),
            ),
        )

    def get_commit_details(self, feature_group_instance, wallclock_timestamp, limit):
        """
        Get feature group commit metadata.
        # Arguments
        feature_group_instance: FeatureGroup, required
            metadata object of feature group.
        limit: number of commits to retrieve
        wallclock_timestamp: specific point in time.
        # Returns
            `FeatureGroupCommit`.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "commits",
        ]
        headers = {"content-type": "application/json"}
        query_params = {"sort_by": "committed_on:desc", "offset": 0, "limit": limit}
        if wallclock_timestamp is not None:
            query_params["filter_by"] = "commited_on_ltoeq:" + str(wallclock_timestamp)

        return feature_group_commit.FeatureGroupCommit.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers),
        )

    def ingestion(self, feature_group_instance, ingestion_conf):
        """
        Setup a Hopsworks job for dataframe ingestion
        Args:
        feature_group_instance: FeatureGroup, required
            metadata object of feature group.
        ingestion_conf: the configuration for the ingestion job application
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "ingestion",
        ]

        headers = {"content-type": "application/json"}
        return ingestion_job.IngestionJob.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=ingestion_conf.json()
            ),
        )
