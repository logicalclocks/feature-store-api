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
from __future__ import annotations

import warnings
from typing import List, Optional, Union

from hsfs import client, feature_group_commit, util
from hsfs import feature_group as fg_mod
from hsfs.core import explicit_provenance, ingestion_job, ingestion_job_conf


class FeatureGroupApi:
    BACKEND_FG_STREAM = "streamFeatureGroupDTO"
    BACKEND_FG_BATCH = "cachedFeaturegroupDTO"
    BACKEND_FG_EXTERNAL = "onDemandFeaturegroupDTO"
    BACKEND_FG_SPINE = "onDemandFeaturegroupDTO"

    def save(
        self,
        feature_group_instance: Union[
            fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup
        ],
    ) -> Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup]:
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
            feature_group_instance.feature_store_id,
            "featuregroups",
        ]
        query_params = {
            "expand": ["features", "expectationsuite", "transformationfunctions"]
        }
        headers = {"content-type": "application/json"}
        feature_group_object = feature_group_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=feature_group_instance.json(),
                query_params=query_params,
            ),
        )
        return feature_group_object

    def get(
        self, feature_store_id: int, name: str, version: Optional[int]
    ) -> Union[
        fg_mod.FeatureGroup,
        fg_mod.SpineGroup,
        fg_mod.ExternalFeatureGroup,
        List[fg_mod.FeatureGroup],
        List[fg_mod.SpineGroup],
        List[fg_mod.ExternalFeatureGroup],
    ]:
        """Get the metadata of a feature group with a certain name and version.

        :param feature_store_id: feature store id
        :type feature_store_id: int
        :param name: name of the feature group
        :type name: str
        :param version: version of the feature group
        :type version: int

        :return: feature group metadata object
        :rtype: FeatureGroup, SpineGroup, ExternalFeatureGroup, List[FeatureGroup], List[SpineGroup], List[ExternalFeatureGroup]
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featuregroups",
            name,
        ]
        query_params = {
            "expand": ["features", "expectationsuite", "transformationfunctions"]
        }
        if version is not None:
            query_params["version"] = version

        fg_objs = []
        # In principle unique names are enforced across fg type and this should therefore
        # return only list of the same type. But it cost nothing to check in case this gets forgotten.
        for fg_json in _client._send_request("GET", path_params, query_params):
            if (
                fg_json["type"] == FeatureGroupApi.BACKEND_FG_STREAM
                or fg_json["type"] == FeatureGroupApi.BACKEND_FG_BATCH
            ):
                fg_objs.append(fg_mod.FeatureGroup.from_response_json(fg_json))
            elif fg_json["type"] == FeatureGroupApi.BACKEND_FG_EXTERNAL:
                if fg_json.get("spine", False):
                    fg_objs.append(fg_mod.SpineGroup.from_response_json(fg_json))
                else:
                    fg_objs.append(
                        fg_mod.ExternalFeatureGroup.from_response_json(fg_json)
                    )
            else:
                list_of_types = [
                    FeatureGroupApi.BACKEND_FG_STREAM,
                    FeatureGroupApi.BACKEND_FG_BATCH,
                    FeatureGroupApi.BACKEND_FG_EXTERNAL,
                    FeatureGroupApi.BACKEND_FG_SPINE,
                ]
                raise ValueError(
                    f"Unknown feature group type: {fg_json['type']}, expected one of: "
                    + str(list_of_types)
                )

        if version is not None:
            self._check_features(fg_objs[0])
            return fg_objs[0]
        else:
            for fg_obj in fg_objs:
                self._check_features(fg_obj)
            return fg_objs

    def get_by_id(
        self, feature_store_id: int, feature_group_id: int
    ) -> Union[
        fg_mod.FeatureGroup,
        fg_mod.SpineGroup,
        fg_mod.ExternalFeatureGroup,
    ]:
        """Get the metadata of a feature group with a certain id.

        :param feature_store_id: feature store id
        :type feature_store_id: int
        :param feature_group_id: id of the feature group
        :type feature_group_id: int

        :return: feature group metadata object
        :rtype: FeatureGroup, SpineGroup, ExternalFeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featuregroups",
            feature_group_id,
        ]
        query_params = {
            "expand": ["features", "expectationsuite", "transformationfunctions"]
        }
        fg_json = _client._send_request("GET", path_params, query_params)
        if (
            fg_json["type"] == FeatureGroupApi.BACKEND_FG_STREAM
            or fg_json["type"] == FeatureGroupApi.BACKEND_FG_BATCH
        ):
            return fg_mod.FeatureGroup.from_response_json(fg_json)
        elif fg_json["type"] == FeatureGroupApi.BACKEND_FG_EXTERNAL:
            if fg_json.get("spine", False):
                return fg_mod.SpineGroup.from_response_json(fg_json)
            else:
                return fg_mod.ExternalFeatureGroup.from_response_json(fg_json)

    def delete_content(
        self,
        feature_group_instance: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup],
    ) -> None:
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
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "clear",
        ]
        _client._send_request("POST", path_params)

    def delete(
        self,
        feature_group_instance: Union[
            fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup
        ],
    ) -> None:
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
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
        ]
        _client._send_request("DELETE", path_params)

    def update_metadata(
        self,
        feature_group_instance: Union[
            fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup
        ],
        feature_group_copy: Union[
            fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup
        ],
        query_parameter: str,
        query_parameter_value=True,
    ) -> Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup, fg_mod.SpineGroup]:
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
            query_parameter: str. Query parameter that controls which information is updated. E.g. "updateMetadata".
            query_parameter_value: Str. Value of the query_parameter.

        # Returns
            FeatureGroup. The updated feature group metadata object.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
        ]
        headers = {"content-type": "application/json"}
        query_params = {query_parameter: query_parameter_value}
        feature_group_object = feature_group_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                query_params,
                headers=headers,
                data=feature_group_copy.json(),
            ),
        )
        return feature_group_object

    def commit(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        feature_group_commit_instance: feature_group_commit.FeatureGroupCommit,
    ) -> feature_group_commit.FeatureGroupCommit:
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
            feature_group_instance.feature_store_id,
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

    def get_commit_details(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        wallclock_timestamp: int,
        limit: int,
    ) -> feature_group_commit.FeatureGroupCommit:
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
            feature_group_instance.feature_store_id,
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

    def ingestion(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        ingestion_conf: ingestion_job_conf.IngestionJobConf,
    ) -> ingestion_job.IngestionJob:
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
            feature_group_instance.feature_store_id,
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

    def get_parent_feature_groups(
        self,
        feature_group_instance: Union[
            fg_mod.FeatureGroup, fg_mod.SpineGroup, fg_mod.ExternalFeatureGroup
        ],
    ) -> explicit_provenance.Links:
        """Get the parents of this feature group, based on explicit provenance.
        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only a minimal information is
        returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ExplicitProvenance.Links`:  the feature groups used to generate this
            feature group
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 1,
            "downstreamLvls": 0,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.UPSTREAM,
            explicit_provenance.Links.Type.FEATURE_GROUP,
        )

    def get_storage_connector_provenance(self, feature_group_instance):
        """Get the parents of this feature group, based on explicit provenance.
        Parents are storage connectors. These storage connector can be accessible,
        deleted or inaccessible.
        For deleted and inaccessible storage connector, only a minimal information is
        returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ExplicitProvenance.Links`: the storage connector used to generated this
            feature group
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 1,
            "downstreamLvls": 0,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.UPSTREAM,
            explicit_provenance.Links.Type.STORAGE_CONNECTOR,
        )

    def get_generated_feature_views(
        self,
        feature_group_instance: Union[
            fg_mod.FeatureGroup, fg_mod.SpineGroup, fg_mod.ExternalFeatureGroup
        ],
    ) -> explicit_provenance.Links:
        """Get the generated feature view using this feature group, based on explicit
        provenance. These feature views can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature view links, so deleted
        will always be empty.
        For inaccessible feature views, only a minimal information is returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ExplicitProvenance.Links`: the feature views generated using this feature
            group
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 0,
            "downstreamLvls": 1,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.DOWNSTREAM,
            explicit_provenance.Links.Type.FEATURE_VIEW,
        )

    def get_generated_feature_groups(
        self,
        feature_group_instance: Union[
            fg_mod.FeatureGroup, fg_mod.SpineGroup, fg_mod.ExternalFeatureGroup
        ],
    ) -> explicit_provenance.Links:
        """Get the generated feature groups using this feature group, based on explicit
        provenance. These feature groups can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature group links, so deleted
        will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        # Arguments
            feature_group_instance: Metadata object of feature group.

        # Returns
            `ExplicitProvenance.Links`: the feature groups generated using this
            feature group
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 0,
            "downstreamLvls": 1,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.DOWNSTREAM,
            explicit_provenance.Links.Type.FEATURE_GROUP,
        )

    def _check_features(self, feature_group_instance) -> None:
        if not feature_group_instance._features:
            warnings.warn(
                f"Feature Group `{feature_group_instance._name}`, version `{feature_group_instance._version}` has no features (to resolve this issue contact the admin or delete and recreate the feature group)",
                util.FeatureGroupWarning,
                stacklevel=1,
            )
