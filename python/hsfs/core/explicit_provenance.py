#
#   Copyright 2022 Logical Clocks AB
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

import json
from enum import Enum
from typing import Set
from hsfs import feature_group, feature_view, training_dataset
import humps


class Artifact:
    class MetaType(Enum):
        DELETED = 1
        INACCESSIBLE = 2
        FAULTY = 3

    def __init__(
        self,
        feature_store_name,
        name,
        version,
        type,
        meta_type,
        href=None,
        exception_cause=None,
    ):
        self._feature_store_name = feature_store_name
        self._name = name
        self._version = version
        self._type = type
        self._meta_type = meta_type
        self._href = href
        self._exception_cause = exception_cause

    @property
    def feature_store_name(self):
        """Name of the feature store in which the artifact is located."""
        return self._feature_store_name

    @property
    def name(self):
        """Name of the artifact."""
        return self._name

    @property
    def version(self):
        """Version of the artifact"""
        return self._version

    def __str__(self):
        return {
            "feature_store_name": self._feature_store_name,
            "name": self._name,
            "version": self._version,
        }

    def __repr__(self):
        return (
            f"Artifact({self._feature_store_name!r}, {self._name!r}, "
            f"{self._version!r}, {self._type!r}, {self._meta_type!r}, "
            f"{self._href!r}, {self._exception_cause!r})"
        )

    @staticmethod
    def from_response_json(json_dict: dict):
        link_json = humps.decamelize(json_dict)
        if link_json.get("exception_cause") is not None:
            return Artifact(
                link_json["artifact"]["project"],
                link_json["artifact"]["name"],
                link_json["artifact"]["version"],
                link_json["artifact_type"],
                Artifact.MetaType.FAULTY,
            )
        elif bool(link_json["deleted"]):
            return Artifact(
                link_json["artifact"]["project"],
                link_json["artifact"]["name"],
                link_json["artifact"]["version"],
                link_json["artifact_type"],
                Artifact.MetaType.DELETED,
            )
        elif not bool(link_json["accessible"]):
            return Artifact(
                link_json["artifact"]["project"],
                link_json["artifact"]["name"],
                link_json["artifact"]["version"],
                link_json["artifact_type"],
                Artifact.MetaType.INACCESSIBLE,
                link_json["artifact"]["href"],
            )


class Links:
    def __init__(self):
        self._accessible = []
        self._deleted = []
        self._inaccessible = []
        self._faulty = []

    @property
    def deleted(self):
        """List of [Artifact objects](../links#artifact) which contains
        minimal information (name, version) about the entities
        (feature groups, feature views) they represent.
        These entities have been removed from the feature store.
        """
        return self._deleted

    @property
    def inaccessible(self):
        """List of [Artifact objects](../links#artifact) which contains
        minimal information (name, version) about the entities
        (feature groups, feature views) they represent.
        These entities exist in the feature store, however the user
        does not have access to them anymore.
        """
        return self._inaccessible

    @property
    def accessible(self):
        """List of [feature groups](../feature_group_api) or
        [feature views](../feature_view_api) metadata objects
        which are part of the provenance graph requested. These entities
        exist in the feature store and the user has access to them.
        """
        return self._accessible

    @property
    def faulty(self):
        """List of [Artifact objects](../links#artifact) which contains
        minimal information (name, version) about the entities
        (feature groups, feature views) they represent.
        These entities exist in the feature store, however they are corrupted.
        """
        return self._faulty

    class Direction(Enum):
        UPSTREAM = 1
        DOWNSTREAM = 2

    class Type(Enum):
        FEATURE_GROUP = 1
        FEATURE_VIEW = 2

    def __str__(self, indent=None):
        return json.dumps(self, cls=ProvenanceEncoder, indent=indent)

    def __repr__(self):
        return (
            f"Links({self._accessible!r}, {self._deleted!r}"
            f", {self._inaccessible!r}, {self._faulty!r})"
        )

    @staticmethod
    def __feature_group(link_json: dict):
        if link_json["artifact_type"] == "FEATURE_GROUP":
            return feature_group.FeatureGroup.from_response_json(link_json["artifact"])
        elif link_json["artifact_type"] == "EXTERNAL_FEATURE_GROUP":
            return feature_group.ExternalFeatureGroup.from_response_json(
                link_json["artifact"]
            )

    @staticmethod
    def __parse_feature_groups(links_json: dict, artifacts: Set[str]):
        links = Links()
        for link_json in links_json:
            if link_json["node"]["artifact_type"] in artifacts:
                if link_json["node"].get("exception_cause") is not None:
                    links._faulty.append(Artifact.from_response_json(link_json["node"]))
                elif bool(link_json["node"]["accessible"]):
                    links.accessible.append(Links.__feature_group(link_json["node"]))
                elif bool(link_json["node"]["deleted"]):
                    links.deleted.append(Artifact.from_response_json(link_json["node"]))
                else:
                    links.inaccessible.append(
                        Artifact.from_response_json(link_json["node"])
                    )
        return links

    @staticmethod
    def __parse_feature_views(links_json: dict, artifacts: Set[str]):
        links = Links()
        for link_json in links_json:
            if link_json["node"]["artifact_type"] in artifacts:
                if link_json["node"].get("exception_cause") is not None:
                    links._faulty.append(Artifact.from_response_json(link_json["node"]))
                elif bool(link_json["node"]["accessible"]):
                    links.accessible.append(
                        feature_view.FeatureView.from_response_json(
                            link_json["node"]["artifact"]
                        )
                    )
                elif bool(link_json["node"]["deleted"]):
                    links.deleted.append(Artifact.from_response_json(link_json["node"]))
                else:
                    links.inaccessible.append(
                        Artifact.from_response_json(link_json["node"])
                    )
        return links

    @staticmethod
    def from_response_json(json_dict: dict, direction: Direction, artifact: Type):
        """Parse explicit links from json response. There are three types of
        Links: UpstreamFeatureGroups, DownstreamFeatureGroups, DownstreamFeatureViews

        # Arguments
            links_json: json response from the explicit provenance endpoint
            direction: subset of links to parse - UPSTREAM/DOWNSTREAM
            type: subset of links to parse - FEATURE_GROUP/FEATURE_VIEW

        # Returns
            A ProvenanceLink object for the selected parse type.
        """

        links_json = humps.decamelize(json_dict)

        if direction == Links.Direction.UPSTREAM:
            # upstream is currently, always, only feature groups
            return Links.__parse_feature_groups(
                links_json["upstream"],
                {
                    "FEATURE_GROUP",
                    "EXTERNAL_FEATURE_GROUP",
                },
            )

        if direction == Links.Direction.DOWNSTREAM:
            if artifact == Links.Type.FEATURE_GROUP:
                return Links.__parse_feature_groups(
                    links_json["downstream"],
                    {
                        "FEATURE_GROUP",
                        "EXTERNAL_FEATURE_GROUP",
                    },
                )
            else:
                return Links.__parse_feature_views(
                    links_json["downstream"], {"FEATURE_VIEW"}
                )


class ProvenanceEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Links):
            return {
                "accessible": obj.accessible,
                "inaccessible": obj.inaccessible,
                "deleted": obj.deleted,
                "faulty": obj.faulty,
            }
        elif isinstance(
            obj,
            (
                feature_group.FeatureGroup,
                feature_group.ExternalFeatureGroup,
                feature_view.FeatureView,
                training_dataset.TrainingDataset,
                Artifact,
            ),
        ):
            return {
                "feature_store_name": obj.feature_store_name,
                "name": obj.name,
                "version": obj.version,
            }
        return json.JSONEncoder.default(self, obj)
