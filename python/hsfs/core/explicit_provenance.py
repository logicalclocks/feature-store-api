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
from __future__ import annotations

import json
from enum import Enum
from typing import Optional, Set

import humps
from hsfs import feature_group, feature_view, storage_connector, training_dataset, util


class Artifact:
    class MetaType(Enum):
        DELETED = 1
        INACCESSIBLE = 2
        FAULTY = 3
        NOT_SUPPORTED = 4

        def json(self):
            return json.dumps(self, cls=util.FeatureStoreEncoder)

        def to_dict(self):
            return self.name

        def __str__(self):
            return self.json()

        def __repr__(self):
            return f"<MetaType.{self.name}>"

    def __init__(
        self,
        feature_store_name,
        name,
        version,
        type,
        meta_type,
        href=None,
        exception_cause=None,
        **kwargs,
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

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "feature_store_name": self._feature_store_name,
            "name": self.name,
            "version": self._version,
            "type": self._type,
            "meta_type": self._meta_type,
            "href": self._href,
            "err": self._exception_cause,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"Artifact({self._feature_store_name!r}, {self._name!r}, "
            f"{self._version!r}, {self._type!r}, {self._meta_type!r}, "
            f"{self._href!r}, {self._exception_cause!r})"
        )

    @staticmethod
    def from_response_json(json_dict: dict):
        link_json = humps.decamelize(json_dict)
        href = None
        exception_cause = None
        if link_json.get("exception_cause") is not None:
            meta_type = Artifact.MetaType.FAULTY
            exception_cause = link_json.get("exception_cause")
        elif bool(link_json["deleted"]):
            meta_type = Artifact.MetaType.DELETED
        elif not bool(link_json["accessible"]):
            meta_type = Artifact.MetaType.INACCESSIBLE
            href = link_json["artifact"]["href"]
        else:
            meta_type = Artifact.MetaType.NOT_SUPPORTED
            href = link_json["artifact"]["href"]
        return Artifact(
            link_json["artifact"]["project"],
            link_json["artifact"]["name"],
            link_json["artifact"]["version"],
            link_json["artifact_type"],
            meta_type,
            href=href,
            exception_cause=exception_cause,
        )


class Links:
    def __init__(self):
        self._accessible = []
        self._deleted = []
        self._inaccessible = []
        self._faulty = []

    @property
    def deleted(self):
        """List of [Artifact objects] which contains
        minimal information (name, version) about the entities
        (storage connectors, feature groups, feature views, models) they represent.
        These entities have been removed from the feature store/model registry.
        """
        return self._deleted

    @property
    def inaccessible(self):
        """List of [Artifact objects] which contains
        minimal information (name, version) about the entities
        (storage connectors, feature groups, feature views, models) they represent.
        These entities exist in the feature store/model registry, however the user
        does not have access to them anymore.
        """
        return self._inaccessible

    @property
    def accessible(self):
        """List of [StorageConnectors|FeatureGroups|FeatureViews|Models] objects
        which are part of the provenance graph requested. These entities
        exist in the feature store/model registry and the user has access to them.
        """
        return self._accessible

    @property
    def faulty(self):
        """List of [Artifact objects] which contains
        minimal information (name, version) about the entities
        (storage connectors, feature groups, feature views, models) they represent.
        These entities exist in the feature store/model registry, however they are corrupted.
        """
        return self._faulty

    class Direction(Enum):
        UPSTREAM = 1
        DOWNSTREAM = 2

    class Type(Enum):
        FEATURE_GROUP = 1
        FEATURE_VIEW = 2
        MODEL = 3
        STORAGE_CONNECTOR = 4

    def __str__(self, indent=None):
        return json.dumps(self, cls=ProvenanceEncoder, indent=indent)

    def __repr__(self):
        return (
            f"Links({self._accessible!r}, {self._deleted!r}"
            f", {self._inaccessible!r}, {self._faulty!r})"
        )

    @staticmethod
    def __parse_storage_connector(links_json: dict, artifacts: Set[str]):
        links = Links()
        for link_json in links_json:
            if link_json["node"]["artifact_type"] in artifacts:
                if link_json["node"].get("exception_cause") is not None:
                    links._faulty.append(Artifact.from_response_json(link_json["node"]))
                elif bool(link_json["node"]["accessible"]):
                    links.accessible.append(
                        storage_connector.StorageConnector.from_response_json(
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
    def __parse_models(
        links_json: dict, training_dataset_version: Optional[int] = None
    ):
        from hsml import model
        from hsml.core import explicit_provenance as hsml_explicit_provenance

        links = Links()
        for link_json in links_json:
            if link_json["node"]["artifact_type"] == "MODEL":
                if link_json["node"].get("exception_cause") is not None:
                    links._faulty.append(
                        hsml_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
                elif bool(link_json["node"]["accessible"]):
                    links.accessible.append(
                        model.Model.from_response_json(link_json["node"]["artifact"])
                    )
                elif bool(link_json["node"]["deleted"]):
                    links.deleted.append(
                        hsml_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
                else:
                    links.inaccessible.append(
                        hsml_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
            else:
                # the only artifact types here are MODEL and TRAINING_DATASET
                # elif link_json["node"]["artifact_type"] == "TRAINING_DATASET":
                if (
                    training_dataset_version
                    and link_json["node"]["artifact"]["version"]
                    != training_dataset_version
                ):
                    # Skip the following operations if the versions don't match
                    pass
                else:
                    new_links = Links.__parse_models(link_json["downstream"])
                    links.faulty.extend(new_links.faulty)
                    links.accessible.extend(new_links.accessible)
                    links.inaccessible.extend(new_links.inaccessible)
                    links.deleted.extend(new_links.deleted)

        return links

    @staticmethod
    def from_response_json(
        json_dict: dict,
        direction: Direction,
        artifact: Type,
        training_dataset_version: Optional[int] = None,
    ):
        """Parse explicit links from json response. There are four types of
        Links: UpstreamStorageConnectors, UpstreamFeatureGroups, DownstreamFeatureGroups, DownstreamFeatureViews

        # Arguments
            json_dict: json response from the explicit provenance endpoint
            direction: subset of links to parse - UPSTREAM/DOWNSTREAM
            artifact: subset of links to parse - STORAGE_CONNECTOR/FEATURE_GROUP/FEATURE_VIEW/MODEL
            training_dataset_version: training dataset version

        # Returns
            A ProvenanceLink object for the selected parse type.
        """
        links_json = humps.decamelize(json_dict)

        if direction == Links.Direction.DOWNSTREAM and artifact == Links.Type.MODEL:
            import importlib.util

            if not importlib.util.find_spec("hsml"):
                raise Exception(
                    "hsml is not installed in the environment - cannot parse model registry artifacts"
                )
            if not importlib.util.find_spec("hopsworks"):
                raise Exception(
                    "hopsworks is not installed in the environment - cannot switch from hsml connection to hsfs connection"
                )

            # make sure the hsml connection is initialized so that the model can actually be used after being returned
            import hopsworks

            if not hopsworks._connected_project:
                raise Exception(
                    "hopsworks connection is not initialized - use hopsworks.login to connect if you want the ability to use provenance with connections between hsfs and hsml"
                )

            hopsworks._connected_project.get_model_registry()

            return Links.__parse_models(
                links_json["downstream"],
                training_dataset_version=training_dataset_version,
            )
        else:
            if direction == Links.Direction.UPSTREAM:
                if artifact == Links.Type.FEATURE_GROUP:
                    return Links.__parse_feature_groups(
                        links_json["upstream"],
                        {
                            "FEATURE_GROUP",
                            "EXTERNAL_FEATURE_GROUP",
                        },
                    )
                elif artifact == Links.Type.STORAGE_CONNECTOR:
                    return Links.__parse_storage_connector(
                        links_json["upstream"], {"STORAGE_CONNECTOR"}
                    )
                else:
                    return Links()

            if direction == Links.Direction.DOWNSTREAM:
                if artifact == Links.Type.FEATURE_GROUP:
                    return Links.__parse_feature_groups(
                        links_json["downstream"],
                        {
                            "FEATURE_GROUP",
                            "EXTERNAL_FEATURE_GROUP",
                        },
                    )
                elif artifact == Links.Type.FEATURE_VIEW:
                    return Links.__parse_feature_views(
                        links_json["downstream"], {"FEATURE_VIEW"}
                    )
                else:
                    return Links()


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
        elif isinstance(
            obj,
            (
                storage_connector.StorageConnector
            ),
        ):
            return {
                "name": obj.name,
            }
        else:
            import importlib.util

            if importlib.util.find_spec("hsml"):
                from hsml import model
                from hsml.core import explicit_provenance as hsml_explicit_provenance

                if isinstance(
                    obj,
                    (
                        model.Model,
                        hsml_explicit_provenance.Artifact,
                    ),
                ):
                    return {
                        "model_registry_id": obj.model_registry_id,
                        "name": obj.name,
                        "version": obj.version,
                    }
            return json.JSONEncoder.default(self, obj)
