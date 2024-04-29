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

from typing import Any, Dict

from hsfs import client, storage_connector


class StorageConnectorApi:
    def _get(self, feature_store_id: int, name: str) -> Dict[str, Any]:
        """Returning response dict instead of initialized object."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
        ]
        query_params = {"temporaryCredentials": True}
        return _client._send_request("GET", path_params, query_params=query_params)

    def get(
        self, feature_store_id: int, name: str
    ) -> storage_connector.StorageConnector:
        """Get storage connector with name.

        :param feature_store_id: feature store id
        :type feature_store_id: int
        :param name: name of the storage connector
        :type name: str
        :return: the storage connector
        :rtype: StorageConnector
        """
        return storage_connector.StorageConnector.from_response_json(
            self._get(feature_store_id, name)
        )

    def refetch(
        self, storage_connector_instance: storage_connector.StorageConnector
    ) -> storage_connector.StorageConnector:
        """
        Refetches the storage connector from Hopsworks, in order to update temporary
        credentials.
        """
        return storage_connector_instance.update_from_response_json(
            self._get(
                storage_connector_instance._featurestore_id,
                storage_connector_instance.name,
            )
        )

    def get_online_connector(
        self, feature_store_id: int
    ) -> storage_connector.OnlineStorageConnector:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            "onlinefeaturestore",
        ]

        return storage_connector.StorageConnector.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_kafka_connector(
        self, feature_store_id: int, external: bool = False
    ) -> storage_connector.KafkaConnector:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            "kafka_connector",
            "byok",
        ]
        query_params = {"external": external}

        return storage_connector.StorageConnector.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

    def get_feature_groups_provenance(
        self, storage_connector_instance
    ):
        """Get the generated feature groups using this storage connector, based on explicit
        provenance. These feature groups can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature group links, so deleted
        will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        # Arguments
            storage_connector_instance: Metadata object of storage connector.

        # Returns
            `ExplicitProvenance.Links`: the feature groups generated using this
            storage connector
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector_instance._featurestore_id,
            "storageconnectors",
            storage_connector_instance.name,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 0,
            "downstreamLvls": 1,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        from hsfs.core import explicit_provenance
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.DOWNSTREAM,
            explicit_provenance.Links.Type.FEATURE_GROUP,
        )
