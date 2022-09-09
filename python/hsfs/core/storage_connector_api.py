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

from hsfs import client, storage_connector


class StorageConnectorApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def _get(self, name, session_duration):
        """Returning response dict instead of initialized object."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "storageconnectors",
            name,
        ]
        query_params = {"temporaryCredentials": True}
        if session_duration is not None:
            query_params["durationSeconds"] = session_duration
        return _client._send_request("GET", path_params, query_params=query_params)

    def get(self, name, session_duration):
        """Get storage connector with name and type.

        :param name: name of the storage connector
        :type name: str
        :return: the storage connector
        :rtype: StorageConnector
        """
        connector = storage_connector.StorageConnector.from_response_json(
            self._get(name, session_duration)
        )
        connector._session_duration = session_duration
        return connector

    def refetch(self, storage_connector_instance):
        """
        Refetches the storage connector from Hopsworks, in order to update temporary
        credentials.
        """
        return storage_connector_instance.update_from_response_json(
            self._get(
                storage_connector_instance.name,
                storage_connector_instance._session_duration,
            ),
            storage_connector_instance._session_duration,
        )

    def get_online_connector(self):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "storageconnectors",
            "onlinefeaturestore",
        ]

        return storage_connector.StorageConnector.from_response_json(
            _client._send_request("GET", path_params)
        )
