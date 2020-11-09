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
    CONST_ONLINE_FEATURE_STORE_CONNECTOR_SUFFIX = "_onlinefeaturestore"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def get(self, name, connector_type):
        """Get storage connector with name and type.

        :param name: name of the storage connector
        :type name: str
        :param connector_type: connector type
        :type connector_type: str
        :return: the storage connector
        :rtype: StorageConnector
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "storageconnectors",
            connector_type,
        ]
        result = [
            conn
            for conn in _client._send_request("GET", path_params)
            if conn["name"] == name
        ]

        if len(result) == 1:
            return storage_connector.StorageConnector.from_response_json(result[0])
        else:
            raise Exception(
                "Could not find the storage connector `{}` with type `{}`.".format(
                    name, connector_type
                )
            )

    def get_by_id(self, connector_id, connector_type):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "storageconnectors",
            connector_type,
            connector_id,
        ]
        return storage_connector.StorageConnector.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_online_connector(self):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "storageconnectors",
        ]

        result = [
            conn
            for conn in _client._send_request("GET", path_params)
            if self.CONST_ONLINE_FEATURE_STORE_CONNECTOR_SUFFIX in conn["name"]
        ]

        if len(result) > 0:
            return storage_connector.StorageConnector.from_response_json(result[0])
        else:
            raise Exception("Could not find online storage connector")
