#
#   Copyright 2023 Logical Clocks AB
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
from hsfs.client.external import Client
from hsfs.client.exceptions import FeatureStoreException
import logging


class OpenSearchClientSingleton:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(OpenSearchClientSingleton, cls).__new__(cls)
            cls._instance._opensearch_client = None
            cls._instance._setup_opensearch_client()
        return cls._instance

    def _setup_opensearch_client(self):
        if not self._opensearch_client:
            try:
                import hopsworks
                from opensearchpy import OpenSearch
                from opensearchpy.exceptions import (
                    ConnectionError as OpenSearchConnectionError,
                )

                self.OpenSearchConnectionError = OpenSearchConnectionError
            except ModuleNotFoundError:
                raise FeatureStoreException(
                    "hopsworks and opensearchpy are required for embedding similarity search"
                )
            # query log is at INFO level
            # 2023-11-24 15:10:49,470 INFO: POST https://localhost:9200/index/_search [status:200 request:0.041s]
            logging.getLogger("opensearchpy").setLevel(logging.WARNING)
            if not hopsworks._connected_project:
                if isinstance(client.get_instance(), Client):
                    hopsworks.login(
                        host=client.get_instance().host,
                        port=client.get_instance()._port,
                        project=client.get_instance()._project_name,
                        api_key_value=client.get_instance()._auth._token,
                    )
                else:
                    hopsworks.login()
            opensearch_api = hopsworks._connected_project.get_opensearch_api()
            self._opensearch_client = OpenSearch(
                **opensearch_api.get_default_py_config()
            )

    def _refresh_opensearch_connection(self):
        self._opensearch_client.close()
        self._opensearch_client = None
        self._setup_opensearch_client()

    def search(self, index=None, body=None):
        try:
            return self._opensearch_client.search(body=body, index=index)
        except self.OpenSearchConnectionError:
            self._refresh_opensearch_connection()
            return self._opensearch_client.search(body=body, index=index)

    def close(self):
        if self._opensearch_client:
            self._opensearch_client.close()
