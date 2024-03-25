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

import logging
import re
import urllib3

from hsfs import client
from hsfs.client.exceptions import FeatureStoreException, VectorDatabaseException
from hsfs.core.opensearch_api import OpenSearchApi
from retrying import retry


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
                from opensearchpy import OpenSearch
                from opensearchpy.exceptions import (
                    AuthenticationException as OpenSearchAuthenticationException,
                )
                from opensearchpy.exceptions import (
                    ConnectionError as OpenSearchConnectionError,
                )
                from opensearchpy.exceptions import (
                    RequestError as RequestError,
                )

                self.OpenSearchConnectionError = OpenSearchConnectionError
                self.OpenSearchAuthenticationException = (
                    OpenSearchAuthenticationException
                )
                self.RequestError = RequestError

            except ModuleNotFoundError as err:
                raise FeatureStoreException(
                    "hopsworks and opensearchpy are required for embedding similarity search"
                ) from err
            # query log is at INFO level
            # 2023-11-24 15:10:49,470 INFO: POST https://localhost:9200/index/_search [status:200 request:0.041s]
            logging.getLogger("opensearchpy").setLevel(logging.WARNING)
            self._opensearch_client = OpenSearch(
                **OpenSearchApi(
                    client.get_instance()._project_id,
                    client.get_instance()._project_name,
                ).get_default_py_config()
            )

    def _refresh_opensearch_connection(self):
        self._opensearch_client.close()
        self._opensearch_client = None
        self._setup_opensearch_client()

    def _is_timeout(self, exception):
        return isinstance(exception, urllib3.exceptions.ReadTimeoutError)

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=3,
        retry_on_exception=_is_timeout,
    )
    def search(self, index=None, body=None):
        try:
            return self._opensearch_client.search(body=body, index=index)
        except (self.OpenSearchConnectionError, self.OpenSearchAuthenticationException):
            # OpenSearchConnectionError occurs when connection is closed.
            # OpenSearchAuthenticationException occurs when jwt is expired
            self._refresh_opensearch_connection()
            return self._opensearch_client.search(body=body, index=index)
        except self.RequestError as e:
            caused_by = e.info.get("error") and e.info["error"].get("caused_by")
            if caused_by and caused_by["type"] == "illegal_argument_exception":
                raise self._create_vector_database_exception(caused_by["reason"]) from e
            raise VectorDatabaseException(
                VectorDatabaseException.OTHERS,
                f"Error in Opensearch request: {e}",
                e.info,
            )  from e

    def close(self):
        if self._opensearch_client:
            self._opensearch_client.close()

    def _create_vector_database_exception(self, message):
        if "[knn] requires k" in message:
            pattern = r"\[knn\] requires k <= (\d+)"
            match = re.search(pattern, message)
            if match:
                k = match.group(1)
                reason = VectorDatabaseException.REQUESTED_K_TOO_LARGE
                message = (
                    f"Illegal argument in vector database request: "
                    f"Requested k is too large, it needs to be less than {k}."
                )
                info = {VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K: int(k)}
            else:
                reason = VectorDatabaseException.REQUESTED_K_TOO_LARGE
                message = "Illegal argument in vector database request: Requested k is too large."
                info = {}
        elif "Result window is too large" in message:
            pattern = r"or equal to: \[(\d+)\]"
            match = re.search(pattern, message)
            if match:
                n = match.group(1)
                reason = VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE
                message = (
                    f"Illegal argument in vector database request: "
                    f"Requested n is too large, it needs to be less than {n}."
                )
                info = {
                    VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N: int(
                        n
                    )
                }
            else:
                reason = VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE
                message = (
                    "Illegal argument in vector database request: "
                    "Requested n is too large."
                )
                info = {}
        else:
            reason = VectorDatabaseException.OTHERS
            message = message
            info = {}
        return VectorDatabaseException(reason, message, info)
