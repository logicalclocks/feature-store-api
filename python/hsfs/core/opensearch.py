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
from __future__ import annotations

import logging
import re
from functools import wraps

import opensearchpy
import urllib3
from hsfs import client
from hsfs.client.exceptions import FeatureStoreException, VectorDatabaseException
from hsfs.core.opensearch_api import OpenSearchApi
from opensearchpy import OpenSearch
from opensearchpy.exceptions import (
    AuthenticationException,
    ConnectionError,
    ConnectionTimeout,
    RequestError,
)
from retrying import retry


def _is_timeout(exception):
    return isinstance(exception, urllib3.exceptions.ReadTimeoutError) or isinstance(
        exception, ConnectionTimeout
    )


def _handle_opensearch_exception(func):
    @wraps(func)
    def error_handler_wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except (ConnectionError, AuthenticationException):
            # OpenSearchConnectionError occurs when connection is closed.
            # OpenSearchAuthenticationException occurs when jwt is expired
            OpenSearchClientSingleton()._refresh_opensearch_connection()
            return func(*args, **kw)
        except RequestError as e:
            caused_by = e.info.get("error") and e.info["error"].get("caused_by")
            if caused_by and caused_by["type"] == "illegal_argument_exception":
                raise OpenSearchClientSingleton()._create_vector_database_exception(
                    caused_by["reason"]) from e
            raise VectorDatabaseException(
                VectorDatabaseException.OTHERS,
                f"Error in Opensearch request: {e}",
                e.info,
            ) from e
        except Exception as e:
            if _is_timeout(e):
                raise FeatureStoreException(
                    OpenSearchClientSingleton.TIMEOUT_ERROR_MSG
                ) from e
            else:
                raise e

    return error_handler_wrapper


class OpensearchRequestOption:
    DEFAULT_OPTION_MAP = {
        "timeout": "30s",
    }

    DEFAULT_OPTION_MAP_V2_3 = {
        # starting from opensearch client v2.3, timeout should be in int/float
        # https://github.com/opensearch-project/opensearch-py/pull/394
        "timeout": 30,
    }

    @classmethod
    def get_version(cls):
        return opensearchpy.__version__[0:2]

    @classmethod
    def get_options(cls, options: dict):
        """
        Construct a map of options for the request to the vector database.

        Args:
            options (dict): The options used for the request to the vector database.
                The keys are attribute values of the OpensearchRequestOption class.

        Returns:
            dict: A dictionary containing the constructed options map, where keys represent
            attribute values of the OpensearchRequestOption class, and values are obtained
            either from the provided options or default values if not available.
        """
        default_option = (cls.DEFAULT_OPTION_MAP
                          if cls.get_version() < (2, 3)
                          else cls.DEFAULT_OPTION_MAP_V2_3)
        if options:
            # make lower case to avoid issues with cases
            options = {k.lower(): v for k, v in options.items()}
            new_options = {}
            for option, value in default_option.items():
                if option in options:
                    if (option == "timeout"
                        and cls.get_version() < (2, 3)
                        and isinstance(options[option], int)
                    ):
                        new_options[option] = f"{options[option]}s"
                    else:
                        new_options[option] = options[option]
                else:
                    new_options[option] = value
            return new_options
        else:
            return default_option


class OpenSearchClientSingleton:
    _instance = None

    TIMEOUT_ERROR_MSG = """
    Cannot fetch results from Opensearch due to timeout. It is because the server is busy right now or longer time is needed to reload a large index. Try and increase the timeout limit by providing the parameter `options={"timeout": 60}` in the method `find_neighbor` or `count`.
    """

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(OpenSearchClientSingleton, cls).__new__(cls)
            cls._instance._opensearch_client = None
            cls._instance._setup_opensearch_client()
        return cls._instance

    def _setup_opensearch_client(self):
        if not self._opensearch_client:
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

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_is_timeout,
    )
    @_handle_opensearch_exception
    def search(self, index=None, body=None, options=None):
        return self._opensearch_client.search(body=body, index=index, params=OpensearchRequestOption.get_options(options))

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_is_timeout,
    )
    @_handle_opensearch_exception
    def count(self, index, body=None, options=None):
        result = self._opensearch_client.count(
            index=index, body=body, params=OpensearchRequestOption.get_options(options)
        )
        return result["count"]

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
