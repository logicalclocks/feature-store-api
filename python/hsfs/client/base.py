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

import os
import furl
from abc import ABC, abstractmethod

import requests
import urllib3

from hsfs.client import exceptions, auth
from hsfs.decorators import connected


urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Client(ABC):
    TOKEN_FILE = "token.jwt"
    REST_ENDPOINT = "REST_ENDPOINT"
    DEFAULT_DATABRICKS_ROOT_VIRTUALENV_ENV = "DEFAULT_DATABRICKS_ROOT_VIRTUALENV_ENV"

    @abstractmethod
    def __init__(self):
        """To be implemented by clients."""
        pass

    def _get_verify(self, verify, trust_store_path):
        """Get verification method for sending HTTP requests to Hopsworks.

        Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c

        :param verify: perform hostname verification, 'true' or 'false'
        :type verify: str
        :param trust_store_path: path of the truststore locally if it was uploaded manually to
            the external environment such as AWS Sagemaker
        :type trust_store_path: str
        :return: if verify is true and the truststore is provided, then return the trust store location
                 if verify is true but the truststore wasn't provided, then return true
                 if verify is false, then return false
        :rtype: str or boolean
        """
        if verify == "true":
            if trust_store_path is not None:
                return trust_store_path
            else:
                return True

        return False

    def _get_host_port_pair(self):
        """
        Removes "http or https" from the rest endpoint and returns a list
        [endpoint, port], where endpoint is on the format /path.. without http://

        :return: a list [endpoint, port]
        :rtype: list
        """
        endpoint = self._base_url
        if "http" in endpoint:
            last_index = endpoint.rfind("/")
            endpoint = endpoint[last_index + 1 :]
        host, port = endpoint.split(":")
        return host, port

    def _read_jwt(self):
        """Retrieve jwt from local container."""
        with open(os.path.join(self._secrets_dir, self.TOKEN_FILE), "r") as jwt:
            return jwt.read()

    @connected
    def _send_request(
        self,
        method,
        path_params,
        query_params=None,
        headers=None,
        data=None,
        stream=False,
        files=None,
    ):
        """Send REST request to Hopsworks.

        Uses the client it is executed from. Path parameters are url encoded automatically.

        :param method: 'GET', 'PUT' or 'POST'
        :type method: str
        :param path_params: a list of path params to build the query url from starting after
            the api resource, for example `["project", 119, "featurestores", 67]`.
        :type path_params: list
        :param query_params: A dictionary of key/value pairs to be added as query parameters,
            defaults to None
        :type query_params: dict, optional
        :param headers: Additional header information, defaults to None
        :type headers: dict, optional
        :param data: The payload as a python dictionary to be sent as json, defaults to None
        :type data: dict, optional
        :param stream: Set if response should be a stream, defaults to False
        :type stream: boolean, optional
        :param files: dictionary for multipart encoding upload
        :type files: dict, optional
        :raises RestAPIError: Raised when request wasn't correctly received, understood or accepted
        :return: Response json
        :rtype: dict
        """
        base_path_params = ["hopsworks-api", "api"]
        f_url = furl.furl(self._base_url)
        f_url.path.segments = base_path_params + path_params
        url = str(f_url)

        request = requests.Request(
            method,
            url=url,
            headers=headers,
            data=data,
            params=query_params,
            auth=self._auth,
            files=files,
        )

        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, verify=self._verify, stream=stream)

        if response.status_code == 401 and self.REST_ENDPOINT in os.environ:
            # refresh token and retry request - only on hopsworks
            self._auth = auth.BearerAuth(self._read_jwt())
            # Update request with the new token
            request.auth = self._auth
            prepped = self._session.prepare_request(request)
            response = self._session.send(prepped, verify=self._verify, stream=stream)

        if response.status_code // 100 != 2:
            raise exceptions.RestAPIError(url, response)

        if stream:
            return response
        else:
            # handle different success response codes
            if len(response.content) == 0:
                return None
            return response.json()

    def _close(self):
        """Closes a client. Can be implemented for clean up purposes, not mandatory."""
        self._connected = False
