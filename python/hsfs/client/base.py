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

import base64
import os
import textwrap
import furl
from pathlib import Path
from abc import ABC, abstractmethod

import requests
import urllib3

from hsfs.client import exceptions, auth
from hsfs.decorators import connected

try:
    import jks
except ImportError:
    pass


urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Client(ABC):
    TOKEN_FILE = "token.jwt"
    APIKEY_FILE = "api.key"
    REST_ENDPOINT = "REST_ENDPOINT"
    DEFAULT_DATABRICKS_ROOT_VIRTUALENV_ENV = "DEFAULT_DATABRICKS_ROOT_VIRTUALENV_ENV"
    HOPSWORKS_PUBLIC_HOST = "HOPSWORKS_PUBLIC_HOST"

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
        return self._read_file(self.TOKEN_FILE)

    def _read_apikey(self):
        """Retrieve apikey from local container."""
        return self._read_file(self.APIKEY_FILE)

    def _read_file(self, secret_file):
        """Retrieve secret from local container."""
        with open(os.path.join(self._secrets_dir, secret_file), "r") as secret:
            return secret.read()

    def _get_credentials(self, project_id):
        """Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive

        :param project_id: id of the project
        :type project_id: int
        :return: JSON response with credentials
        :rtype: dict
        """
        return self._send_request("GET", ["project", project_id, "credentials"])

    def _write_pem_file(self, content: str, path: str) -> None:
        with open(path, "w") as f:
            f.write(content)

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
        :raises hsfs.client.exceptions.RestAPIError: Raised when request wasn't correctly received, understood or accepted
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

    def _write_pem(
        self, keystore_path, keystore_pw, truststore_path, truststore_pw, prefix
    ):
        ks = jks.KeyStore.load(Path(keystore_path), keystore_pw, try_decrypt_keys=True)
        ts = jks.KeyStore.load(
            Path(truststore_path), truststore_pw, try_decrypt_keys=True
        )

        ca_chain_path = os.path.join("/tmp", f"{prefix}_ca_chain.pem")
        self._write_ca_chain(ks, ts, ca_chain_path)

        client_cert_path = os.path.join("/tmp", f"{prefix}_client_cert.pem")
        self._write_client_cert(ks, client_cert_path)

        client_key_path = os.path.join("/tmp", f"{prefix}_client_key.pem")
        self._write_client_key(ks, client_key_path)

        return ca_chain_path, client_cert_path, client_key_path

    def _write_ca_chain(self, ks, ts, ca_chain_path):
        """
        Converts JKS keystore and truststore file into ca chain PEM to be compatible with Python libraries
        """
        ca_chain = ""
        for store in [ks, ts]:
            for _, c in store.certs.items():
                ca_chain = ca_chain + self._bytes_to_pem_str(c.cert, "CERTIFICATE")

        with Path(ca_chain_path).open("w") as f:
            f.write(ca_chain)

    def _write_client_cert(self, ks, client_cert_path):
        """
        Converts JKS keystore file into client cert PEM to be compatible with Python libraries
        """
        client_cert = ""
        for _, pk in ks.private_keys.items():
            for c in pk.cert_chain:
                client_cert = client_cert + self._bytes_to_pem_str(c[1], "CERTIFICATE")

        with Path(client_cert_path).open("w") as f:
            f.write(client_cert)

    def _write_client_key(self, ks, client_key_path):
        """
        Converts JKS keystore file into client key PEM to be compatible with Python libraries
        """
        client_key = ""
        for _, pk in ks.private_keys.items():
            client_key = client_key + self._bytes_to_pem_str(
                pk.pkey_pkcs8, "PRIVATE KEY"
            )

        with Path(client_key_path).open("w") as f:
            f.write(client_key)

    def _bytes_to_pem_str(self, der_bytes, pem_type):
        """
        Utility function for creating PEM files

        Args:
            der_bytes: DER encoded bytes
            pem_type: type of PEM, e.g Certificate, Private key, or RSA private key

        Returns:
            PEM String for a DER-encoded certificate or private key
        """
        pem_str = ""
        pem_str = pem_str + "-----BEGIN {}-----".format(pem_type) + "\n"
        pem_str = (
            pem_str
            + "\r\n".join(
                textwrap.wrap(base64.b64encode(der_bytes).decode("ascii"), 64)
            )
            + "\n"
        )
        pem_str = pem_str + "-----END {}-----".format(pem_type) + "\n"
        return pem_str
