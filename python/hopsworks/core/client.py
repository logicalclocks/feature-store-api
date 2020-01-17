import os
import socket
from OpenSSL import SSL
from cryptography import x509
from cryptography.x509.oid import NameOID
import idna
import furl
import boto3
import json
import base64
from abc import ABC, abstractmethod

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BaseClient(ABC):
    TOKEN_FILE = "token.jwt"
    REST_ENDPOINT = "REST_ENDPOINT"
    REQUESTS_VERIFY = "REQUESTS_VERIFY"
    DOMAIN_CA_TRUSTSTORE_PEM = "DOMAIN_CA_TRUSTSTORE_PEM"

    @abstractmethod
    def __init__(self):
        pass

    def _get_verify(self, host, port, verify, trust_store_path):
        """
        Get verification method for sending HTTP requests to Hopsworks.
        Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c
        Returns:
            if env var HOPS_UTIL_VERIFY is not false
                then if hopsworks certificate is self-signed, return the path to the
                truststore (PEM)
            else if hopsworks is not self-signed, return true
            return false
        """
        if verify == "true":

            hostname_idna = idna.encode(host)
            sock = socket.socket()

            sock.connect((host, int(port)))
            ctx = SSL.Context(SSL.SSLv23_METHOD)
            ctx.check_hostname = False
            ctx.verify_mode = SSL.VERIFY_NONE

            sock_ssl = SSL.Connection(ctx, sock)
            sock_ssl.set_connect_state()
            sock_ssl.set_tlsext_host_name(hostname_idna)
            sock_ssl.do_handshake()
            cert = sock_ssl.get_peer_certificate()
            crypto_cert = cert.to_cryptography()
            sock_ssl.close()
            sock.close()

            try:
                commonname = crypto_cert.subject.get_attributes_for_oid(
                    NameOID.COMMON_NAME
                )[0].value
                issuer = crypto_cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME)[
                    0
                ].value
                if commonname == issuer and trust_store_path:
                    return trust_store_path
                else:
                    return True
            except x509.ExtensionNotFound:
                return True

        return False

    def _get_hopsworks_rest_endpoint(self):
        """Get the hopsworks REST endpoint for making requests to the REST API
        """
        return os.environ[self.REST_ENDPOINT]

    def _get_host_port_pair(self):
        """
        Removes "http or https" from the rest endpoint and returns a list
        [endpoint, port], where endpoint is on the format /path.. without http://

        Returns:
            a list [endpoint, port]
        """
        endpoint = self._base_url
        if "http" in endpoint:
            last_index = endpoint.rfind("/")
            endpoint = endpoint[last_index + 1 :]
        host, port = endpoint.split(":")
        return host, port

    def _read_jwt(self):
        """
        Retrieves jwt from local container.

        Returns:
            Content of jwt.token file in local container.
        """
        with open(self.TOKEN_FILE, "r") as jwt:
            return jwt.read()

    def _send_request(
        self, method, path_params, query_params=None, headers=None, data=None
    ):
        f_url = furl.furl(self._base_url)
        f_url.path.segments = path_params
        url = str(f_url)

        request = requests.Request(
            method,
            url=url,
            headers=headers,
            data=data,
            params=query_params,
            auth=self._auth,
        )

        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, verify=self._verify)

        if response.status_code // 100 != 2:
            raise RestAPIError(url, response)
        else:
            return response.json()


class HopsworksClient(BaseClient):
    def __init__(self):
        self._base_url = self._get_hopsworks_rest_endpoint()
        host, port = self._get_host_port_pair()
        # TODO(Fabio) : Have a thread that refreshes the token
        trust_store_path = (
            os.environ[self.DOMAIN_CA_TRUSTSTORE_PEM]
            if self.DOMAIN_CA_TRUSTSTORE_PEM in os.environ
            else None
        )
        hostname_verification = (
            os.environ[self.REQUESTS_VERIFY]
            if self.REQUESTS_VERIFY in os.environ
            else "true"
        )
        self._auth = BearerAuth(self._read_jwt())
        self._verify = self._get_verify(
            host, port, hostname_verification, trust_store_path
        )
        self._session = requests.session()


class ExternalClient(BaseClient):
    DEFAULT_REGION = "default"
    SECRETS_MANAGER = "secretsmanager"
    PARAMETER_STORE = "parameterstore"
    LOCAL_STORE = "local"

    def __init__(
        self,
        host,
        port,
        project,
        region_name,
        secrets_store,
        hostname_verification,
        trust_store_path,
        cert_folder,
        api_key_file,
    ):
        if not host:
            # TODO: make custom exception missing possitional arguments
            raise TypeError(
                "host cannot be of type NoneType, host is a non-optional "
                "argument to connect to hopsworks from an external environment."
            )
        if not project:
            raise TypeError(
                "project cannot be of type NoneType, project is a "
                "non-optional argument to connect to hopsworks from an external "
                "environment"
            )

        self._base_url = "https://" + host + ":" + str(port)
        self._project = project
        self._region_name = region_name
        self._cert_folder = cert_folder

        self._auth = ApiKeyAuth(
            self._get_secret(secrets_store, "api-key", api_key_file)
        )

        self._session = requests.session()
        self._verify = self._get_verify(
            host, port, hostname_verification, trust_store_path
        )

        project_info = self._get_project_info(project)
        self._project_id = str(project_info["projectId"])

        credentials = self._get_credentials(self._project_id)
        self._write_b64_cert_to_bytes(
            str(credentials["kStore"]), path=os.path.join(cert_folder, "keyStore.jks")
        )
        self._write_b64_cert_to_bytes(
            str(credentials["tStore"]), path=os.path.join(cert_folder, "trustStore.jks")
        )

    def _close(self):
        self._cleanup_certs(os.path.join(self._cert_folder, "keyStore.jks"))
        self._cleanup_certs(os.path.join(self._cert_folder, "trustStore.jks"))

    def _get_secret(self, secrets_store, secret_key=None, api_key_file=None):
        """
        Returns secret value from the AWS Secrets Manager or Parameter Store

        Args:
            :secrets_store: the underlying secrets storage to be used, e.g. `secretsmanager` or `parameterstore`
            :secret_type (str): key for the secret value, e.g. `api-key`, `cert-key`, `trust-store`, `key-store`
            :api_token_file: path to a file containing an api key
        Returns:
            :str: secret value
        """
        if secrets_store == self.SECRETS_MANAGER:
            return self._query_secrets_manager(secret_key)
        elif secrets_store == self.PARAMETER_STORE:
            return self._query_parameter_store(secret_key)
        elif secrets_store == self.LOCAL_STORE:
            if not api_key_file:
                raise Exception("api_key_file needs to be set for local mode")
            with open(api_key_file) as f:
                return f.readline().strip()
        else:
            raise UnkownSecretStorageError(
                "Secrets storage " + secrets_store + " is not supported."
            )

    def _query_secrets_manager(self, secret_key):
        secret_name = "hopsworks/role/" + self._assumed_role()
        args = {"service_name": "secretsmanager"}
        region_name = self._get_region()
        if region_name:
            args["region_name"] = region_name
        client = boto3.client(**args)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return json.loads(get_secret_value_response["SecretString"])[secret_key]

    def _assumed_role(self):
        client = boto3.client("sts")
        response = client.get_caller_identity()
        # arns for assumed roles in SageMaker follow the following schema
        # arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
        local_identifier = response["Arn"].split(":")[-1].split("/")
        if len(local_identifier) != 3 or local_identifier[0] != "assumed-role":
            raise Exception(
                "Failed to extract assumed role from arn: " + response["Arn"]
            )
        return local_identifier[1]

    def _get_region(self):
        if self._region_name != self.DEFAULT_REGION:
            return self._region_name
        else:
            return None

    def _query_parameter_store(self, secret_key):
        args = {"service_name": "ssm"}
        region_name = self._get_region()
        if region_name:
            args["region_name"] = region_name
        client = boto3.client(**args)
        name = "/hopsworks/role/" + self._assumed_role() + "/type/" + secret_key
        return client.get_parameter(Name=name, WithDecryption=True)["Parameter"][
            "Value"
        ]

    def _get_project_info(self, project_name):
        """
        Makes a REST call to hopsworks to get all metadata of a project for the provided project.

        Args:
            :project_name: the name of the project

        Returns:
            JSON response

        Raises:
            :RestAPIError: if there was an error in the REST call to Hopsworks
        """
        return self._send_request(
            "GET", ["hopsworks-api", "api", "project", "getProjectInfo", project_name]
        )

    def _get_credentials(self, project_id):
        """
        Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive

        Args:
            :project_name: id of the project

        Returns:
            JSON response

        Raises:
            :RestAPIError: if there was an error in the REST call to Hopsworks
        """
        return self._send_request(
            "GET", ["hopsworks-api", "api", "project", project_id, "credentials"]
        )

    def _write_b64_cert_to_bytes(self, b64_string, path):
        """Converts b64 encoded certificate to bytes file .

        Args:
            :b64_string (str): b64 encoded string of certificate
            :path (str): path where file is saved, including file name. e.g. /path/key-store.jks
        """

        with open(path, "wb") as f:
            cert_b64 = base64.b64decode(b64_string)
            f.write(cert_b64)

    def _cleanup_certs(self, file_path):
        try:
            os.remove(file_path)
        except OSError:
            pass


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self._token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self._token
        return r


class ApiKeyAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self._token = token

    def __call__(self, r):
        r.headers["Authorization"] = "ApiKey " + self._token
        return r


class RestAPIError(Exception):
    """REST Exception encapsulating the response object and url.
    """

    def __init__(self, url, response):
        error_object = response.json()
        message = (
            "Metadata operation error: (url: {}). Server response: \n"
            "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user "
            "msg: {}".format(
                url,
                response.status_code,
                response.reason,
                error_object.get("errorCode", ""),
                error_object.get("errorMsg", ""),
                error_object.get("usrMsg", ""),
            )
        )
        super().__init__(message)
        self.url = url
        self.response = response


class UnkownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter"""
