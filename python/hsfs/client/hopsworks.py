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
from pathlib import Path

import requests

from hsfs.client import base, auth

try:
    import jks
except ImportError:
    pass


class Client(base.Client):
    REQUESTS_VERIFY = "REQUESTS_VERIFY"
    DOMAIN_CA_TRUSTSTORE_PEM = "DOMAIN_CA_TRUSTSTORE_PEM"
    PROJECT_ID = "HOPSWORKS_PROJECT_ID"
    PROJECT_NAME = "HOPSWORKS_PROJECT_NAME"
    HADOOP_USER_NAME = "HADOOP_USER_NAME"
    MATERIAL_DIRECTORY = "MATERIAL_DIRECTORY"
    HDFS_USER = "HDFS_USER"
    T_CERTIFICATE = "t_certificate"
    K_CERTIFICATE = "k_certificate"
    TRUSTSTORE_SUFFIX = "__tstore.jks"
    KEYSTORE_SUFFIX = "__kstore.jks"
    PEM_CA_CHAIN = "ca_chain.pem"
    CERT_KEY_SUFFIX = "__cert.key"
    MATERIAL_PWD = "material_passwd"
    SECRETS_DIR = "SECRETS_DIR"

    def __init__(self):
        """Initializes a client being run from a job/notebook directly on Hopsworks."""
        self._base_url = self._get_hopsworks_rest_endpoint()
        self._host, self._port = self._get_host_port_pair()
        self._secrets_dir = (
            os.environ[self.SECRETS_DIR] if self.SECRETS_DIR in os.environ else ""
        )
        self._cert_key = self._get_cert_pw()
        trust_store_path = self._get_trust_store_path()
        hostname_verification = (
            os.environ[self.REQUESTS_VERIFY]
            if self.REQUESTS_VERIFY in os.environ
            else "true"
        )
        self._project_id = os.environ[self.PROJECT_ID]
        self._project_name = self._project_name()
        try:
            self._auth = auth.BearerAuth(self._read_jwt())
        except FileNotFoundError:
            self._auth = auth.ApiKeyAuth(self._read_apikey())
        self._verify = self._get_verify(hostname_verification, trust_store_path)
        self._session = requests.session()

        self._connected = True

        credentials = self._get_credentials(self._project_id)

        self._write_pem_file(credentials["caChain"], self._get_ca_chain_path())
        self._write_pem_file(credentials["clientCert"], self._get_client_cert_path())
        self._write_pem_file(credentials["clientKey"], self._get_client_key_path())

    def _get_hopsworks_rest_endpoint(self):
        """Get the hopsworks REST endpoint for making requests to the REST API."""
        return os.environ[self.REST_ENDPOINT]

    def _get_trust_store_path(self):
        """Convert truststore from jks to pem and return the location"""
        ca_chain_path = Path(self.PEM_CA_CHAIN)
        if not ca_chain_path.exists():
            self._write_ca_chain(ca_chain_path)
        return str(ca_chain_path)

    def _get_ca_chain_path(self) -> str:
        return os.path.join("/tmp", "ca_chain.pem")

    def _get_client_cert_path(self) -> str:
        return os.path.join("/tmp", "client_cert.pem")

    def _get_client_key_path(self) -> str:
        return os.path.join("/tmp", "client_key.pem")

    def _write_ca_chain(self, ca_chain_path):
        """
        Converts JKS trustore file into PEM to be compatible with Python libraries
        """
        keystore_pw = self._cert_key
        keystore_ca_cert = self._convert_jks_to_pem(
            self._get_jks_key_store_path(), keystore_pw
        )
        truststore_ca_cert = self._convert_jks_to_pem(
            self._get_jks_trust_store_path(), keystore_pw
        )

        with ca_chain_path.open("w") as f:
            f.write(keystore_ca_cert + truststore_ca_cert)

    def _convert_jks_to_pem(self, jks_path, keystore_pw):
        """
        Converts a keystore JKS that contains client private key,
         client certificate and CA certificate that was used to
         sign the certificate to PEM format and returns the CA certificate.
        Args:
        :jks_path: path to the JKS file
        :pw: password for decrypting the JKS file
        Returns:
             strings: (ca_cert)
        """
        # load the keystore and decrypt it with password
        ks = jks.KeyStore.load(jks_path, keystore_pw, try_decrypt_keys=True)
        ca_certs = ""

        # Convert CA Certificates into PEM format and append to string
        for alias, c in ks.certs.items():
            ca_certs = ca_certs + self._bytes_to_pem_str(c.cert, "CERTIFICATE")
        return ca_certs

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

    def _get_jks_trust_store_path(self):
        """
        Get truststore location

        Returns:
             truststore location
        """
        t_certificate = Path(self.T_CERTIFICATE)
        if t_certificate.exists():
            return str(t_certificate)
        else:
            username = os.environ[self.HADOOP_USER_NAME]
            material_directory = Path(os.environ[self.MATERIAL_DIRECTORY])
            return str(material_directory.joinpath(username + self.TRUSTSTORE_SUFFIX))

    def _get_jks_key_store_path(self):
        """
        Get keystore location

        Returns:
             keystore location
        """
        k_certificate = Path(self.K_CERTIFICATE)
        if k_certificate.exists():
            return str(k_certificate)
        else:
            username = os.environ[self.HADOOP_USER_NAME]
            material_directory = Path(os.environ[self.MATERIAL_DIRECTORY])
            return str(material_directory.joinpath(username + self.KEYSTORE_SUFFIX))

    def _project_name(self):
        try:
            return os.environ[self.PROJECT_NAME]
        except KeyError:
            pass

        hops_user = self._project_user()
        hops_user_split = hops_user.split(
            "__"
        )  # project users have username project__user
        project = hops_user_split[0]
        return project

    def _project_user(self):
        try:
            hops_user = os.environ[self.HADOOP_USER_NAME]
        except KeyError:
            hops_user = os.environ[self.HDFS_USER]
        return hops_user

    def _get_cert_pw(self):
        """
        Get keystore password from local container

        Returns:
            Certificate password
        """
        pwd_path = Path(self.MATERIAL_PWD)
        if not pwd_path.exists():
            username = os.environ[self.HADOOP_USER_NAME]
            material_directory = Path(os.environ[self.MATERIAL_DIRECTORY])
            pwd_path = material_directory.joinpath(username + self.CERT_KEY_SUFFIX)

        with pwd_path.open() as f:
            return f.read()

    def replace_public_host(self, url):
        """replace hostname to public hostname set in HOPSWORKS_PUBLIC_HOST"""
        ui_url = url._replace(netloc=os.environ[self.HOPSWORKS_PUBLIC_HOST])
        return ui_url

    @property
    def host(self):
        return self._host
