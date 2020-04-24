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
import requests

from hsfs.client import base, auth


class Client(base.Client):
    REQUESTS_VERIFY = "REQUESTS_VERIFY"
    DOMAIN_CA_TRUSTSTORE_PEM = "DOMAIN_CA_TRUSTSTORE_PEM"
    PROJECT_ID = "HOPSWORKS_PROJECT_ID"
    PROJECT_NAME = "HOPSWORKS_PROJECT_NAME"
    HADOOP_USER_NAME = "HADOOP_USER_NAME"
    HDFS_USER = "HDFS_USER"

    def __init__(self):
        """Initializes a client being run from a job/notebook directly on Hopsworks."""
        self._base_url = self._get_hopsworks_rest_endpoint()
        self._host, self._port = self._get_host_port_pair()
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
        self._project_id = os.environ[self.PROJECT_ID]
        self._project_name = self._project_name()
        self._auth = auth.BearerAuth(self._read_jwt())
        self._verify = self._get_verify(
            self._host, self._port, hostname_verification, trust_store_path
        )
        self._session = requests.session()

        self._connected = True

    def _get_hopsworks_rest_endpoint(self):
        """Get the hopsworks REST endpoint for making requests to the REST API."""
        return os.environ[self.REST_ENDPOINT]

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
