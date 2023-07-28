#
#   Copyright 2022 Hopsworks AB
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
import re

from hsfs.client.exceptions import RestAPIError


class VariableApi:
    def __init__(self):
        pass

    def get_version(self, software: str):
        _client = client.get_instance()
        path_params = [
            "variables",
            "versions",
        ]

        resp = _client._send_request("GET", path_params)
        for entry in resp:
            if entry["software"] == software:
                return entry["version"]
        return None

    def parse_major_and_minor(self, backend_version):
        version_pattern = r"(\d+)\.(\d+)"
        matches = re.match(version_pattern, backend_version)

        return matches.group(1), matches.group(2)

    def get_flyingduck_enabled(self):
        _client = client.get_instance()
        path_params = [
            "variables",
            "enable_flyingduck",
        ]

        resp = _client._send_request("GET", path_params)
        return resp["successMessage"] == "true"

    def get_loadbalancer_external_domain(self):
        _client = client.get_instance()
        path_params = [
            "variables",
            "loadbalancer_external_domain",
        ]

        try:
            resp = _client._send_request("GET", path_params)
            return resp["successMessage"]
        except RestAPIError:
            return ""

    def get_service_discovery_domain(self):
        _client = client.get_instance()
        path_params = [
            "variables",
            "service_discovery_domain",
        ]

        try:
            resp = _client._send_request("GET", path_params)
            return resp["successMessage"]
        except RestAPIError:
            return ""
