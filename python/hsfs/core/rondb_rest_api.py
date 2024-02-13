#
#   Copyright 2024 Hopsworks AB
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
from typing import Any
from hsfs.client import rondb_rest_client


class RondbRestApi:
    def get_rest_single_raw_feature_vector(self, payload: dict[str, Any]):
        return rondb_rest_client.get_instance()._send_request(
            "POST",
            path_params=["feature_store"],
            headers={"Content-Type": "application/json"},
            data=payload,
        )

    def get_rest_batch_raw_feature_vectors(self, payload: dict[str, Any]):
        return rondb_rest_client.get_instance()._send_request(
            "POST",
            path_params=["batch_feature_store"],
            headers={"Content-Type": "application/json"},
            data=payload,
        )

    def ping_rondb_rest_server(self):
        return rondb_rest_client.get_instance()._send_request(
            "GET", path_params=["ping"]
        )
