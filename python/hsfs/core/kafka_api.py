#
#   Copyright 2021 Logical Clocks AB
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


class KafkaApi:
    def get_topic_subject(self, topic: str):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "topics",
            topic,
            "subjects",
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def get_broker_endpoints(self, externalListeners: bool = False):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "clusterinfo",
        ]
        query_params = {"external": externalListeners}
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, query_params=query_params, headers=headers
        )["brokers"]
