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
    def get_subject(
        self,
        subject: str,
        version: str = "latest",
        project_id: str = None,
    ):
        _client = client.get_instance()
        if project_id is None:
            project_id = _client._project_id
        path_params = [
            "project",
            project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
            version,
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)
